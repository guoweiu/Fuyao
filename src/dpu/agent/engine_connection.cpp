//
// Created by LSwor on 2022/9/8.
//

#include "engine_connection.h"
#include "agent.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

ABSL_FLAG(bool, engine_conn_enable_nodelay, true,
          "Enable TCP_NODELAY for connections to engine");
ABSL_FLAG(bool, engine_conn_enable_keepalive, true,
          "Enable TCP keep-alive for connections to engine");

namespace faas::agent {

    agent::EngineConnection::EngineConnection(Agent *agent, uint16_t conn_id, bool bind)
            : state_(kCreated),
              log_header_(fmt::format("EngineConnection[{}]: ", conn_id)),
              agent_(agent),
              conn_id_(conn_id),
              bind_(bind),
              last_time_us_(0),
              poll_time_internal_(0),
              uv_tcp_handle_(nullptr),
              io_worker_(nullptr),
              queue_pair_(nullptr),
              memory_region_name_(fmt::format("AgentMemoryRegion-{}", conn_id)),
              old_head_idx_(0),
              old_tail_idx_(-1),
              timestamp_for_last_send_notification_(0),
              poll_completion_cb_(absl::bind_front(&EngineConnection::PollCompletionCb, this)) {
        timeout_as_ms_for_poller_ = agent_->config_.GetSystemConfig()->timeout_as_ms_for_poller;
    }

    EngineConnection::~EngineConnection() {
        DCHECK(state_.load() == kCreated || state_.load() == kClosed);

        if (uv_tcp_handle_ != nullptr) {
            free(uv_tcp_handle_);
        }
    }

    uv_stream_t *EngineConnection::InitUVHandle(uv_loop_t *uv_loop) {
        uv_tcp_handle_ = new uv_tcp_t;
        uv_tcp_handle_->data = this;

        UV_DCHECK_OK(uv_tcp_init(uv_loop, uv_tcp_handle_));
        return UV_AS_STREAM(uv_tcp_handle_);
    }

    void EngineConnection::Start(server::IOWorker *io_worker) {
        DCHECK(state_.load() == kCreated);
        DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_->loop);

        io_worker_ = io_worker;
        // config
        auto system_config_ = agent_->config_.GetSystemConfig();
        poll_time_internal_ = system_config_->timeout_as_ms_for_poller;

        // New queue pair
        queue_pair_ = agent_->infinity_->CreateQueuePair();

        // RDMA memory region
        local_memory_region_info_ = agent_->RegisterMemoryRegion(queue_pair_, memory_region_name_);

        // queue pair info : lid, qp_num, psn, gid
        auto qp_info = queue_pair_->GetQueuePairInfo();

        // will send handshake to engine
        protocol::AgentMessage handshake_message_ =
                NewAgentHandshakeMessage(agent_->engine_guid_,
                                         qp_info.lid, qp_info.qp_num, qp_info.psn, qp_info.gid);

        uv_buf_t buf = {
                .base = reinterpret_cast<char *>(&handshake_message_),
                .len = sizeof(AgentMessage)
        };

        UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(uv_tcp_handle_),
                              &buf, 1, &EngineConnection::HandshakeSentCallback));
        state_.store(kHandshake);
    }

    void EngineConnection::ScheduleClose() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
        if (state_ == kClosing) {
            HLOG(WARNING) << "Already scheduled for closing";
            return;
        }
        DCHECK(state_ == kHandshake || state_ == kRunning);
        uv_close(UV_AS_HANDLE(uv_tcp_handle_), &EngineConnection::CloseCallback);
        state_ = kClosing;
    }

    rdma::WorkRequest EngineConnection::NewWorkRequest(rdma::WorkIDType work_id_type, uint32_t remote_offset,
                                                       uint32_t expected_size,
                                                       void *value, uint32_t idx) {
        uint64_t start_addr = agent_->AllocateMemoryWithLock(expected_size);
        if (value != nullptr) {
            memcpy(reinterpret_cast<void *>(start_addr), value, expected_size);
        }
        uint32_t local_offset = start_addr - local_memory_region_info_.addr;

        uint64_t wr_id;
        if (idx != -1) {
            wr_id = MakeWorkID(work_id_type, expected_size, idx);
        } else {
            wr_id = MakeWorkID(work_id_type, expected_size, local_offset);
        }

        return MakeWorkRequest(local_memory_region_info_,
                               dest_memory_region_info_,
                               local_offset, remote_offset, wr_id, expected_size);
    }

    void EngineConnection::PollCompletionCb(uint64_t wr_id) {
        rdma::WorkID work_id = ParseWorkID(wr_id);
        uint64_t base_addr = this->local_memory_region_info_.addr + work_id.offset;

        if (work_id.type == WorkIDType::RDMA_READ_META_DATA_PULL) {
            uint32_t head, tail;
            head = *(reinterpret_cast<uint32_t *>(base_addr));
            tail = *(reinterpret_cast<uint32_t *>(base_addr + CACHE_LINE));

            // update Point in Engine Meta-data Region
            if (head != old_head_idx_) {
                auto wr = NewWorkRequest(rdma::WorkIDType::RDMA_UPDATE_HEAD_PULL, 0, 8, &old_head_idx_);
                queue_pair_->PostWriteRequest(wr);
            }
            // no new message
            if (tail == old_head_idx_)
                return;

            auto meta_data_start_addr = base_addr + 2 * CACHE_LINE;

            for (auto i = old_head_idx_; i != tail; agent_->GetPreviousOrNextIdxWithLock(i, true)) {
                auto entry = reinterpret_cast<MetaDataEntry *>(
                        meta_data_start_addr + i * META_DATA_ENTRY_SIZE);

                // Work Request For READ Engine Raw-data Region
                auto wr = NewWorkRequest(rdma::WorkIDType::RDMA_READ_RAW_DATA_PULL, entry->offset, entry->size);
                // Post Read Request for READ Engine Raw-data Region
                queue_pair_->PostReadRequest(wr);
            }
            // update old head
            old_head_idx_ = tail;

        } else if (work_id.type == WorkIDType::RDMA_READ_RAW_DATA_PULL) {
            const protocol::Message *message = reinterpret_cast<protocol::Message *>(base_addr);
            DLOG(INFO) << fmt::format(
                    "RDMA READ a message[func_id={}, client_id={}, call_id={}, payload_size={}] from Host-Engine",
                    message->func_id, message->client_id, message->call_id, message->payload_size);

            agent_->PushMetaDataEntryWithLock(base_addr, work_id.size);
            DLOG(INFO) << fmt::format("Push Meta Data Entry");

        } else if (work_id.type == rdma::WorkIDType::RDMA_UPDATE_HEAD_PULL) {
            DLOG(INFO) << "Advance head pointer";
        } else if (work_id.type == WorkIDType::RDMA_WRITE_RAW_DATA_PUSH) {
            uint32_t idx = work_id.idx;
            uint32_t offset = 2 * CACHE_LINE + idx * META_DATA_ENTRY_SIZE;

            uint64_t new_wr_id = MakeWorkID(rdma::WorkIDType::RDMA_WRITE_META_DATA_PUSH, META_DATA_ENTRY_SIZE, idx);
            auto wr = MakeWorkRequest(local_memory_region_info_, dest_memory_region_info_,
                                      offset, offset, new_wr_id, META_DATA_ENTRY_SIZE);
            queue_pair_->PostWriteRequest(wr);
        } else if (work_id.type == WorkIDType::RDMA_WRITE_META_DATA_PUSH) {
            uint32_t idx = work_id.idx;
            DLOG(INFO) << fmt::format("RDMA WRITE a message[idx={}] to engine", idx);
            uint32_t old_tail_idx_copy = old_tail_idx_;
            agent_->GetPreviousOrNextIdxWithLock(old_tail_idx_copy, true);

            auto wr = NewWorkRequest(rdma::WorkIDType::RDMA_UPDATE_TAIL_PUSH, CACHE_LINE, 8, &old_tail_idx_copy,
                                     old_tail_idx_);
            queue_pair_->PostWriteRequest(wr);
        } else if (work_id.type == rdma::WorkIDType::RDMA_UPDATE_TAIL_PUSH) {
            uint32_t idx = work_id.idx;
            agent_->PopMetaDataEntryWithLock(idx);
            // send a message to engine to restart Poller
            Notification();
            DLOG(INFO) << fmt::format("Advance tail pointer, then pop Meta Data Entry[idx={}]", idx);
        } else {
            DLOG(WARNING) << "Illegal Work ID type.";
        }
    }

    void EngineConnection::Execute() {
        DCHECK(state_.load() == kRunning);

        int64_t start_time_msc = GetMonotonicMicroTimestamp(), current_time_msc;
        int poll_result;
        do {
            poll_result = queue_pair_->PollCompletion(poll_completion_cb_);
            current_time_msc = GetMonotonicMicroTimestamp();
        } while ((poll_result == 0) && ((current_time_msc - start_time_msc) < 100));

        if (!bind_) { // Pull message via RDMA
            // Post Read Request
            queue_pair_->PostReadRequest(wr_for_read_metadata_);
        } else { // Push message via RDMA
            uint32_t offset, size, idx;
            idx = agent_->PopMessageWithLock(offset, size);
            // no messages
            if (idx == old_tail_idx_ || idx == -1) return;
            old_tail_idx_ = idx;
            // Work Request For WRITE Engine Raw-data Region
            uint64_t wr_id = MakeWorkID(rdma::WorkIDType::RDMA_WRITE_RAW_DATA_PUSH,
                                        size, idx);
            WorkRequest wr = MakeWorkRequest(local_memory_region_info_,
                                             dest_memory_region_info_,
                                             offset, offset, wr_id, size);
            DLOG(INFO) << fmt::format("Exec PushMessageViaRDMA");
            // Post Read Request
            queue_pair_->PostWriteRequest(wr);
        }
    }

    UV_ALLOC_CB_FOR_CLASS(EngineConnection, BufferAlloc) {
        io_worker_->NewReadBuffer(suggested_size, buf);
    }

    UV_WRITE_CB_FOR_CLASS(EngineConnection, MessageSent) {
        auto reclaim_worker_resource = gsl::finally([this, req] {
            io_worker_->ReturnWriteRequest(req);
        });
        if (status != 0) {
            HLOG(ERROR) << "Failed to send data, will close this connection: "
                        << uv_strerror(status);
            ScheduleClose();
        }
    }

    UV_WRITE_CB_FOR_CLASS(EngineConnection, HandshakeSent) {
        if (status != 0) {
            HLOG(ERROR) << "Failed to send handshake, will close this connection: "
                        << uv_strerror(status);
            ScheduleClose();
            return;
        }
        HLOG(INFO) << "Handshake sent, Ready to receive response";
        state_.store(kRDMACreating);
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_tcp_handle_),
                                   &EngineConnection::BufferAllocCallback,
                                   &EngineConnection::RecvDataCallback));
    }

    UV_READ_CB_FOR_CLASS(EngineConnection, RecvData) {
        auto reclaim_worker_resource = gsl::finally([this, buf] {
            if (buf->base != nullptr) {
                io_worker_->ReturnReadBuffer(buf);
            }
        });
        if (nread < 0) {
            if (nread == UV_EOF) {
                HLOG(WARNING) << "Connection closed remotely";
            } else {
                HLOG(ERROR) << "Read error, will close this connection: "
                            << uv_strerror(nread);
            }
            ScheduleClose();
            return;
        }
        if (nread == 0) {
            return;
        }
        read_buffer_.AppendData(buf->base, nread);
        ProcessEngineMessages();
    }

    UV_CLOSE_CB_FOR_CLASS(EngineConnection, Close) {
        DCHECK(state_.load() == kClosing);
        state_.store(kClosed);
        io_worker_->OnConnectionClose(this);
    }

    void EngineConnection::ProcessEngineMessages() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_->loop);
        while (read_buffer_.length() >= sizeof(AgentMessage)) {
            auto message = reinterpret_cast<AgentMessage *>(read_buffer_.data());

            // handshake response from engine
            if (IsAgentHandshakeResponseMessage(*message)) {
                DCHECK(state_.load() == kRDMACreating);
                // change queue pair state
                queue_pair_->StateFromResetToInit();
                queue_pair_->StateFromInitToRTR(message->lid, message->qp_num, message->psn, message->gid);
                queue_pair_->StateFromRTRToRTS();

                state_.store(kRDMACreated);
            } else if (IsMemoryRegionRegisterMessage(*message)) {
                DCHECK(state_.load() == kRDMACreated);
                // memory region info from engine
                dest_memory_region_info_ = {message->addr, message->length, message->lkey, message->rkey};

                HLOG(INFO) << fmt::format("remote memory region: addr=0x{:x}, length={}, lkey={}, rkey={}",
                                          dest_memory_region_info_.addr, dest_memory_region_info_.length,
                                          dest_memory_region_info_.lkey, dest_memory_region_info_.rkey);

                // Work Request For READ Engine Metadata Region
                wr_for_read_metadata_ = NewWorkRequest(rdma::WorkIDType::RDMA_READ_META_DATA_PULL, 0,
                                                       BASIC_ALLOCATE_SIZE);

                state_.store(kRunning);
            } else if (IsPollerNotification(*message)) {
                HLOG(INFO) << fmt::format("Receive a Poller Notification Message");
                this->agent_->RestartPoller();
                last_time_us_ = GetRealtimeMicroTimestamp();
            } else {
                HLOG(ERROR) << "Unknown handshake message type";
            }
            read_buffer_.ConsumeFront(sizeof(AgentMessage));
        }
    }

    void EngineConnection::Notification() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);

        bool need_to_notification;

        auto current_timestamp = GetMilliTimestamp();
        auto timeout = current_timestamp - timestamp_for_last_send_notification_;
        need_to_notification = (timeout >= (timeout_as_ms_for_poller_ * 3.0 / 4));

        if (need_to_notification) {
            // update timestamp
            {
                absl::MutexLock lk(&mu_);
                timestamp_for_last_send_notification_ = current_timestamp;
            }
            // send a message
            AgentMessage notification_message = NewPollerNotificationMessage();

            uv_buf_t buf = {
                    .base = reinterpret_cast<char *>(&notification_message),
                    .len = sizeof(AgentMessage)
            };

            UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(uv_tcp_handle_),
                                  &buf, 1, &EngineConnection::MessageSentCallback));
        }
    }

}