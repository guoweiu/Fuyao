//
// Created by tank on 9/5/22.
//

#include "agent_connection.h"
#include "engine.h"
#include "base/logging.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas::engine {

    AgentConnection::AgentConnection(Engine *engine, uint16_t conn_id)
            : server::ConnectionBase(kTypeId),
              state_(kCreated),
              uv_tcp_handle_(nullptr),
              log_header_(fmt::format("AgentConnection[{}]: ", conn_id_)),
              conn_id_(conn_id),
              engine_(engine),
              io_worker_(nullptr),
              bind_(false),
              memory_region_name_(fmt::format("EngineMemoryRegion-{}", conn_id)),
              queue_pair_(nullptr),
              timestamp_for_last_send_notification_(0) {

        // Initialize shared Memory
        shared_memory_ = new rdma::SharedMemory();
        shared_memory_info_ = shared_memory_->GetSharedMemoryInfo();

        timeout_as_ms_for_poller_ = engine_->GetConfig()->GetSystemConfig()->timeout_as_ms_for_poller;
    }

    AgentConnection::~AgentConnection() {
        DCHECK(state_ == kCreated || state_ == kClosed);
        if (uv_tcp_handle_ != nullptr) {
            free(uv_tcp_handle_);
        }

        delete queue_pair_;
        delete shared_memory_;

    }

    uv_stream_t *AgentConnection::InitUVHandle(uv_loop_t *uv_loop) {
        uv_tcp_handle_ = new uv_tcp_t;
        UV_DCHECK_OK(uv_tcp_init(uv_loop, uv_tcp_handle_));

        handle_scope_.Init(uv_loop, absl::bind_front(&AgentConnection::OnAllHandlesClosed, this));
        handle_scope_.AddHandle(uv_tcp_handle_);

        return UV_AS_STREAM(uv_tcp_handle_);
    }

    void AgentConnection::Start(server::IOWorker *io_worker) {
        DCHECK(state_ == kCreated);
        io_worker_ = io_worker;
        uv_tcp_handle_->data = this;
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_tcp_handle_),
                                   &AgentConnection::BufferAllocCallback,
                                   &AgentConnection::ReadHandshakeCallback));
        state_ = kHandshake;

    }

    void AgentConnection::ScheduleClose() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_->loop);
        if (state_ == kClosing) {
            HLOG(WARNING) << "Already scheduled for closing";
            return;
        }
        DCHECK(state_ == kHandshake || state_ == kRunning);
        handle_scope_.CloseHandle(uv_tcp_handle_);
        state_ = kClosing;
    }

    void AgentConnection::OnAllHandlesClosed() {
        DCHECK(state_ == kClosing);
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }

    UV_ALLOC_CB_FOR_CLASS(AgentConnection, BufferAlloc) {
        io_worker_->NewReadBuffer(suggested_size, buf);
    }

    UV_READ_CB_FOR_CLASS(AgentConnection, ReadHandshake) {
        auto reclaim_worker_resource = gsl::finally([this, buf] {
            if (buf->base != nullptr) {
                io_worker_->ReturnReadBuffer(buf);
            }
        });
        if (nread < 0) {
            HLOG(ERROR) << "Read error on handshake, will close this connection: "
                        << uv_strerror(nread);
            ScheduleClose();
            return;
        }
        if (nread == 0) {
            HLOG(WARNING) << "nread=0, will do nothing";
            return;
        }
        message_buffer_.AppendData(buf->base, nread);
        if (message_buffer_.length() > sizeof(AgentMessage)) {
            HLOG(ERROR) << "Invalid handshake, will close this connection";
            ScheduleClose();
        } else if (message_buffer_.length() == sizeof(AgentMessage)) {
            RecvHandshakeMessage();
        }
    }

    void AgentConnection::RecvHandshakeMessage() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);
        UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(uv_tcp_handle_)));
        auto message = reinterpret_cast<AgentMessage *>(message_buffer_.data());

        if (IsAgentHandshakeMessage(*message)) {
            // New queue pair
            queue_pair_ = engine_->infinity_->CreateQueuePair();

            // queue pair info : lid, qp_num, psn, gid
            auto qp_info = queue_pair_->GetQueuePairInfo();

            // change queue pair state
            queue_pair_->StateFromResetToInit();
            queue_pair_->StateFromInitToRTR(message->lid, message->qp_num, message->psn, message->gid);
            queue_pair_->StateFromRTRToRTS();

            // register memory region
            auto shared_memory_info = this->shared_memory_->GetSharedMemoryInfo();
            queue_pair_->RegisterMemoryRegion(memory_region_name_, shared_memory_info.addr, shared_memory_info.length);

            memory_region_info_ = queue_pair_->GetMemoryRegionInfo(memory_region_name_);

            HLOG(INFO) << fmt::format("New Memory Region: addr={:x}, length={}, lkey={}, rkey={}",
                                      memory_region_info_.addr, memory_region_info_.length,
                                      memory_region_info_.lkey, memory_region_info_.rkey);

            // record engine_guid from agent
            engine_guid_ = message->engine_guid;

            // identify the relationship between engine and agent
            if (engine_guid_ == this->engine_->guid_)
                bind_ = true;

            // record conn
            engine_->SetAgentConnsGuidWithLock(message->engine_guid, this);

            // will send handshake response to agent
            AgentMessage handshake_message =
                    NewAgentHandshakeResponseMessage(qp_info.lid, qp_info.qp_num, qp_info.psn, qp_info.gid);

            AgentMessage memory_region_register_message =
                    NewMemoryRegionRegisterMessage(memory_region_info_.addr, memory_region_info_.length,
                                                   memory_region_info_.lkey, memory_region_info_.rkey);

            uv_buf_t buf[2];

            buf[0] = {
                    .base = reinterpret_cast<char *>(&handshake_message),
                    .len = sizeof(AgentMessage)
            };
            buf[1] = {
                    .base = reinterpret_cast<char *>(&memory_region_register_message),
                    .len = sizeof(AgentMessage)
            };
            UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(uv_tcp_handle_),
                                  buf, 2, &AgentConnection::HandshakeSentCallback));
            state_ = kHandshake;

        } else {
            HLOG(ERROR) << "Unknown handshake message type";
        }
    }

    uint64_t AgentConnection::AllocateMemoryWithLock(uint32_t expected_size) {
        absl::MutexLock lk(&mutex_);
        return shared_memory_->AllocateMemory(expected_size);
    }

    void AgentConnection::PopMetaDataEntryWithLock(uint32_t idx) {
        absl::MutexLock lk(&mutex_);
        shared_memory_->PopMetaDataEntry(idx);
    }

    void AgentConnection::PushMetaDataEntryWithLock(uint64_t addr, uint32_t size) {
        absl::MutexLock lk(&mutex_);
        shared_memory_->PushMetaDataEntry(addr, size);
    }

    uint32_t AgentConnection::PopMessageWithLock(uint32_t &offset, uint32_t &size) {
        uint64_t addr;
        absl::MutexLock lk(&mutex_);
        uint32_t idx = shared_memory_->PopMessage(addr, size);
        offset = addr - shared_memory_info_.addr;
        return idx;
    }

    uint32_t AgentConnection::PopMessageWithLock(uint64_t &addr, uint32_t &size) {
        absl::MutexLock lk(&mutex_);
        uint32_t idx = shared_memory_->PopMessage(addr, size);
        return idx;
    }

    UV_WRITE_CB_FOR_CLASS(AgentConnection, PollerNotification) {
        auto reclaim_worker_resource = gsl::finally([this, req] {
            io_worker_->ReturnWriteRequest(req);
        });
        if (status != 0) {
            HLOG(ERROR) << "Failed to send handshake, will close this connection: "
                        << uv_strerror(status);
            ScheduleClose();
            return;
        }
        LOG(INFO) << "Send a Poller Notification message";
    }

    UV_WRITE_CB_FOR_CLASS(AgentConnection, HandshakeSent) {
        if (status != 0) {
            HLOG(ERROR) << "Failed to send handshake, will close this connection: "
                        << uv_strerror(status);
            ScheduleClose();
            return;
        }
        engine_->SetMemoryRegionsInfoWithLock(this, &memory_region_info_);
        state_ = kRunning;
        HLOG(INFO) << "Handshake done, Engine enters running";
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_tcp_handle_),
                                   &AgentConnection::BufferAllocCallback,
                                   &AgentConnection::RecvDataCallback));
    }

    UV_READ_CB_FOR_CLASS(AgentConnection, RecvData) {
        auto reclaim_worker_resource = gsl::finally([this, buf] {
            if (buf->base != nullptr) {
                io_worker_->ReturnReadBuffer(buf);
            }
        });
        if (nread < 0) {
            if (nread == UV_EOF) {
                HLOG(INFO) << "Connection closed remotely";
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
        message_buffer_.AppendData(buf->base, nread);
        ProcessAgentMessages();
    }

    void AgentConnection::ProcessAgentMessages() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_->loop);
        while (message_buffer_.length() >= sizeof(AgentMessage)) {
            auto message = reinterpret_cast<AgentMessage *>(message_buffer_.data());

            if (IsPollerNotification(*message)) {
                HLOG(INFO) << fmt::format("Receive a Poller Notification Message, start poller");
                this->engine_->poller_->Start();
            } else {
                HLOG(ERROR) << "Unknown handshake message type";
            }
            message_buffer_.ConsumeFront(sizeof(AgentMessage));
        }
    }

    void AgentConnection::Notification() {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);

        bool need_to_notification;

        auto current_timestamp = GetMilliTimestamp();
        auto timeout = current_timestamp - timestamp_for_last_send_notification_;
        need_to_notification = (timeout >= (timeout_as_ms_for_poller_ * 3.0 / 4));

        if (need_to_notification) {
            // update timestamp
            {
                absl::MutexLock lk(&mutex_);
                timestamp_for_last_send_notification_ = current_timestamp;
            }
            // send a message
            AgentMessage notification_message = NewPollerNotificationMessage();
            uv_buf_t buf = {
                    .base = reinterpret_cast<char *>(&notification_message),
                    .len = sizeof(AgentMessage)
            };
            UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(uv_tcp_handle_),
                                  &buf, 1, &AgentConnection::PollerNotificationCallback));
        }


    }


}