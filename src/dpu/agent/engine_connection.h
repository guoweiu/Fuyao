//
// Created by LSwor on 2022/9/8.
//

#ifndef LUMINE_DPU_BENCH_ENGINE_CONNECTION_H
#define LUMINE_DPU_BENCH_ENGINE_CONNECTION_H

#include "base/common.h"
#include "common/protocol.h"
#include "server/connection_base.h"
#include "server/io_worker.h"
#include "rdma/infinity.h"
#include "rdma/queue_pair.h"
#include "utils/appendable_buffer.h"
#include "rdma/shared_memory.h"

namespace faas::agent {

    class Agent;

    using protocol::AgentMessage;
    using protocol::NewAgentHandshakeMessage;
    using protocol::IsMemoryRegionRegisterMessage;
    using protocol::IsPollerNotification;
    using protocol::NewPollerNotificationMessage;
    using protocol::AgentMessage;
    using protocol::IsAgentHandshakeResponseMessage;
    using rdma::ParseWorkID;
    using rdma::MakeWorkID;
    using rdma::WorkRequest;
    using rdma::MakeWorkRequest;
    using rdma::MetaDataEntry;
    using rdma::WorkIDType;

    enum State {
        kCreated, kHandshake, kRunning, kRDMACreating, kRDMACreated, kClosing, kClosed
    };

    class EngineConnection final : public server::ConnectionBase {
    public:
        EngineConnection(Agent *agent, uint16_t conn_id, bool bind);

        ~EngineConnection() override;

        uv_stream_t *InitUVHandle(uv_loop_t *uv_loop) override;

        void Start(server::IOWorker *io_worker) override;

        void ScheduleClose() override;

        void Notification();

        void Execute();

    private:

        std::atomic<State> state_;
        std::string log_header_;

        Agent *agent_;
        uint16_t conn_id_;
        bool bind_;
        int64_t last_time_us_;
        uint64_t poll_time_internal_;
        uint64_t timeout_as_ms_for_poller_;

        uv_tcp_t *uv_tcp_handle_;
        server::IOWorker *io_worker_;

        utils::AppendableBuffer read_buffer_;

        rdma::QueuePair *queue_pair_;

        std::string memory_region_name_;
        rdma::MemoryRegionInfo dest_memory_region_info_;
        rdma::MemoryRegionInfo local_memory_region_info_;
        rdma::WorkRequest wr_for_read_metadata_;

        uint32_t old_head_idx_;
        uint32_t old_tail_idx_;

        absl::Mutex mu_;

        uint64_t timestamp_for_last_send_notification_ GUARDED_BY(mu_);

        std::function<void(uint64_t)> poll_completion_cb_;

        void ProcessEngineMessages();

        void PollCompletionCb(uint64_t wr_id);

        rdma::WorkRequest NewWorkRequest(rdma::WorkIDType work_id_type, uint32_t remote_offset, uint32_t expected_size, void *value = nullptr, uint32_t idx = -1);

        DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);

        DECLARE_UV_WRITE_CB_FOR_CLASS(HandshakeSent);

        DECLARE_UV_WRITE_CB_FOR_CLASS(MessageSent);

        DECLARE_UV_READ_CB_FOR_CLASS(RecvData);

        DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    };
}


#endif //LUMINE_DPU_BENCH_ENGINE_CONNECTION_H
