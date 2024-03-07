//
// Created by tank on 9/5/22.
//

#ifndef LUMINE_HOST_BENCH_AGENT_CONNECTION_H
#define LUMINE_HOST_BENCH_AGENT_CONNECTION_H

#include "base/common.h"
#include "common/protocol.h"
#include "common/uv.h"
#include "server/connection_base.h"
#include "server/io_worker.h"
#include "rdma/infinity.h"
#include "rdma/queue_pair.h"
#include "rdma/shared_memory.h"
#include "utils/appendable_buffer.h"

namespace faas::engine {

    using protocol::AgentMessage;
    using protocol::IsAgentHandshakeMessage;
    using protocol::NewAgentHandshakeResponseMessage;
    using protocol::NewMemoryRegionRegisterMessage;
    using protocol::NewPollerNotificationMessage;
    using protocol::IsPollerNotification;

    enum State {
        kCreated, kHandshake, kRunning, kClosing, kClosed
    };

    // forward declaration
    class Engine;

    class AgentConnection final : public server::ConnectionBase {

        friend class Engine;

    public:
        static constexpr int kTypeId = 2;

        AgentConnection(Engine *engine, uint16_t conn_id);

        ~AgentConnection() override;

        uv_stream_t *InitUVHandle(uv_loop_t *uv_loop) override;

        void Start(server::IOWorker *io_worker) override;

        void ScheduleClose() override;

        void set_conn_id(uint64_t conn_id) { conn_id_ = conn_id; }

        uint16_t conn_id() const { return conn_id_; }

        void Notification();

        bool IsBind() const { return bind_; }

        uint16_t GetEngineGuid() const { return engine_guid_; }

    private:
        State state_;
        uv_tcp_t *uv_tcp_handle_;
        std::string log_header_;
        uint16_t conn_id_;
        Engine *engine_;
        server::IOWorker *io_worker_;
        utils::AppendableBuffer message_buffer_;
        uv::HandleScope handle_scope_;
        uint16_t engine_guid_;
        bool bind_;

        // thread-safe
        std::string memory_region_name_;
        rdma::MemoryRegionInfo memory_region_info_;
        rdma::QueuePair *queue_pair_;
        rdma::SharedMemoryInfo shared_memory_info_;
        uint64_t timeout_as_ms_for_poller_;

        absl::Mutex mutex_;

        // thread-unsafe
        rdma::SharedMemory *shared_memory_ GUARDED_BY(mutex_);
        uint64_t timestamp_for_last_send_notification_ GUARDED_BY(mutex_);

        void RecvHandshakeMessage();

        void OnAllHandlesClosed();

        void ProcessAgentMessages();

        uint64_t AllocateMemoryWithLock(uint32_t expected_size);

        void PopMetaDataEntryWithLock(uint32_t idx);

        void PushMetaDataEntryWithLock(uint64_t addr, uint32_t size);

        uint32_t PopMessageWithLock(uint32_t &offset, uint32_t &size);

        uint32_t PopMessageWithLock(uint64_t &addr, uint32_t &size);

        DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);

        DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshake);

        DECLARE_UV_WRITE_CB_FOR_CLASS(HandshakeSent);

        DECLARE_UV_WRITE_CB_FOR_CLASS(PollerNotification);

        DECLARE_UV_READ_CB_FOR_CLASS(RecvData);

    };
}

#endif //LUMINE_HOST_BENCH_AGENT_CONNECTION_H
