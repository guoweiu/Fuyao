#ifndef LUMINE_ENGINE_H
#define LUMINE_ENGINE_H

#include <utility>

#include "base/common.h"
#include "common/uv.h"
#include "common/protocol.h"
#include "common/config.h"
#include "ipc/shm_region.h"
#include "server/poller.h"
#include "server/server_base.h"
#include "gateway_connection.h"
#include "message_connection.h"
#include "dispatcher.h"
#include "worker_manager.h"
#include "monitor.h"
#include "tracer.h"
#include "rdma/infinity.h"
#include "rdma/queue_pair.h"
#include "rdma/shared_memory.h"


namespace faas::engine {

    class Engine;

    struct TupleForPipeWrite {
        Engine *engine;
        uv_buf_t *uv_buf;
        int nbufs;

        TupleForPipeWrite(Engine *engine, uv_buf_t *uv_buf, int nbufs)
                : engine(engine), uv_buf(uv_buf), nbufs(nbufs) {}
    };

    // forward declaration
    class AgentConnection;

    class Engine final : public server::ServerBase {

        friend class AgentConnection;
        friend class MessageConnection;

    public:
        Engine();

        ~Engine() override;

        void SetConfigFile(std::string config_file) { config_file_ = std::move(config_file); }

        void SetGuid(uint16_t guid) { guid_ = guid; }

        uint16_t GetGuid() const { return guid_; }

        config::Config *GetConfig() { return &config_; }

        bool func_worker_use_engine_socket() const { return func_worker_use_engine_socket_; }

        WorkerManager *worker_manager() { return worker_manager_.get(); }

        Monitor *monitor() { return monitor_.get(); }

        Tracer *tracer() { return tracer_.get(); }

        // Must be thread-safe
        bool OnNewHandshake(MessageConnection *connection,
                            const protocol::Message &handshake_message,
                            protocol::Message *response,
                            std::span<const char> *response_payload);

        void OnRecvMessage(MessageConnection *connection, protocol::Message *message);

        void OnRecvGatewayOrAgentMessage(server::ConnectionBase *connection,
                                         const protocol::GatewayMessage &message,
                                         std::span<const char> payload);

        Dispatcher *GetOrCreateDispatcher(uint16_t func_id);

        void DiscardFuncCall(const protocol::FuncCall &func_call);

        void OnMessageReceiveCallBack();

    private:
        // Set only once
        std::string config_file_;
        std::string config_json_;
        config::Config config_;
        bool func_worker_use_engine_socket_;
        bool use_fifo_for_nested_call_;
        uint16_t guid_;

        const config::EngineEntry *engine_config_;

        // thread-safe
        server::Poller *poller_;
        rdma::Infinity *infinity_;
        uv_stream_t *uv_handle_;
        uv_tcp_t uv_agent_conn_handle_;
        std::vector<server::IOWorker *> io_workers_;
        size_t next_gateway_conn_worker_id_;
        size_t next_ipc_conn_worker_id_;
        uint16_t next_gateway_conn_id_;
        uint64_t next_agent_conn_id_;
        size_t next_agent_conn_worker_id_;
        uv_timer_t *timer_req_;
        uint64_t timeout_for_drc_;

        std::unique_ptr<WorkerManager> worker_manager_;
        std::unique_ptr<Monitor> monitor_;
        std::unique_ptr<Tracer> tracer_;
        std::atomic<int> inflight_external_requests_;

        absl::Mutex mu_;
        absl::Mutex mu_for_conn_;

        // thread-unsafe
        absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>>
                message_connections_ ABSL_GUARDED_BY(mu_);
        absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>>
                gateway_connections_ ABSL_GUARDED_BY(mu_);
        absl::flat_hash_map</* agent_conn id*/ int, rdma::MemoryRegionInfo *>
                memory_regions_info_ ABSL_GUARDED_BY(mu_);
        absl::flat_hash_map</* engine_guid */ uint16_t, std::shared_ptr<AgentConnection>>
                agent_conns_guid_ ABSL_GUARDED_BY(mu_);
        absl::flat_hash_map</* full_call_id */ uint64_t, std::unique_ptr<ipc::ShmRegion>>
                external_func_call_shm_inputs_ ABSL_GUARDED_BY(mu_);
        absl::flat_hash_map</* func_id */ uint16_t, std::unique_ptr<Dispatcher>>
                dispatchers_ ABSL_GUARDED_BY(mu_);

        std::set<protocol::MessageTuple> message_tuples_ ABSL_GUARDED_BY(mu_);
        std::vector<protocol::FuncCall> discarded_func_calls_ ABSL_GUARDED_BY(mu_);

        int64_t last_external_request_timestamp_ ABSL_GUARDED_BY(mu_);
        stat::Counter incoming_external_requests_stat_ ABSL_GUARDED_BY(mu_);
        stat::Counter incoming_internal_requests_stat_ ABSL_GUARDED_BY(mu_);
        stat::StatisticsCollector<float> external_requests_instant_rps_stat_ ABSL_GUARDED_BY(mu_);
        stat::StatisticsCollector<uint16_t> inflight_external_requests_stat_ ABSL_GUARDED_BY(mu_);

        stat::StatisticsCollector<int32_t> message_delay_stat_ ABSL_GUARDED_BY(mu_);
        stat::Counter input_use_shm_stat_ ABSL_GUARDED_BY(mu_);
        stat::Counter output_use_shm_stat_ ABSL_GUARDED_BY(mu_);
        stat::Counter discarded_func_call_stat_ ABSL_GUARDED_BY(mu_);

        void StartInternal() override;

        void StopInternal() override;

        void OnConnectionClose(server::ConnectionBase *connection) override;

        void OnExternalFuncCall(const protocol::FuncCall &func_call, std::span<const char> input);

        void ExternalFuncCallCompleted(const protocol::FuncCall &func_call,
                                       std::span<const char> output, int32_t processing_time);

        void ExternalFuncCallFailed(const protocol::FuncCall &func_call, int status_code = 0);

        AgentConnection *RandomChooseAgentConnExceptSelf();

        Dispatcher *GetOrCreateDispatcherLocked(uint16_t func_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

        std::unique_ptr<ipc::ShmRegion> GrabExternalFuncCallShmInput(
                const protocol::FuncCall &func_call) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

        void ProcessDiscardedFuncCallIfNecessary();

        void SetAgentConnsGuidWithLock(uint16_t guid, AgentConnection *conn);

        AgentConnection *GetAgentConnsByGuidWithLock(uint16_t guid);

        void SetMemoryRegionsInfoWithLock(AgentConnection *conn, rdma::MemoryRegionInfo *memory_region_info);

        void PushMessageIntoSharedMemoryWithLock(AgentConnection *conn, protocol::Message *message,
                                                 protocol::MessageType message_type) const;

        bool DoesMessageBelongToMe(protocol::Message *message) const;

        static void ReclaimDRCResources(uv_timer_t *req);

        void OnReclaimDRCResources();

        DECLARE_UV_CONNECT_CB_FOR_CLASS(GatewayConnect);

        DECLARE_UV_CONNECTION_CB_FOR_CLASS(MessageConnection);

        DECLARE_UV_CONNECTION_CB_FOR_CLASS(AgentConnection);

        DISALLOW_COPY_AND_ASSIGN(Engine);
    };

}  // namespace faas

#endif //LUMINE_ENGINE_H