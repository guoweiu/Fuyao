#ifndef LUMINE_FUNC_CONFIG_H
#define LUMINE_FUNC_CONFIG_H

#include "base/common.h"
#include "common/protocol.h"

namespace faas::config {

    struct System {
        uint64_t timeout_as_ms_for_poller;
        uint64_t timeout_as_ms_for_drc;
        uint64_t poll_time_internal_as_us;
        uint64_t memory_region_size;
        uint64_t message_size_of_fabric;
    };

    struct Gateway {
        std::string gateway_ip;             // Address to listen
        int http_port;                      // Port for HTTP connections
        int grpc_port;                      // Port for gRPC connections
        int engine_conn_port;               // Port for engine connections
        int num_io_workers;                 // Number of IO workers
        int listen_backlog;
    };

    struct EngineEntry {
        uint16_t guid;                      // Engine guid
        std::string engine_ip;              // Engine ip address, will be used by agent
        int agent_conn_port;                // Port for agent connections
        int func_worker_use_engine_socket;  // If set not 0, Launcher and FuncWorker will communicate with engine via TCP socket
        int engine_tcp_port;                // Engine TCP port for Launcher and FuncWorker
        int num_io_workers;                 // Number of IO workers
        int listen_backlog;
        int gateway_conn_per_worker;        // Number of gateway connections per IO worker
        std::string root_path_for_ipc;      // Root directory for IPCs used by FaaS
        std::string device_name;            // RDMA device name
        int device_port;                    // RDMA port
        int device_gid_index;               // RDMA local port gid index
        uint64_t time_internal_for_reclaim_drc;  // units: ms
    };

    struct AgentEntry {
        uint16_t guid;                      // Agent guid
        std::string device_name;            // RDMA device name
        int device_port;                    // RDMA port
        int device_gid_index;               // RDMA local port gid index
        int num_io_workers;                 // Number of IO workers
        uint16_t bind_engine_guid;          // Indicates the binding relationship between the DPU-Agent and Host-Engine
    };

    struct FunctionEntry {
        std::string func_name;
        int func_id;
        int min_workers;
        int max_workers;
        bool allow_http_get;
        bool qs_as_input;
        bool is_grpc_service;
        std::string grpc_service_name;
        std::vector<std::string> grpc_methods;
        std::unordered_map<std::string, int> grpc_method_ids;
    };

    class Config {
    public:
        static constexpr int kMaxFuncId = (1 << protocol::kFuncIdBits) - 1;
        static constexpr int kMaxMethodId = (1 << protocol::kMethodIdBits) - 1;

        bool Load(std::string_view json_contents);

        const FunctionEntry *FindFunctionByFuncName(std::string_view func_name);

        const FunctionEntry * FindFunctionByFuncId(int func_id) const;

        const Gateway *GetGatewayConfig();

        const System *GetSystemConfig();

        const EngineEntry *GetEngineConfigByGuid(uint64_t guid);

        const AgentEntry *GetAgentConfigByGuid(uint64_t guid);

        std::vector<EngineEntry *> GetEngineList();


    private:
        // for system
        std::unique_ptr<System> system_;

        // for gateway
        std::unique_ptr<Gateway> gateway_;

        // for engines
        std::unordered_map<uint16_t /* guid */, std::unique_ptr<EngineEntry>> engine_entries_;
        std::vector<EngineEntry *> engines_;

        // for agents
        std::unordered_map<uint16_t /* guid */, std::unique_ptr<AgentEntry>> agent_entries_;

        std::unordered_set<uint16_t> bind_engines_;

        // for functions
        std::vector<std::unique_ptr<FunctionEntry>> func_entries_{};
        std::unordered_map<std::string, FunctionEntry *> entries_by_func_name_{};
        std::unordered_map<int, FunctionEntry *> entries_by_func_id_{};

        static bool ValidateFuncId(int func_id);

        static bool ValidateFuncName(std::string_view func_name);

        //DISALLOW_COPY_AND_ASSIGN(Config);
    };

}  // namespace faas

#endif //LUMINE_FUNC_CONFIG_H