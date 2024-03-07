//
// Created by LSwor on 2022/9/8.
//

#ifndef LUMINE_DPU_BENCH_AGENT_H
#define LUMINE_DPU_BENCH_AGENT_H

#include "base/common.h"
#include "common/config.h"
#include "common/protocol.h"
#include "base/logging.h"
#include "server/server_base.h"
#include "server/poller.h"
#include "utils/socket.h"
#include "utils/fs.h"
#include "rdma/infinity.h"
#include "rdma/queue_pair.h"
#include "rdma/shared_memory.h"

namespace faas::agent {

    // forward declaration
    class EngineConnection;

    class Agent;

    struct Tuple{
        Agent *agent;
        bool bind;
    };

    class Agent final : public server::ServerBase {

        friend class EngineConnection;

        static constexpr int kDefaultNumIOWorkers = 1;

    public:
        Agent();

        ~Agent() override;

        void SetConfigFile(std::string config_file) { config_file_ = std::move(config_file); }

        void SetGuid(uint16_t guid) { guid_ = guid; }

        void OnDataCollectionCallBack();

    private:

        // Set only once
        uint16_t guid_;
        std::string config_file_;

        // thread-safe
        config::Config config_;
        uint16_t engine_guid_;
        rdma::Infinity *infinity_;
        server::Poller *poller_;
        size_t next_agent_conn_worker_id_;
        std::vector<server::IOWorker *> io_workers_;
        size_t next_engine_conn_id_;
        rdma::SharedMemoryInfo shared_memory_info_;

        absl::Mutex mutex_for_conn_;
        absl::Mutex mutex_for_shared_memory_;

        // thread unsafe
        rdma::SharedMemory *shared_memory_  GUARDED_BY(mutex_for_shared_memory_);
        absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>>
                engine_connections_ GUARDED_BY(mutex_for_conn_);

        void StartInternal() override;

        void StopInternal() override;

        void OnEngineConnect(uv_connect_t *req, int status, bool bind);

        static void EngineConnectCallback(uv_connect_t *req, int status);

        void ConnectToEngine(void *arg);

        void RestartPoller();

        rdma::MemoryRegionInfo RegisterMemoryRegion(rdma::QueuePair *queue_pair, std::string_view memory_region_name);

        uint64_t AllocateMemoryWithLock(uint32_t expected_size);

        void PopMetaDataEntryWithLock(uint32_t idx);

        void PushMetaDataEntryWithLock(uint64_t addr, uint32_t size);

        uint32_t PopMessageWithLock(uint32_t &offset, uint32_t &size);

        void GetPreviousOrNextIdxWithLock(uint32_t &idx, bool direction);

    };

}

#endif //LUMINE_DPU_BENCH_AGENT_H
