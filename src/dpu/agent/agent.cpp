//
// Created by LSwor on 2022/9/8.
//

#include "agent.h"
#include "engine_connection.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

#define HLOG(l) LOG(l) << "Agent: "

namespace faas::agent {

    Agent::Agent()
            : engine_guid_(-1),
              infinity_(nullptr),
              next_agent_conn_worker_id_(0),
              next_engine_conn_id_(0) {
    }

    Agent::~Agent() {
        delete poller_;
        delete infinity_;
        delete shared_memory_;
    }

    void Agent::ConnectToEngine(void *arg) {
        auto engine_entry = reinterpret_cast<config::EngineEntry *>(arg);

        struct sockaddr_in addr{};
        if (!utils::FillTcpSocketAddr(&addr, engine_entry->engine_ip, engine_entry->agent_conn_port)) {
            HLOG(FATAL) << "Failed to fill socker address for " << engine_entry->engine_ip;
        }

        auto uv_handle = new uv_tcp_t;
        UV_CHECK_OK(uv_tcp_init(uv_loop(), uv_handle));
        uv_handle->data = new Tuple{.agent = this, .bind = (engine_entry->guid == engine_guid_)};

        auto req = new uv_connect_t;
        UV_CHECK_OK(uv_tcp_connect(req, uv_handle, (const struct sockaddr *) &addr,
                                   &Agent::EngineConnectCallback));
    }

    void Agent::RestartPoller() {
        poller_->Start();
    }

    void Agent::StartInternal() {
        // Load agent config file
        std::string config_json;
        CHECK(!config_file_.empty());
        CHECK(fs_utils::ReadContents(config_file_, &config_json))
        << "Failed to read from file " << config_file_;
        config_.Load(config_json);
        auto agent_config = config_.GetAgentConfigByGuid(guid_);
        auto system_config = config_.GetSystemConfig();

        // Create RDMA context
        auto device_name = agent_config->device_name;
        auto device_port = agent_config->device_port;
        auto device_gid_index = agent_config->device_gid_index;
        DCHECK(!device_name.empty());
        DCHECK(device_port != -1);
        infinity_ = new rdma::Infinity(device_name, device_port, device_gid_index);

        // Initialize shared Memory
        shared_memory_ = new rdma::SharedMemory();
        shared_memory_info_ = shared_memory_->GetSharedMemoryInfo();

        // Start IO workers
        auto num_io_workers = agent_config->num_io_workers;
        CHECK_GT(num_io_workers, 0);
        HLOG(INFO) << fmt::format("Start {} IO workers", num_io_workers);
        for (int i = 0; i < num_io_workers; i++) {
            auto io_worker = CreateIOWorker(fmt::format("IO-{}", i));
            io_workers_.push_back(io_worker);
        }

        // Connect to engines
        engine_guid_ = agent_config->bind_engine_guid;
        std::vector<base::Thread *> threads;
        auto engines = config_.GetEngineList();
        std::function<void(void *)> conn_to_engine_fn = absl::bind_front(&Agent::ConnectToEngine, this);
        for (long unsigned int i = 0; i < engines.size(); i++) {
            auto thread = new base::Thread(fmt::format("ConnToE-{}", i), conn_to_engine_fn);
            threads.push_back(thread);
            thread->Start(engines[i]);
        }

        for (auto &thread: threads)
            thread->Join();

        // Initialize Poller
        auto timeout = system_config->timeout_as_ms_for_poller;
        std::function<void()> fn_poller = absl::bind_front(&Agent::OnDataCollectionCallBack, this);
        poller_ = new server::Poller(fn_poller, timeout);

    }

    void Agent::OnDataCollectionCallBack() {
        absl::MutexLock lk(&mutex_for_conn_);
        for (const auto &engine_conn: engine_connections_) {
            auto conn = reinterpret_cast<EngineConnection *>(engine_conn.second.get());
            conn->Execute();
        }
    }

    void Agent::StopInternal() {}

    void Agent::EngineConnectCallback(uv_connect_t *req, int status) {
        auto tuple = reinterpret_cast<Tuple *>(req->handle->data);
        tuple->agent->OnEngineConnect(req, status, tuple->bind);
        delete tuple;
    }

    void Agent::OnEngineConnect(uv_connect_t *req, int status, bool bind) {
        auto reclaim_resources = gsl::finally([req] {
            delete req;
        });

        auto uv_handle = reinterpret_cast<uv_tcp_t *>(req->handle);
        if (status != 0) {
            HLOG(WARNING) << "Failed to connect to Engine: " << uv_strerror(status);
            uv_close(UV_AS_HANDLE(uv_handle), uv::HandleFreeCallback);
            return;
        }
        uint64_t conn_id = next_engine_conn_id_++;
        std::shared_ptr<server::ConnectionBase> connection(new EngineConnection(this, conn_id, bind));
        DCHECK_LT(next_agent_conn_worker_id_, io_workers_.size());
        HLOG(INFO) << fmt::format("New Engine connection (conn_id={}) assigned to IO worker {}",
                                  conn_id, next_agent_conn_worker_id_);

        server::IOWorker *io_worker = io_workers_[next_agent_conn_worker_id_];
        next_agent_conn_worker_id_ = (next_agent_conn_worker_id_ + 1) % io_workers_.size();
        RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(uv_handle));

        DCHECK_GE(connection->id(), 0);
        {
            absl::MutexLock lk(&mutex_for_conn_);
            DCHECK(!engine_connections_.contains(connection->id()));
            engine_connections_[connection->id()] = std::move(connection);
        }
    }

    rdma::MemoryRegionInfo
    Agent::RegisterMemoryRegion(rdma::QueuePair *queue_pair, const std::string_view memory_region_name) {
        rdma::MemoryRegionInfo memory_region_info{};
        {
            absl::MutexLock lk(&mutex_for_shared_memory_);
            queue_pair->RegisterMemoryRegion(memory_region_name, shared_memory_info_.addr, shared_memory_info_.length);
            memory_region_info = queue_pair->GetMemoryRegionInfo(memory_region_name);
        }
        return memory_region_info;
    }

    uint64_t Agent::AllocateMemoryWithLock(uint32_t expected_size) {
        absl::MutexLock lk(&mutex_for_shared_memory_);
        return shared_memory_->AllocateMemory(expected_size);
    }

    void Agent::PopMetaDataEntryWithLock(uint32_t idx) {
        absl::MutexLock lk(&mutex_for_shared_memory_);
        shared_memory_->PopMetaDataEntry(idx);
    }

    void Agent::PushMetaDataEntryWithLock(uint64_t addr, uint32_t size) {
        absl::MutexLock lk(&mutex_for_shared_memory_);
        shared_memory_->PushMetaDataEntry(addr, size);
    }

    uint32_t Agent::PopMessageWithLock(uint32_t &offset, uint32_t &size) {
        uint64_t addr;
        absl::MutexLock lk(&mutex_for_shared_memory_);
        uint32_t idx = shared_memory_->PopMessage(addr, size);
        offset = addr - shared_memory_info_.addr;
        return idx;
    }

    void Agent::GetPreviousOrNextIdxWithLock(uint32_t &idx, bool direction) {
        absl::MutexLock lk(&mutex_for_shared_memory_);
        shared_memory_->GetPreviousOrNextIdx(idx, direction);
    }

}