#include "engine.h"
#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/docker.h"
#include "utils/socket.h"
#include "runtime/worker_lib.h"
#include "agent_connection.h"
#include <absl/flags/flag.h>

ABSL_FLAG(bool, disable_monitor, false, "");
//ABSL_FLAG(bool, func_worker_use_engine_socket, false, "");
ABSL_FLAG(bool, use_fifo_for_nested_call, false, "");

#define HLOG(l) LOG(l) << "Engine: "
#define HVLOG(l) VLOG(l) << "Engine: "

namespace faas::engine {

    using protocol::FuncCall;
    using protocol::FuncCallDebugString;
    using protocol::Message;
    using protocol::GatewayMessage;
    using protocol::GetFuncCallFromMessage;
    using protocol::GetInlineDataFromMessage;
    using protocol::IsLauncherHandshakeMessage;
    using protocol::IsFuncWorkerHandshakeMessage;
    using protocol::IsInvokeFuncMessage;
    using protocol::IsFuncCallCompleteMessage;
    using protocol::IsFuncCallFailedMessage;
    using protocol::IsInvokeFuncMessageAcrossHosts;
    using protocol::NewHandshakeResponseMessage;
    using protocol::NewFuncCallCompleteGatewayMessage;
    using protocol::NewFuncCallFailedGatewayMessage;
    using protocol::ComputeMessageDelay;

    Engine::Engine()
            : func_worker_use_engine_socket_(false),
              use_fifo_for_nested_call_(absl::GetFlag(FLAGS_use_fifo_for_nested_call)),
              engine_config_(nullptr),
              uv_handle_(nullptr),
              next_gateway_conn_worker_id_(0),
              next_ipc_conn_worker_id_(0),
              next_gateway_conn_id_(0),
              next_agent_conn_id_(0),
              next_agent_conn_worker_id_(0),
              worker_manager_(new WorkerManager(this)),
              monitor_(absl::GetFlag(FLAGS_disable_monitor) ? nullptr : new Monitor(this)),
              tracer_(new Tracer(this)),
              inflight_external_requests_(0),
              last_external_request_timestamp_(-1),
              incoming_external_requests_stat_(
                      stat::Counter::StandardReportCallback("incoming_external_requests")),
              incoming_internal_requests_stat_(
                      stat::Counter::StandardReportCallback("incoming_internal_requests")),
              external_requests_instant_rps_stat_(
                      stat::StatisticsCollector<float>::StandardReportCallback("external_requests_instant_rps")),
              inflight_external_requests_stat_(
                      stat::StatisticsCollector<uint16_t>::StandardReportCallback("inflight_external_requests")),
              message_delay_stat_(
                      stat::StatisticsCollector<int32_t>::StandardReportCallback("message_delay")),
              input_use_shm_stat_(stat::Counter::StandardReportCallback("input_use_shm")),
              output_use_shm_stat_(stat::Counter::StandardReportCallback("output_use_shm")),
              discarded_func_call_stat_(stat::Counter::StandardReportCallback("discarded_func_call")) {

    }

    Engine::~Engine() {
        // Stop Poller
        poller_->Stop();
        delete poller_;
        delete timer_req_;
        delete uv_handle_;
    }

    void Engine::StartInternal() {
        // Load function config file
        CHECK(!config_file_.empty());
        CHECK(fs_utils::ReadContents(config_file_, &config_json_))
        << "Failed to read from file " << config_file_;
        CHECK(config_.Load(config_json_));

        auto system_config = config_.GetSystemConfig();
        auto gateway_config = config_.GetGatewayConfig();
        auto engine_config = config_.GetEngineConfigByGuid(guid_);
        engine_config_ = engine_config;

        timeout_for_drc_ = system_config->timeout_as_ms_for_drc;

        // Set root path for IPC
        auto root_path_for_ipc = engine_config->root_path_for_ipc;
        faas::ipc::SetRootPathForIpc(root_path_for_ipc, /* create= */ true);

        func_worker_use_engine_socket_ = engine_config->func_worker_use_engine_socket != 0;

        // Start IO workers
        auto num_io_workers = engine_config->num_io_workers;
        CHECK_GT(num_io_workers, 0);
        HLOG(INFO) << fmt::format("Start {} IO workers", num_io_workers);
        for (int i = 0; i < num_io_workers; i++) {
            auto io_worker = CreateIOWorker(fmt::format("IO-{}", i));
            io_workers_.push_back(io_worker);
        }

        // Connect to gateway
        auto gateway_conn_per_worker = engine_config->gateway_conn_per_worker;
        auto gateway_addr = gateway_config->gateway_ip;
        auto gateway_port = gateway_config->engine_conn_port;
        auto engine_tcp_port = engine_config->engine_tcp_port;
        auto listen_backlog = engine_config->listen_backlog;
        CHECK_GT(gateway_conn_per_worker, 0);
        CHECK(!gateway_addr.empty());
        CHECK_NE(gateway_port, -1);
        struct sockaddr_in addr{};
        if (!utils::FillTcpSocketAddr(&addr, gateway_addr, gateway_port)) {
            HLOG(FATAL) << "Failed to fill socker address for " << gateway_addr;
        }
        int total_gateway_conn = num_io_workers * gateway_conn_per_worker;
        for (int i = 0; i < total_gateway_conn; i++) {
            auto uv_handle = new uv_tcp_t;
            UV_CHECK_OK(uv_tcp_init(uv_loop(), uv_handle));
            uv_handle->data = this;
            auto req = new uv_connect_t;
            UV_CHECK_OK(uv_tcp_connect(req, uv_handle, (const struct sockaddr *) &addr,
                                       &Engine::GatewayConnectCallback));
        }
        // Listen on ipc_path
        if (engine_tcp_port == -1) {
            auto pipe_handle = new uv_pipe_t;
            UV_CHECK_OK(uv_pipe_init(uv_loop(), pipe_handle, 0));
            pipe_handle->data = this;
            std::string ipc_path(ipc::GetEngineUnixSocketPath());
            if (fs_utils::Exists(ipc_path)) {
                PCHECK(fs_utils::Remove(ipc_path));
            }
            UV_CHECK_OK(uv_pipe_bind(pipe_handle, ipc_path.c_str()));
            HLOG(INFO) << fmt::format("Listen on {} for IPC connections", ipc_path);
            uv_handle_ = UV_AS_STREAM(pipe_handle);
        } else {
            auto tcp_handle = new uv_tcp_t;
            UV_CHECK_OK(uv_tcp_init(uv_loop(), tcp_handle));
            tcp_handle->data = this;
            UV_CHECK_OK(uv_ip4_addr("0.0.0.0", engine_tcp_port, &addr));
            UV_CHECK_OK(uv_tcp_bind(tcp_handle, (const struct sockaddr *) &addr, 0));
            HLOG(INFO) << fmt::format("Listen on 0.0.0.0:{} for IPC connections", engine_tcp_port);
            uv_handle_ = UV_AS_STREAM(tcp_handle);
        }
        UV_CHECK_OK(uv_listen(uv_handle_, listen_backlog, &Engine::MessageConnectionCallback));

        // Listen on address:agent_port for agent connections
        auto agent_conn_port = engine_config->agent_conn_port;
        UV_CHECK_OK(uv_tcp_init(uv_loop(), &uv_agent_conn_handle_));
        uv_agent_conn_handle_.data = this;
        UV_CHECK_OK(uv_ip4_addr("0.0.0.0", agent_conn_port, &addr));
        UV_CHECK_OK(uv_tcp_bind(&uv_agent_conn_handle_, (const struct sockaddr *) &addr, 0));
        HLOG(INFO) << fmt::format("Listen on 0.0.0.0:{} for agent connections", agent_conn_port);
        UV_CHECK_OK(uv_listen(UV_AS_STREAM(&uv_agent_conn_handle_), listen_backlog, &Engine::AgentConnectionCallback));

        // Create RDMA context
        auto device_name = engine_config->device_name;
        auto device_port = engine_config->device_port;
        auto device_gid_index = engine_config->device_gid_index;
        DCHECK(!device_name.empty());

        infinity_ = new rdma::Infinity(device_name, device_port, device_gid_index);

        // Initialize tracer
        tracer_->Init();

        // Initialize poller
        auto timeout = system_config->timeout_as_ms_for_poller;
        std::function<void()> fn_poller = absl::bind_front(&Engine::OnMessageReceiveCallBack, this);
        poller_ = new server::Poller(fn_poller, timeout);

        // Start repeating timer for DRC Reclamation
        auto time_internal = engine_config->time_internal_for_reclaim_drc;
        timer_req_ = new uv_timer_t;
        timer_req_->data = this;
        uv_timer_init(uv_loop(), timer_req_);
        uv_timer_start(timer_req_, ReclaimDRCResources, 2000, time_internal);

    }

    void Engine::OnMessageReceiveCallBack() {
        uint64_t addr;
        uint32_t size;

        mu_for_conn_.Lock(); // require lock
        if (agent_conns_guid_.contains(guid_)) {
            auto conn = agent_conns_guid_[guid_];
            mu_for_conn_.Unlock(); // release
            DCHECK(conn->IsBind());
            auto idx = conn->PopMessageWithLock(addr, size);

            // no messages
            if (idx == -1) return;

            auto message = reinterpret_cast<Message *>(addr);
            this->OnRecvMessage(nullptr, message);

            conn->PopMetaDataEntryWithLock(idx);
        } else {
            mu_for_conn_.Unlock(); // release
            sleep(1);
        }
    }

    void Engine::ReclaimDRCResources(uv_timer_t *req) {
        auto self = reinterpret_cast<Engine *>(req->data);
        self->OnReclaimDRCResources();
    }

    void Engine::OnReclaimDRCResources() {
        Dispatcher *dispatcher;
        uint16_t target_func_id, target_client_id;

        absl::MutexLock lk(&mu_);
        for (auto &message_tuple: message_tuples_) {
            target_func_id = message_tuple.source.func_id;
            target_client_id = message_tuple.source.client_id;

            dispatcher = GetOrCreateDispatcherLocked(target_func_id);
            if (!dispatcher) {
                LOG(ERROR) << "get dispatcher failed, func_id = " << target_func_id;
                continue;
            }

            bool ret = dispatcher->OnRDMAReclaimMessage(message_tuple);
            if (ret) {
                LOG(INFO) << fmt::format(
                        "dispatch reclaim message(target_func_id = {0}, target_client_id = {1}) succeed.",
                        target_func_id,
                        target_client_id);
            } else {
                LOG(WARNING) << fmt::format(
                        "func_worker is busy, dispatch reclaim message(target_func_id = {0}, target_client_id = {1}) failed. Try it later",
                        target_func_id, target_client_id);
            }
        }

    }

    void Engine::StopInternal() {
        uv_close(UV_AS_HANDLE(timer_req_), nullptr);
        uv_close(UV_AS_HANDLE(uv_handle_), nullptr);
        uv_close(UV_AS_HANDLE(&uv_agent_conn_handle_), nullptr);
    }

    void Engine::OnConnectionClose(server::ConnectionBase *connection) {
        DCHECK_IN_EVENT_LOOP_THREAD(uv_loop());
        if (connection->type() == MessageConnection::kTypeId) {
            DCHECK(message_connections_.contains(connection->id()));
            auto message_connection = connection->as_ptr<MessageConnection>();
            if (message_connection->handshake_done()) {
                if (message_connection->is_launcher_connection()) {
                    worker_manager_->OnLauncherDisconnected(message_connection);
                } else {
                    worker_manager_->OnFuncWorkerDisconnected(message_connection);
                }
            }
            message_connections_.erase(connection->id());
            HLOG(INFO) << "A MessageConnection is returned";
        } else if (connection->type() == GatewayConnection::kTypeId) {
            DCHECK(gateway_connections_.contains(connection->id()));
            auto gateway_connection = connection->as_ptr<GatewayConnection>();
            HLOG(WARNING) << fmt::format("Gateway connection (conn_id={}) disconencted",
                                         gateway_connection->conn_id());
            gateway_connections_.erase(connection->id());
        } else if (connection->type() == AgentConnection::kTypeId) {
            absl::MutexLock lk(&mu_for_conn_);
            auto agent_connection = connection->as_ptr<AgentConnection>();
            HLOG(WARNING) << fmt::format("Agent connection (conn_id={}) disconencted",
                                         agent_connection->conn_id());
            agent_conns_guid_.erase(agent_connection->GetEngineGuid());
        } else {
            HLOG(ERROR) << "Unknown connection type!";
        }
    }

    bool Engine::OnNewHandshake(MessageConnection *connection,
                                const Message &handshake_message, Message *response,
                                std::span<const char> *response_payload) {
        if (!IsLauncherHandshakeMessage(handshake_message)
            && !IsFuncWorkerHandshakeMessage(handshake_message)) {
            HLOG(ERROR) << "Received message is not a handshake message";
            return false;
        }
        HLOG(INFO) << "Receive new handshake message from message connection";
        uint16_t func_id = handshake_message.func_id;
        if (config_.FindFunctionByFuncId(func_id) == nullptr) {
            HLOG(ERROR) << "Invalid func_id " << func_id << " in handshake message";
            return false;
        }
        bool success;
        if (IsLauncherHandshakeMessage(handshake_message)) {
            std::span<const char> payload = GetInlineDataFromMessage(handshake_message);
            if (payload.size() != docker_utils::kContainerIdLength) {
                HLOG(ERROR) << "Launcher handshake does not have container ID in inline data";
                return false;
            }
            std::string container_id(payload.data(), payload.size());
            if (monitor_ != nullptr && container_id != docker_utils::kInvalidContainerId) {
                monitor_->OnNewFuncContainer(func_id, container_id);
            }
            success = worker_manager_->OnLauncherConnected(connection);
        } else {
            success = worker_manager_->OnFuncWorkerConnected(connection);
            ProcessDiscardedFuncCallIfNecessary();
        }
        if (!success) {
            return false;
        }
        if (IsLauncherHandshakeMessage(handshake_message)) {

            *response = NewHandshakeResponseMessage(config_json_.size());
            if (func_worker_use_engine_socket_) {
                response->flags |= protocol::kFuncWorkerUseEngineSocketFlag;
            }
            *response_payload = std::span<const char>(config_json_.data(),
                                                      config_json_.size());
        } else {
            *response = NewHandshakeResponseMessage(0);
            response->timeout_for_drc = timeout_for_drc_;
            if (use_fifo_for_nested_call_) {
                response->flags |= protocol::kUseFifoForNestedCallFlag;
            }
            *response_payload = std::span<const char>();
        }
        return true;
    }

    void Engine::OnRecvGatewayOrAgentMessage(server::ConnectionBase *connection, const GatewayMessage &message,
                                             std::span<const char> payload) {
        if (IsDispatchFuncCallMessage(message)) {
            const FuncCall func_call = GetFuncCallFromMessage(message, guid_);
            OnExternalFuncCall(func_call, payload);
        } else {
            HLOG(ERROR) << "Unknown engine message type";
        }
    }

    // TODO: need to fix
    AgentConnection *Engine::RandomChooseAgentConnExceptSelf() {
        auto engines = this->config_.GetEngineList();
        auto engine_num = engines.size();

        if (engine_num < 2) {
            return nullptr;
        } else {
            absl::MutexLock lk(&mu_for_conn_);
            auto random_num = GetMilliTimestamp() % engine_num;
            if (engines[random_num]->guid == guid_)
                random_num = (random_num + 1) % engine_num;
            auto choose_engine = engines[random_num];
            auto conn = agent_conns_guid_[choose_engine->guid].get();
            return conn;
        }
    }

    // TODO: need to fix, payload size than 1K ?
    void Engine::PushMessageIntoSharedMemoryWithLock(AgentConnection *conn, protocol::Message *message,
                                                     protocol::MessageType message_type) const {
        if (!conn) {
            LOG(WARNING) << "No Agent-connection!";
            return;
        }

        uint32_t expected_size = sizeof(Message);
        uint64_t allocated_addr = conn->AllocateMemoryWithLock(expected_size);

        if (!allocated_addr) {
            LOG(WARNING) << "Allocated memory failed!";
            return;
        }

        auto new_message = reinterpret_cast<Message *>(allocated_addr);
        *new_message = *message;
        new_message->message_type = static_cast<uint16_t>(message_type);

        if (message_type == protocol::MessageType::INVOKE_FUNC)
            new_message->engine_guid = this->guid_;

        conn->PushMetaDataEntryWithLock(allocated_addr, expected_size);

        DLOG(INFO) << fmt::format(
                    "Push new message[func_id={},client_id={},call_id={}] into RDMA Shared Memory",
                    (uint16_t) message->func_id, (uint16_t) message->client_id, (uint32_t) message->call_id);

    }

    void Engine::OnRecvMessage(MessageConnection *connection, Message *message) {
        int32_t message_delay = ComputeMessageDelay(*message);

        FuncCall func_call = GetFuncCallFromMessage(*message);
        FuncCall parent_func_call{};
        parent_func_call.full_call_id = message->parent_call_id;
        auto message_tuple = protocol::GetMessageTupleFromMessage(*message);

        // RDMA request
        if (IsInvokeFuncMessage(*message) /* DRC_OVER_IPC */ ||
            IsInvokeFuncMessageAcrossHosts(*message) /* DRC_OVER_Fabric */) {
            // only source be recorded
            if (protocol::IsRDMARequestMessage(*message) && DoesMessageBelongToMe(message)) {
                absl::MutexLock lk(&mu_);
                if (message_tuples_.find(message_tuple) == message_tuples_.end()) {
                    message_tuples_.insert(message_tuple);
                } else {
                    LOG(WARNING) << fmt::format("MessageTuple(source_func_id = {}, "
                                                "source_client_id = {}) already existed.",
                                                message_tuple.source.func_id,
                                                message_tuple.source.client_id);
                }
            }
        }

        if (IsInvokeFuncMessage(*message)) {
            uint16_t func_id = message->func_id;
            Dispatcher *dispatcher = nullptr;
            {
                absl::MutexLock lk(&mu_);
                incoming_internal_requests_stat_.Tick();
                if (message->payload_size < 0) {
                    input_use_shm_stat_.Tick();
                }
                if (message_delay >= 0) {
                    message_delay_stat_.AddSample(message_delay);
                }
                dispatcher = GetOrCreateDispatcherLocked(func_id);
            }

            bool success = false;
            if (dispatcher != nullptr) {
                if (message->payload_size < 0) {
                    success = dispatcher->OnNewFuncCall(
                            func_call, parent_func_call,
                            /* input_size= */ gsl::narrow_cast<size_t>(-message->payload_size),
                            std::span<const char>(), /* shm_input= */ true);
                } else {
                    success = dispatcher->OnNewFuncCall(
                            func_call, parent_func_call,
                            /* input_size= */ gsl::narrow_cast<size_t>(message->payload_size),
                            GetInlineDataFromMessage(*message), /* shm_input= */ false);
                }
            }
            if (!success) {
                HLOG(ERROR) << "Dispatcher failed for func_id " << func_id;
            }

        } else if (IsInvokeFuncMessageAcrossHosts(*message)) {
            auto input_size = message->payload_size < 0 ? -message->payload_size : message->payload_size;
            this->tracer()->OnNewFuncCall(func_call, parent_func_call, input_size);
            auto conn = RandomChooseAgentConnExceptSelf();
            PushMessageIntoSharedMemoryWithLock(conn, message, protocol::MessageType::INVOKE_FUNC);
            conn->Notification();

        } else if (IsFuncCallCompleteMessage(*message) || IsFuncCallFailedMessage(*message)) {
            Dispatcher *dispatcher = nullptr;
            std::unique_ptr<ipc::ShmRegion> input_region = nullptr;
            {
                absl::MutexLock lk(&mu_);
                if (message_delay >= 0) {
                    message_delay_stat_.AddSample(message_delay);
                }
                if (IsFuncCallCompleteMessage(*message)) {
                    if ((func_call.client_id == 0 && message->payload_size < 0)
                        || (func_call.client_id > 0
                            && message->payload_size + sizeof(int32_t) > PIPE_BUF)) {
                        output_use_shm_stat_.Tick();
                    }
                }
                if (func_call.client_id == 0) {
                    input_region = GrabExternalFuncCallShmInput(func_call);
                }

                dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
            }
            bool success = false;
            if (dispatcher != nullptr) {
                if (protocol::IsRDMAReclaimCompletionMessage(*message)) {
                    {
                        absl::MutexLock lk(&mu_);
                        DCHECK(message_tuples_.find(message_tuple) != message_tuples_.end());
                        if (!message->flags) {
                            message_tuples_.erase(message_tuple);
                        }
                    }
                    dispatcher->OnRDMAReclaimCompletionMessage(func_call);
                } else if (protocol::IsRDMAResponseCompleteMessage(*message)) {
                    dispatcher->OnRDMAReclaimCompletionMessage(func_call);
                } else if (IsFuncCallCompleteMessage(*message)) {
                    success = dispatcher->OnFuncCallCompleted(
                            func_call, message->processing_time, message->dispatch_delay,
                            /* output_size= */ gsl::narrow_cast<size_t>(std::abs(message->payload_size)),
                            protocol::IsRDMARequestMessage(*message));
                    if (success && func_call.client_id == 0) {
                        if (message->payload_size < 0) {
                            auto output_region = ipc::ShmOpen(
                                    ipc::GetFuncCallOutputShmName(func_call.full_call_id));
                            if (output_region == nullptr) {
                                ExternalFuncCallFailed(func_call);
                            } else {
                                output_region->EnableRemoveOnDestruction();
                                ExternalFuncCallCompleted(func_call, output_region->to_span(),
                                                          message->processing_time);
                            }
                        } else {
                            ExternalFuncCallCompleted(func_call, GetInlineDataFromMessage(*message),
                                                      message->processing_time);
                        }
                    }
                } else {
                    success = dispatcher->OnFuncCallFailed(func_call, message->dispatch_delay);
                    if (success && func_call.client_id == 0) {
                        ExternalFuncCallFailed(func_call);
                    }
                }
            }
            if (success && func_call.client_id > 0 && !use_fifo_for_nested_call_) {
                Message message_copy = *message;
                if (!DoesMessageBelongToMe(message)) { // for across-host response
                    auto conn = GetAgentConnsByGuidWithLock(message->engine_guid);
                    PushMessageIntoSharedMemoryWithLock(conn, message,
                                                        static_cast<protocol::MessageType>(message->message_type));
                    conn->Notification();
                } else {
                    worker_manager_->GetFuncWorker(func_call.client_id)->SendMessage(&message_copy);
                }
            }
        } else {
            LOG(ERROR) << "Unknown message type!";
        }
        ProcessDiscardedFuncCallIfNecessary();
    }

    void Engine::OnExternalFuncCall(const FuncCall &func_call, std::span<const char> input) {
        inflight_external_requests_.fetch_add(1);
        std::unique_ptr<ipc::ShmRegion> input_region = nullptr;
        if (input.size() > MESSAGE_INLINE_DATA_SIZE) {
            input_region = ipc::ShmCreate(
                    ipc::GetFuncCallInputShmName(func_call.full_call_id), input.size());
            if (input_region == nullptr) {
                ExternalFuncCallFailed(func_call);
                return;
            }
            input_region->EnableRemoveOnDestruction();
            if (input.size() > 0) {
                memcpy(input_region->base(), input.data(), input.size());
            }
        }
        Dispatcher *dispatcher = nullptr;
        {
            absl::MutexLock lk(&mu_);
            incoming_external_requests_stat_.Tick();
            int64_t current_timestamp = GetMonotonicMicroTimestamp();
            if (current_timestamp <= last_external_request_timestamp_) {
                current_timestamp = last_external_request_timestamp_ + 1;
            }
            if (last_external_request_timestamp_ != -1) {
                external_requests_instant_rps_stat_.AddSample(gsl::narrow_cast<float>(
                        1e6 / (current_timestamp - last_external_request_timestamp_)));
            }
            last_external_request_timestamp_ = current_timestamp;
            inflight_external_requests_stat_.AddSample(
                    gsl::narrow_cast<uint16_t>(inflight_external_requests_.load()));
            dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
            if (input_region != nullptr) {
                if (dispatcher != nullptr) {
                    external_func_call_shm_inputs_[func_call.full_call_id] = std::move(input_region);
                }
                input_use_shm_stat_.Tick();
            }
        }
        if (dispatcher == nullptr) {
            ExternalFuncCallFailed(func_call);
            return;
        }
        bool success = false;
        if (input.size() <= MESSAGE_INLINE_DATA_SIZE) {
            success = dispatcher->OnNewFuncCall(
                    func_call, protocol::kInvalidFuncCall,
                    input.size(), /* inline_input= */ input, /* shm_input= */ false);
        } else {
            success = dispatcher->OnNewFuncCall(
                    func_call, protocol::kInvalidFuncCall,
                    input.size(), /* inline_input= */ std::span<const char>(), /* shm_input= */ true);
        }
        if (!success) {
            {
                absl::MutexLock lk(&mu_);
                input_region = GrabExternalFuncCallShmInput(func_call);
            }
            ExternalFuncCallFailed(func_call);
        }
    }

    void Engine::ExternalFuncCallCompleted(const protocol::FuncCall &func_call,
                                           std::span<const char> output, int32_t processing_time) {
        inflight_external_requests_.fetch_add(-1);
        server::IOWorker *io_worker = server::IOWorker::current();
        DCHECK(io_worker != nullptr);
        server::ConnectionBase *gateway_connection = io_worker->PickConnection(
                GatewayConnection::kTypeId);
        if (gateway_connection == nullptr) {
            HLOG(ERROR) << "There is not GatewayConnection associated with current IOWorker";
            return;
        }
        GatewayMessage message = NewFuncCallCompleteGatewayMessage(func_call, processing_time);
        message.payload_size = output.size();
        gateway_connection->as_ptr<GatewayConnection>()->SendMessage(message, output);
    }

    void Engine::ExternalFuncCallFailed(const protocol::FuncCall &func_call, int status_code) {
        inflight_external_requests_.fetch_add(-1);
        server::IOWorker *io_worker = server::IOWorker::current();
        DCHECK(io_worker != nullptr);
        server::ConnectionBase *gateway_connection = io_worker->PickConnection(
                GatewayConnection::kTypeId);
        if (gateway_connection == nullptr) {
            HLOG(ERROR) << "There is not GatewayConnection associated with current IOWorker";
            return;
        }
        GatewayMessage message = NewFuncCallFailedGatewayMessage(func_call, status_code);
        gateway_connection->as_ptr<GatewayConnection>()->SendMessage(message);
    }

    Dispatcher *Engine::GetOrCreateDispatcher(uint16_t func_id) {
        absl::MutexLock lk(&mu_);
        Dispatcher *dispatcher = GetOrCreateDispatcherLocked(func_id);
        return dispatcher;
    }

    Dispatcher *Engine::GetOrCreateDispatcherLocked(uint16_t func_id) {
        if (dispatchers_.contains(func_id)) {
            return dispatchers_[func_id].get();
        }
        if (config_.FindFunctionByFuncId(func_id) != nullptr) {
            dispatchers_[func_id] = std::make_unique<Dispatcher>(this, func_id);
            return dispatchers_[func_id].get();
        } else {
            return nullptr;
        }
    }

    std::unique_ptr<ipc::ShmRegion> Engine::GrabExternalFuncCallShmInput(const FuncCall &func_call) {
        std::unique_ptr<ipc::ShmRegion> ret = nullptr;
        if (external_func_call_shm_inputs_.contains(func_call.full_call_id)) {
            ret = std::move(external_func_call_shm_inputs_[func_call.full_call_id]);
            external_func_call_shm_inputs_.erase(func_call.full_call_id);
        }
        return ret;
    }

    void Engine::DiscardFuncCall(const FuncCall &func_call) {
        absl::MutexLock lk(&mu_);
        discarded_func_calls_.push_back(func_call);
        discarded_func_call_stat_.Tick();
    }

    void Engine::ProcessDiscardedFuncCallIfNecessary() {
        std::vector<std::unique_ptr<ipc::ShmRegion>> discarded_input_regions;
        std::vector<FuncCall> discarded_external_func_calls;
        std::vector<FuncCall> discarded_internal_func_calls;
        {
            absl::MutexLock lk(&mu_);
            for (const FuncCall &func_call: discarded_func_calls_) {
                if (func_call.client_id == 0) {
                    auto shm_input = GrabExternalFuncCallShmInput(func_call);
                    if (shm_input != nullptr) {
                        discarded_input_regions.push_back(std::move(shm_input));
                    }
                    discarded_external_func_calls.push_back(func_call);
                } else {
                    discarded_internal_func_calls.push_back(func_call);
                }
            }
            discarded_func_calls_.clear();
        }
        for (const FuncCall &func_call: discarded_external_func_calls) {
            ExternalFuncCallFailed(func_call);
        }
        if (!discarded_internal_func_calls.empty()) {
            char pipe_buf[PIPE_BUF];
            Message dummy_message;
            for (const FuncCall &func_call: discarded_internal_func_calls) {
                if (use_fifo_for_nested_call_) {
                    worker_lib::FifoFuncCallFinished(
                            func_call, /* success= */ false, /* output= */ std::span<const char>(),
                            /* processing_time= */ 0, pipe_buf, &dummy_message);
                } else {
                    // TODO: handle this case
                }
            }
        }
    }

    void Engine::SetAgentConnsGuidWithLock(uint16_t guid, AgentConnection *conn) {
        absl::MutexLock lk(&mu_for_conn_);
        DCHECK(!agent_conns_guid_.contains(guid));
        agent_conns_guid_[guid] = std::shared_ptr<AgentConnection>(conn);
    }

    void Engine::SetMemoryRegionsInfoWithLock(AgentConnection *conn, rdma::MemoryRegionInfo *memory_region_info) {
        absl::MutexLock lk(&mu_);
        DCHECK(!memory_regions_info_.contains(conn->conn_id_));
        memory_regions_info_[conn->conn_id_] = memory_region_info;
    }

    AgentConnection *Engine::GetAgentConnsByGuidWithLock(uint16_t guid) {
        absl::MutexLock lk(&mu_for_conn_);
        AgentConnection *conn = nullptr;
        conn = agent_conns_guid_[guid].get();
        return conn;
    }

    bool Engine::DoesMessageBelongToMe(protocol::Message *message) const {
        return (message->engine_guid == this->guid_) || (message->engine_guid == 0);
    }

    UV_CONNECT_CB_FOR_CLASS(Engine, GatewayConnect) {
        auto uv_handle = reinterpret_cast<uv_tcp_t *>(req->handle);
        free(req);
        if (status != 0) {
            HLOG(WARNING) << "Failed to connect to gateway: " << uv_strerror(status);
            uv_close(UV_AS_HANDLE(uv_handle), uv::HandleFreeCallback);
            return;
        }
        uint16_t conn_id = next_gateway_conn_id_++;
        std::shared_ptr<server::ConnectionBase> connection(new GatewayConnection(this, conn_id));
        DCHECK_LT(next_gateway_conn_worker_id_, io_workers_.size());
        HLOG(INFO) << fmt::format("New gateway connection (conn_id={}) assigned to IO worker {}",
                                  conn_id, next_gateway_conn_worker_id_);
        server::IOWorker *io_worker = io_workers_[next_gateway_conn_worker_id_];
        next_gateway_conn_worker_id_ = (next_gateway_conn_worker_id_ + 1) % io_workers_.size();
        RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(uv_handle));
        DCHECK_GE(connection->id(), 0);
        DCHECK(!gateway_connections_.contains(connection->id()));
        gateway_connections_[connection->id()] = std::move(connection);
    }

    UV_CONNECTION_CB_FOR_CLASS(Engine, MessageConnection) {
        if (status != 0) {
            HLOG(WARNING) << "Failed to open message connection: " << uv_strerror(status);
            return;
        }
        HLOG(INFO) << "New message connection";
        std::shared_ptr<server::ConnectionBase> connection(new MessageConnection(this));
        uv_stream_t *client;
        if (engine_config_->engine_tcp_port == -1) {
            client = UV_AS_STREAM(malloc(sizeof(uv_pipe_t)));
            UV_DCHECK_OK(uv_pipe_init(uv_loop(), reinterpret_cast<uv_pipe_t *>(client), 0));
        } else {
            client = UV_AS_STREAM(malloc(sizeof(uv_tcp_t)));
            UV_DCHECK_OK(uv_tcp_init(uv_loop(), reinterpret_cast<uv_tcp_t *>(client)));
        }
        if (uv_accept(uv_handle_, client) == 0) {
            DCHECK_LT(next_ipc_conn_worker_id_, io_workers_.size());
            server::IOWorker *io_worker = io_workers_[next_ipc_conn_worker_id_];
            next_ipc_conn_worker_id_ = (next_ipc_conn_worker_id_ + 1) % io_workers_.size();
            RegisterConnection(io_worker, connection.get(), client);
            DCHECK_GE(connection->id(), 0);
            DCHECK(!message_connections_.contains(connection->id()));
            message_connections_[connection->id()] = std::move(connection);
        } else {
            LOG(ERROR) << "Failed to accept new message connection";
            free(client);
        }
    }

    UV_CONNECTION_CB_FOR_CLASS(Engine, AgentConnection) {
        if (status != 0) {
            HLOG(WARNING) << "Failed to open agent connection: " << uv_strerror(status);
            return;
        }
        HLOG(INFO) << "New Agent connection";

        auto client = new uv_tcp_t;
        UV_DCHECK_OK(uv_tcp_init(uv_loop(), client));

        uint16_t conn_id = next_agent_conn_id_++;
        auto connection = new AgentConnection(this, conn_id);

        if (uv_accept(UV_AS_STREAM(&uv_agent_conn_handle_), UV_AS_STREAM(client)) == 0) {
            DCHECK_LT(next_agent_conn_worker_id_, io_workers_.size());
            server::IOWorker *io_worker = io_workers_[next_agent_conn_worker_id_];
            next_agent_conn_worker_id_ = (next_agent_conn_worker_id_ + 1) % io_workers_.size();
            RegisterConnection(io_worker, connection, UV_AS_STREAM(client));
        } else {
            LOG(ERROR) << "Failed to accept new Agent connection";
            free(client);
        }
    }

}  // namespace faas
