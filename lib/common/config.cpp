#include "common/config.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace faas::config {

    namespace {
        bool StartsWith(std::string_view s, std::string_view prefix) {
            return s.find(prefix) == 0;
        }

        std::string_view StripPrefix(std::string_view s, std::string_view prefix) {
            if (!StartsWith(s, prefix)) {
                return s;
            } else {
                return s.substr(prefix.length());
            }
        }
    }

    bool Config::ValidateFuncId(int func_id) {
        return 0 < func_id && func_id <= kMaxFuncId;
    }

    bool Config::ValidateFuncName(std::string_view func_name) {
        if (StartsWith(func_name, "grpc:")) {
            // gRPC service
            std::string_view service_name = StripPrefix(func_name, "grpc:");
            for (const char &ch: service_name) {
                if (!(('0' <= ch && ch <= '9') ||
                      ('a' <= ch && ch <= 'z') ||
                      ('A' <= ch && ch <= 'Z') ||
                      ch == '.' || ch == '_')) {
                    return false;
                }
            }
        } else {
            // Normal function
            for (const char &ch: func_name) {
                if (!(('0' <= ch && ch <= '9') ||
                      ('a' <= ch && ch <= 'z') ||
                      ('A' <= ch && ch <= 'Z'))) {
                    return false;
                }
            }
        }
        return true;
    }

    bool Config::Load(std::string_view json_contents) {
        json config;

        // parse contents
        try {
            config = json::parse(json_contents);
        } catch (const json::parse_error &e) {
            LOG(ERROR) << "Failed to parse json: " << e.what();
            return false;
        }

        try {
            if (!config.contains("System")
                || !config.contains("Gateway")
                || !config.contains("Engines")
                || !config.contains("Agents")
                || !config.contains("Functions")) {
                LOG(ERROR) << "config file should contains fields: System Gateway Engines Agents Functions";
                return false;
            }

            auto system = config["System"];
            auto gateway = config["Gateway"];
            auto engines = config["Engines"];
            auto agents = config["Agents"];
            auto functions = config["Functions"];

            // system
            {
                system_ = std::make_unique<System>();
                system_->timeout_as_ms_for_poller = 12000; // ms
                system_->timeout_as_ms_for_drc = 60000; // ms
                system_->poll_time_internal_as_us = 100; //us
                system_->memory_region_size = 1048576; // 1MB
                system_->message_size_of_fabric = 1024; // 1KB

                if (system.contains("timeoutAsMsForPoller")) {
                    system_->timeout_as_ms_for_poller = system.at("timeoutAsMsForPoller").get<uint64_t>();
                }
                if(system.contains("timeoutAsMsForDRC")){
                    system_->timeout_as_ms_for_drc = system.at("timeoutAsMsForDRC").get<uint64_t>();
                }
                if (system.contains("pollTimeInternalAsUs")) {
                    system_->timeout_as_ms_for_poller = system.at("timeoutAsMsForPoller").get<uint64_t>();
                }
                if (system.contains("memoryRegionSize")) {
                    system_->memory_region_size = system.at("memoryRegionSize").get<uint64_t>();
                }
                if (system.contains("messageSizeOfFabric")) {
                    system_->message_size_of_fabric = system.at("messageSizeOfFabric").get<uint64_t>();
                }
            }

            // gateway
            {
                gateway_ = std::make_unique<Gateway>();
                gateway_->gateway_ip = "0.0.0.0";
                gateway_->http_port = 8080;
                gateway_->grpc_port = 50051;
                gateway_->engine_conn_port = 10012;
                gateway_->num_io_workers = 1;
                gateway_->listen_backlog = 64;

                if (gateway.contains("gatewayIP"))
                    gateway_->gateway_ip = gateway.at("gatewayIP").get<std::string>();

                if (gateway.contains("httpPort"))
                    gateway_->http_port = gateway.at("httpPort").get<int>();

                if (gateway.contains("grpcPort"))
                    gateway_->grpc_port = gateway.at("grpcPort").get<int>();

                if (gateway.contains("engineConnPort"))
                    gateway_->engine_conn_port = gateway.at("engineConnPort").get<int>();

                if (gateway.contains("numIOWorkers"))
                    gateway_->num_io_workers = gateway.at("numIOWorkers").get<int>();

                if (gateway.contains("listenBacklog"))
                    gateway_->listen_backlog = gateway.at("listenBacklog").get<int>();

            }

            // engines
            {
                if (!engines.is_array()) {
                    LOG(ERROR) << "Engines is not array";
                    return false;
                }

                for (const auto &item: engines) {
                    auto engine = std::make_unique<EngineEntry>();
                    engine->guid = 0;
                    engine->engine_ip = "0.0.0.0";
                    engine->agent_conn_port = 10020;
                    engine->func_worker_use_engine_socket = 0;
                    engine->engine_tcp_port = -1;
                    engine->num_io_workers = 1;
                    engine->listen_backlog = 64;
                    engine->gateway_conn_per_worker = 2;
                    engine->root_path_for_ipc = "/dev/shm/faas_ipc";
                    engine->device_name = "mlx5_0";
                    engine->device_port = 0;
                    engine->device_gid_index = 0;
                    engine->time_internal_for_reclaim_drc = 60000;  // 60s

                    if (item.contains("guid"))
                        engine->guid = item.at("guid").get<uint16_t>();

                    if (item.contains("engineIP"))
                        engine->engine_ip = item.at("engineIP").get<std::string>();

                    if (item.contains("agentConnPort"))
                        engine->agent_conn_port = item.at("agentConnPort").get<int>();

                    if(item.contains("funcWorkerUseEngineSocket"))
                        engine->func_worker_use_engine_socket = item.at("funcWorkerUseEngineSocket").get<int>();

                    if (item.contains("engineTCPPort"))
                        engine->engine_tcp_port = item.at("engineTCPPort").get<int>();

                    if (item.contains("numIOWorkers"))
                        engine->num_io_workers = item.at("numIOWorkers").get<int>();

                    if (item.contains("listenBacklog"))
                        engine->listen_backlog = item.at("listenBacklog").get<int>();

                    if (item.contains("gatewayConnPerWorker"))
                        engine->gateway_conn_per_worker = item.at("gatewayConnPerWorker").get<int>();

                    if (item.contains("rootPathForIPC"))
                        engine->root_path_for_ipc = item.at("rootPathForIPC").get<std::string>();

                    if (item.contains("deviceName"))
                        engine->device_name = item.at("deviceName").get<std::string>();

                    if (item.contains("devicePort"))
                        engine->device_port = item.at("devicePort").get<int>();

                    if (item.contains("deviceGidIndex"))
                        engine->device_gid_index = item.at("deviceGidIndex").get<int>();

                    if (item.contains("timeInternalForReclaimDRC"))
                        engine->time_internal_for_reclaim_drc = item.at("timeInternalForReclaimDRC").get<uint64_t>();

                    if (engine_entries_.count(engine->guid)) {
                        LOG(ERROR) << "Duplicated Engine guid";
                        return false;
                    }

                    engines_.push_back(engine.get());
                    engine_entries_[engine->guid] = std::move(engine);
                }
            }

            // agents
            {
                if (!agents.is_array()) {
                    LOG(ERROR) << "Agents is not array";
                    return false;
                }

                for (const auto &item: agents) {
                    auto agent = std::make_unique<AgentEntry>();
                    agent->guid = 0;
                    agent->device_name = "mlx5_0";
                    agent->device_port = 0;
                    agent->device_gid_index = 0;
                    agent->num_io_workers = 1;
                    agent->bind_engine_guid = 0;

                    if (item.contains("guid"))
                        agent->guid = item.at("guid").get<uint16_t>();

                    if (item.contains("deviceName"))
                        agent->device_name = item.at("deviceName").get<std::string>();

                    if (item.contains("devicePort"))
                        agent->device_port = item.at("devicePort").get<int>();

                    if (item.contains("deviceGidIndex"))
                        agent->device_gid_index = item.at("deviceGidIndex").get<int>();

                    if (item.contains("numIOWorkers"))
                        agent->num_io_workers = item.at("numIOWorkers").get<int>();

                    if (item.contains("BindEngineGuid"))
                        agent->bind_engine_guid = item.at("BindEngineGuid").get<uint16_t>();

                    if (!engine_entries_.count(agent->bind_engine_guid)) {
                        LOG(ERROR) << "BindEngineGuid illegal: engine guid not exist";
                        return false;
                    }

                    if (bind_engines_.count(agent->bind_engine_guid)) {
                        LOG(ERROR) << "BindEngineGuid illegal: duplicated";
                        return false;
                    }

                    bind_engines_.insert(agent->bind_engine_guid);

                    agent_entries_[agent->guid] = std::move(agent);
                }
            }

            if (engine_entries_.size() != agent_entries_.size()) {
                LOG(ERROR) << "the number of engines is not equal to the number of agents";
                return false;
            }

            // functions
            {
                if (!functions.is_array()) {
                    LOG(ERROR) << "Functions is not array";
                    return false;
                }

                for (const auto &item: functions) {
                    std::string func_name = item.at("funcName").get<std::string>();
                    if (!ValidateFuncName(func_name)) {
                        LOG(ERROR) << "Invalid func_name: " << func_name;
                        return false;
                    }
                    if (entries_by_func_name_.count(func_name) > 0) {
                        LOG(ERROR) << "Duplicate func_name: " << func_name;
                        return false;
                    }
                    int func_id = item.at("funcId").get<int>();
                    if (!ValidateFuncId(func_id)) {
                        LOG(ERROR) << "Invalid func_id: " << func_id;
                        return false;
                    }
                    if (entries_by_func_id_.count(func_id) > 0) {
                        LOG(ERROR) << "Duplicate func_id: " << func_id;
                        return false;
                    }
                    auto entry = std::make_unique<FunctionEntry>();
                    entry->func_name = func_name;
                    entry->func_id = func_id;
                    entry->min_workers = -1;
                    entry->max_workers = -1;
                    if (item.contains("minWorkers")) {
                        entry->min_workers = item.at("minWorkers").get<int>();
                    }
                    if (item.contains("maxWorkers")) {
                        entry->max_workers = item.at("maxWorkers").get<int>();
                    }
                    entry->allow_http_get = false;
                    entry->qs_as_input = false;
                    entry->is_grpc_service = false;
                    if (StartsWith(func_name, "grpc:")) {
                        std::string_view service_name = StripPrefix(func_name, "grpc:");
                        entry->is_grpc_service = true;
                        entry->grpc_service_name = std::string(service_name);
                        LOG(INFO) << "Load configuration for gRPC service " << service_name
                                  << "[" << func_id << "]";
                        const json &grpc_methods = item.at("grpcMethods");
                        if (!grpc_methods.is_array()) {
                            LOG(ERROR) << "grpcMethods field is not array";
                            return false;
                        }
                        for (const auto &method: grpc_methods) {
                            int method_id = entry->grpc_methods.size();
                            if (method_id > kMaxMethodId) {
                                LOG(ERROR) << "More than " << kMaxMethodId << " methods for gRPC service "
                                           << service_name;
                                return false;
                            }
                            std::string method_name = method.get<std::string>();
                            if (entry->grpc_method_ids.count(method_name) > 0) {
                                LOG(ERROR) << "Duplicate method " << method_name << " for gRPC service "
                                           << service_name;
                                return false;
                            } else {
                                LOG(INFO) << "Register method " << method_name << " for gRPC service "
                                          << service_name;
                                entry->grpc_methods.push_back(method_name);
                                entry->grpc_method_ids[method_name] = method_id;
                            }
                        }
                    } else {
                        LOG(INFO) << "Load configuration for function " << func_name
                                  << "[" << func_id << "]";
                        if (item.contains("allowHttpGet") && item.at("allowHttpGet").get<bool>()) {
                            LOG(INFO) << "Allow HTTP GET enabled for " << func_name;
                            entry->allow_http_get = true;
                        }
                        if (item.contains("qsAsInput") && item.at("qsAsInput").get<bool>()) {
                            LOG(INFO) << "Query string used as input for " << func_name;
                            entry->qs_as_input = true;
                        }
                    }
                    entries_by_func_name_[func_name] = entry.get();
                    entries_by_func_id_[func_id] = entry.get();
                    func_entries_.push_back(std::move(entry));
                }

            }

        } catch (const json::exception &e) {
            LOG(ERROR) << "Invalid config file: " << e.what();
            return false;
        }
        return true;
    }

    const FunctionEntry *Config::FindFunctionByFuncName(std::string_view func_name) {
        if (entries_by_func_name_.count(std::string(func_name)) > 0) {
            return entries_by_func_name_.at(std::string(func_name));
        } else {
            return nullptr;
        }
    }

    const FunctionEntry * Config::FindFunctionByFuncId(int func_id) const {
        if (entries_by_func_id_.count(func_id) > 0) {
            return entries_by_func_id_.at(func_id);
        } else {
            return nullptr;
        }
    }

    const Gateway *Config::GetGatewayConfig() {
        return gateway_.get();
    }

    const EngineEntry *Config::GetEngineConfigByGuid(uint64_t guid) {
        if (engine_entries_.count(guid)) {
            return engine_entries_.at(guid).get();
        } else {
            return nullptr;
        }
    }

    const AgentEntry *Config::GetAgentConfigByGuid(uint64_t guid) {
        if (agent_entries_.count(guid)) {
            return agent_entries_.at(guid).get();
        } else {
            return nullptr;
        }
    }

    const System *Config::GetSystemConfig() {
        return system_.get();
    }

    std::vector<EngineEntry *> Config::GetEngineList() {
        return engines_;
    }


}  // namespace faas
