#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/env_variables.h"
#include "utils/fs.h"
#include "func_worker.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

ABSL_FLAG(std::string, debug_file_path, "", "worker debug file path");

int main(int argc, char *argv[]) {
    std::vector<char *> positional_args;
    faas::base::InitMain(argc, argv, &positional_args);

    std::string faas_root_path_for_ipc, rdma_device_name;
    int faas_func_id, faas_fprocess_id, faas_client_id,
            faas_msg_pipe_id, faas_use_engine_socket, faas_engine_tcp_port,
            rdma_device_port, rdma_device_gid_index, timeout_for_drc;

    std::string debug_file_path = absl::GetFlag(FLAGS_debug_file_path);

    auto func_worker = std::make_unique<faas::worker_cpp::FuncWorker>();

    if (!debug_file_path.empty()) {
        std::string func_config_json;
        faas::fs_utils::ReadContents(debug_file_path, &func_config_json);
        json config = json::parse(func_config_json);

        faas_root_path_for_ipc = config.at("FAAS_ROOT_PATH_FOR_IPC").get<std::string>();
        faas_func_id = config.at("FAAS_FUNC_ID").get<int>();
        faas_fprocess_id = config.at("FAAS_FPROCESS_ID").get<int>();
        faas_use_engine_socket = config.at("FAAS_USE_ENGINE_SOCKET").get<int>();
        faas_engine_tcp_port = config.at("FAAS_ENGINE_TCP_PORT").get<int>();
        rdma_device_name = config.at("RDMA_DEVICE_NAME").get<std::string>();
        rdma_device_port = config.at("RDMA_DEVICE_PORT").get<int>();
        rdma_device_gid_index = config.at("RDMA_DEVICE_GID_INDEX").get<int>();

        func_worker->set_debug_file_path(debug_file_path);
        if (faas_engine_tcp_port != -1) {
            func_worker->set_faas_engine_host(config.at("FAAS_ENGINE_HOST").get<std::string>());
        }
    } else {
        faas_root_path_for_ipc = faas::utils::GetEnvVariable("FAAS_ROOT_PATH_FOR_IPC", "/dev/shm/faas_ipc");
        faas_func_id = faas::utils::GetEnvVariableAsInt("FAAS_FUNC_ID", -1);
        faas_fprocess_id = faas::utils::GetEnvVariableAsInt("FAAS_FPROCESS_ID", -1);
        faas_client_id = faas::utils::GetEnvVariableAsInt("FAAS_CLIENT_ID", 0);
        faas_msg_pipe_id = faas::utils::GetEnvVariableAsInt("FAAS_MSG_PIPE_FD", -1);
        faas_use_engine_socket = faas::utils::GetEnvVariableAsInt("FAAS_USE_ENGINE_SOCKET", 0);
        faas_engine_tcp_port = faas::utils::GetEnvVariableAsInt("FAAS_ENGINE_TCP_PORT", -1);
        rdma_device_name = faas::utils::GetEnvVariable("RDMA_DEVICE_NAME", "mlx5_0");
        rdma_device_port = faas::utils::GetEnvVariableAsInt("RDMA_DEVICE_PORT", 0);
        rdma_device_gid_index = faas::utils::GetEnvVariableAsInt("RDMA_DEVICE_GID_INDEX", 0);
    }

    if (faas_use_engine_socket == 1) {
        func_worker->enable_use_engine_socket();
    } else {
        faas::ipc::SetRootPathForIpc(faas_root_path_for_ipc);
    }

    func_worker->set_func_id(faas_func_id);
    func_worker->set_fprocess_id(faas_fprocess_id);
    func_worker->set_client_id(faas_client_id);
    func_worker->set_message_pipe_fd(faas_msg_pipe_id);

    func_worker->set_engine_tcp_port(faas_engine_tcp_port);
    func_worker->set_rdma_device_name(rdma_device_name);
    func_worker->set_rdma_device_port(rdma_device_port);
    func_worker->set_rdma_device_gid_index(rdma_device_gid_index);
    func_worker->Serve();
    return 0;
}
