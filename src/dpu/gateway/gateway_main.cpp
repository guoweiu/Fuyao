#include "base/init.h"
#include "base/common.h"
#include "server.h"

ABSL_FLAG(std::string, config_file, "", "Path to config file");

static std::atomic<faas::gateway::Server *> server_ptr(nullptr);

void SignalHandlerToStopServer(int signal) {
    faas::gateway::Server *server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

int main(int argc, char *argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    faas::base::InitMain(argc, argv);

    auto server = std::make_unique<faas::gateway::Server>();
    server->SetConfigFile(absl::GetFlag(FLAGS_config_file));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();

    return 0;
}
