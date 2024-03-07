//
// Created by LSwor on 2022/9/8.
//

#include "base/common.h"
#include "base/init.h"
#include "agent.h"

ABSL_FLAG(std::string, config_file, "", "Path to config file");
ABSL_FLAG(int, guid, 0, "Agent guid");

static std::atomic<faas::agent::Agent *> agent_ptr(nullptr);

static void SignalHandlerToStopAgent(int signal) {
    faas::agent::Agent *agent = agent_ptr.exchange(nullptr);
    if (agent != nullptr) {
        agent->ScheduleStop();
    }
}

int main(int argc, char *argv[]) {
    signal(SIGINT, SignalHandlerToStopAgent);
    faas::base::InitMain(argc, argv);

    auto agent = std::make_unique<faas::agent::Agent>();
    agent->SetConfigFile(absl::GetFlag(FLAGS_config_file));
    agent->SetGuid(absl::GetFlag(FLAGS_guid));

    agent->Start();
    agent_ptr.store(agent.get());
    agent->WaitForFinish();

    return 0;
}