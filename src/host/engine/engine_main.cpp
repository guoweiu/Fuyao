#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/docker.h"
#include "utils/fs.h"
#include "utils/env_variables.h"
#include "engine.h"

ABSL_FLAG(std::string, config_file, "", "Path to config file");
ABSL_FLAG(int, guid, 0, "Engine guid");

static std::atomic<faas::engine::Engine *> engine_ptr(nullptr);

static void SignalHandlerToStopEngine(int signal) {
    faas::engine::Engine *engine = engine_ptr.exchange(nullptr);
    if (engine != nullptr) {
        engine->ScheduleStop();
    }
}

int main(int argc, char *argv[]) {
    signal(SIGINT, SignalHandlerToStopEngine);
    faas::base::InitMain(argc, argv);

    std::string cgroup_fs_root(faas::utils::GetEnvVariable("FAAS_CGROUP_FS_ROOT", ""));
    if (cgroup_fs_root.length() > 0) {
        faas::docker_utils::SetCgroupFsRoot(cgroup_fs_root);
    }

    auto engine = std::make_unique<faas::engine::Engine>();
    engine->SetConfigFile(absl::GetFlag(FLAGS_config_file));
    engine->SetGuid(absl::GetFlag(FLAGS_guid));

    engine->Start();
    engine_ptr.store(engine.get());
    engine->WaitForFinish();

    return 0;
}
