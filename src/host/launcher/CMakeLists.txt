cmake_minimum_required(VERSION 3.16)
project(launcher)
set(CMAKE_CXX_STANDARD 17)

set(LAUNCHER_ABSL
        absl::flags
        absl::random_random
        absl::bind_front
        absl::failure_signal_handler
        absl::flags_parse)

set(LAUNCHER_LIBRARIES
        ${LAUNCHER_ABSL}
        fmt
        GSL
        nlohmann_json
        uv_a
        nghttp2
        pthread)

set(LAUNCHER_SOURCE_FILES
        ${BASE_DIR}/lib/base/init.cpp
        ${BASE_DIR}/lib/base/logging.cpp
        ${BASE_DIR}/lib/base/thread.cpp
        ${BASE_DIR}/lib/common/protocol.h
        ${BASE_DIR}/lib/common/stat.h
        ${BASE_DIR}/lib/common/uv.cpp
        ${BASE_DIR}/lib/common/subprocess.cpp
        ${BASE_DIR}/lib/common/config.cpp
        ${BASE_DIR}/lib/common/http_status.cpp
        ${BASE_DIR}/lib/ipc/fifo.cpp
        ${BASE_DIR}/lib/ipc/shm_region.cpp
        ${BASE_DIR}/lib/server/io_worker.cpp
        ${BASE_DIR}/lib/server/server_base.cpp
        ${BASE_DIR}/lib/utils/docker.cpp
        ${BASE_DIR}/lib/utils/fs.cpp
        ${BASE_DIR}/lib/utils/procfs.cpp
        ${BASE_DIR}/lib/utils/random.cpp
        ${BASE_DIR}/lib/utils/http_parser.c
        ${BASE_DIR}/lib/utils/socket.cpp
        ${BASE_DIR}/lib/runtime/event_driven_worker.cpp
        ${BASE_DIR}/lib/runtime/worker_lib.cpp
        ${BASE_DIR}/lib/ipc/base.cpp
        ${BASE_DIR}/src/host/launcher/engine_connection.cpp
        ${BASE_DIR}/src/host/launcher/func_process.cpp
        ${BASE_DIR}/src/host/launcher/launcher.cpp
        ${BASE_DIR}/src/host/launcher/launcher_main.cpp)

add_executable(${PROJECT_NAME} ${LAUNCHER_SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} ${LAUNCHER_LIBRARIES})