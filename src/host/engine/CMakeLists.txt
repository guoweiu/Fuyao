cmake_minimum_required(VERSION 3.16)
project(engine)
set(CMAKE_CXX_STANDARD 17)

set(ENGINE_ABSL
        absl::flags
        absl::random_random
        absl::bind_front
        absl::failure_signal_handler
        absl::flags_parse)

set(ENGINE_LIBRARIES
        ${ENGINE_ABSL}
        fmt
        GSL
        nlohmann_json
        uv_a
        nghttp2
        pthread)

set(ENGINE_SOURCE_FILES
        ${BASE_DIR}/lib/base/init.cpp
        ${BASE_DIR}/lib/base/logging.cpp
        ${BASE_DIR}/lib/base/thread.cpp
        ${BASE_DIR}/lib/common/protocol.h
        ${BASE_DIR}/lib/common/stat.h
        ${BASE_DIR}/lib/common/uv.cpp
        ${BASE_DIR}/lib/common/config.cpp
        ${BASE_DIR}/lib/common/http_status.cpp
        ${BASE_DIR}/lib/ipc/fifo.cpp
        ${BASE_DIR}/lib/ipc/shm_region.cpp
        ${BASE_DIR}/lib/rdma/bit_map.cpp
        ${BASE_DIR}/lib/rdma/queue_pair.cpp
        ${BASE_DIR}/lib/rdma/infinity.cpp
        ${BASE_DIR}/lib/rdma/shared_memory.cpp
        ${BASE_DIR}/lib/server/io_worker.cpp
        ${BASE_DIR}/lib/server/poller.cpp
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
        ${BASE_DIR}/src/host/engine/agent_connection.cpp
        ${BASE_DIR}/src/host/engine/dispatcher.cpp
        ${BASE_DIR}/src/host/engine/engine.cpp
        ${BASE_DIR}/src/host/engine/gateway_connection.cpp
        ${BASE_DIR}/src/host/engine/message_connection.cpp
        ${BASE_DIR}/src/host/engine/monitor.cpp
        ${BASE_DIR}/src/host/engine/tracer.cpp
        ${BASE_DIR}/src/host/engine/worker_manager.cpp
        ${BASE_DIR}/src/host/engine/engine_main.cpp)

# executable
add_executable(${PROJECT_NAME} ${ENGINE_SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} ${ENGINE_LIBRARIES} PkgConfig::LIBIBVERBS)