cmake_minimum_required(VERSION 3.16)
project(agent)
set(CMAKE_CXX_STANDARD 17)

set(AGENT_ABSL
        absl::flags
        absl::random_random
        absl::bind_front
        absl::failure_signal_handler
        absl::synchronization
        absl::flags_parse)

set(AGENT_LIBRARIES
        ${AGENT_ABSL}
        fmt
        GSL
        nlohmann_json
        uv_a
        nghttp2
        pthread)

SET(AGENT_SOURCE_FILES
        ${BASE_DIR}/lib/base/init.cpp
        ${BASE_DIR}/lib/base/logging.cpp
        ${BASE_DIR}/lib/base/thread.cpp
        ${BASE_DIR}/lib/common/uv.cpp
        ${BASE_DIR}/lib/common/config.cpp
        ${BASE_DIR}/lib/common/http_status.cpp
        ${BASE_DIR}/lib/server/io_worker.cpp
        ${BASE_DIR}/lib/server/poller.cpp
        ${BASE_DIR}/lib/server/server_base.cpp
        ${BASE_DIR}/lib/rdma/bit_map.cpp
        ${BASE_DIR}/lib/rdma/infinity.cpp
        ${BASE_DIR}/lib/rdma/queue_pair.cpp
        ${BASE_DIR}/lib/rdma/shared_memory.cpp
        ${BASE_DIR}/lib/utils/docker.cpp
        ${BASE_DIR}/lib/utils/http_parser.c
        ${BASE_DIR}/lib/utils/socket.cpp
        ${BASE_DIR}/lib/utils/random.cpp
        ${BASE_DIR}/lib/utils/fs.cpp
        ${BASE_DIR}/src/dpu/agent/engine_connection.cpp
        ${BASE_DIR}/src/dpu/agent/gateway_connection.cpp
        ${BASE_DIR}/src/dpu/agent/agent.cpp
        ${BASE_DIR}/src/dpu/agent/agent_main.cpp)

add_executable(${PROJECT_NAME} ${AGENT_SOURCE_FILES})
target_link_libraries(${PROJECT_NAME} ${AGENT_LIBRARIES} PkgConfig::LIBIBVERBS)