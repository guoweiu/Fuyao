cmake_minimum_required(VERSION 3.16)
project(gateway)
set(CMAKE_CXX_STANDARD 17)

set(GATEWAY_ABSL
        absl::flags
        absl::random_random
        absl::bind_front
        absl::failure_signal_handler
        absl::flags_parse)

set(GATEWAY_LIBRARIES
        ${GATEWAY_ABSL}
        fmt
        GSL
        nlohmann_json
        uv_a
        nghttp2
        pthread)

SET(GATEWAY_SOURCE_FILES
        ${BASE_DIR}/lib/base/init.cpp
        ${BASE_DIR}/lib/base/logging.cpp
        ${BASE_DIR}/lib/base/thread.cpp
        ${BASE_DIR}/lib/common/uv.cpp
        ${BASE_DIR}/lib/common/config.cpp
        ${BASE_DIR}/lib/common/http_status.cpp
        ${BASE_DIR}/lib/server/io_worker.cpp
        ${BASE_DIR}/lib/server/server_base.cpp
        ${BASE_DIR}/lib/utils/docker.cpp
        ${BASE_DIR}/lib/utils/http_parser.c
        ${BASE_DIR}/lib/utils/socket.cpp
        ${BASE_DIR}/lib/utils/random.cpp
        ${BASE_DIR}/lib/utils/fs.cpp
        ${BASE_DIR}/src/dpu/gateway/engine_connection.cpp
        ${BASE_DIR}/src/dpu/gateway/grpc_connection.cpp
        ${BASE_DIR}/src/dpu/gateway/http_connection.cpp
        ${BASE_DIR}/src/dpu/gateway/server.cpp
        ${BASE_DIR}/src/dpu/gateway/gateway_main.cpp)

add_executable(${PROJECT_NAME} ${GATEWAY_SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} ${GATEWAY_LIBRARIES})