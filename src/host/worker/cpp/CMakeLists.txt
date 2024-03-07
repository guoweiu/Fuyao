cmake_minimum_required(VERSION 3.16)
project(${FUNC_WORKER_CPP_LIBRARIES_NAME})
set(CMAKE_CXX_STANDARD 17)

#include_directories(${BASE_DIR}/src/worker/cpp)

set(FUNC_WORKER_CPP_ABSL
        absl::flags
        absl::random_random
        absl::bind_front
        absl::failure_signal_handler
        absl::flags_parse)

set(FUNC_WORKER_CPP_LIBRARIES
        ${FUNC_WORKER_CPP_ABSL}
        fmt
        GSL
        nlohmann_json
        uv_a
        nghttp2
        pthread)

set(FUNC_WORKER_CPP_SOURCE_FILES
        ${BASE_DIR}/lib/base/init.cpp
        ${BASE_DIR}/lib/base/logging.cpp
        ${BASE_DIR}/lib/base/thread.cpp
        ${BASE_DIR}/lib/common/uv.cpp
        ${BASE_DIR}/lib/common/subprocess.cpp
        ${BASE_DIR}/lib/common/config.cpp
        ${BASE_DIR}/lib/common/http_status.cpp
        ${BASE_DIR}/lib/ipc/fifo.cpp
        ${BASE_DIR}/lib/ipc/shm_region.cpp
        ${BASE_DIR}/lib/rdma/bit_map.cpp
        ${BASE_DIR}/lib/rdma/infinity.cpp
        ${BASE_DIR}/lib/rdma/queue_pair.cpp
        ${BASE_DIR}/lib/rdma/shared_memory.cpp
        ${BASE_DIR}/lib/server/io_worker.cpp
        ${BASE_DIR}/lib/server/server_base.cpp
        ${BASE_DIR}/lib/utils/docker.cpp
        ${BASE_DIR}/lib/utils/fs.cpp
        ${BASE_DIR}/lib/utils/procfs.cpp
        ${BASE_DIR}/lib/utils/random.cpp
        ${BASE_DIR}/lib/utils/http_parser.c
        ${BASE_DIR}/lib/utils/socket.cpp
        ${BASE_DIR}/lib/runtime/worker_lib.cpp
        ${BASE_DIR}/lib/ipc/base.cpp
        ${BASE_DIR}/src/host/worker/cpp/func_worker.cpp
        ${BASE_DIR}/src/host/worker/cpp/func_worker_main.cpp)

# add library
add_library(${FUNC_WORKER_CPP_LIBRARIES_NAME} SHARED ${FUNC_WORKER_CPP_SOURCE_FILES})
target_link_libraries(${FUNC_WORKER_CPP_LIBRARIES_NAME} ${FUNC_WORKER_CPP_LIBRARIES} PkgConfig::LIBIBVERBS)