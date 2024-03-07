cmake_minimum_required(VERSION 3.16)
project(worker_python)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-D__WORKER_PYTHON_BUILD)
add_compile_options(-frtti)

include_directories(${BASE_DIR}/deps/pybind11/include)
include_directories(${PYTHON_INCLUDE_DIR})

set(FUNC_WORKER_PYTHON_ABSL
        absl::flags
        absl::random_random
        absl::bind_front
        absl::failure_signal_handler
        absl::flags_parse)

set(FUNC_WORKER_PYTHON_PYBIND11
        pybind11::pybind11)

set(FUNC_WORKER_PYTHON_LIBRARIES
        ${FUNC_WORKER_PYTHON_ABSL}
        ${FUNC_WORKER_PYTHON_PYBIND11}
        uv_a
        fmt
        GSL
        nghttp2
        pthread
        nlohmann_json)

set(FUNC_WORKER_PYTHON_SOURCE_FILES
        ${BASE_DIR}/lib/base/logging.cpp
        ${BASE_DIR}/lib/base/thread.cpp
        ${BASE_DIR}/lib/common/config.cpp
        ${BASE_DIR}/lib/ipc/base.cpp
        ${BASE_DIR}/lib/ipc/fifo.cpp
        ${BASE_DIR}/lib/ipc/shm_region.cpp
        ${BASE_DIR}/lib/utils/docker.cpp
        ${BASE_DIR}/lib/utils/fs.cpp
        ${BASE_DIR}/lib/utils/socket.cpp
        ${BASE_DIR}/lib/runtime/event_driven_worker.cpp
        ${BASE_DIR}/lib/runtime/worker_lib.cpp
        ${BASE_DIR}/src/host/worker/python/module.cpp)

add_library(${PROJECT_NAME} SHARED ${FUNC_WORKER_PYTHON_SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} ${FUNC_WORKER_PYTHON_LIBRARIES})

set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME ${FUNC_WORKER_PYTHON_OUTPUT_NAME})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "")