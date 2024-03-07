#ifndef LUMINE_WORKER_V1_INTERFACE_H_
#define LUMINE_WORKER_V1_INTERFACE_H_

#include <stddef.h>

typedef void (*faas_append_output_fn_t)(
        void *caller_context, const char *data, size_t length);

enum PassingMethod{
    IPC,
    Fabric,
    DRC_OVER_IPC,
    DRC_OVER_Fabric,
    DRC_OVER_IPC_POLLING,
    DRC_OVER_Fabric_POLLING
};

// Return 0 on success.
typedef int (*faas_invoke_func_fn_t)(
        void *caller_context, const char *func_name,
        const char *input_data, size_t input_length,
        const char **output_data, size_t *output_length, PassingMethod method);

// Below are APIs that function library must implement.
// For all APIs, return 0 on success.

// Initialize function library, will be called once after loading
// the dynamic library.
int faas_init();

// Create a new function worker.
// When calling `invoke_func_fn` and `append_output_fn`, caller_context
// received in `faas_create_func_worker` should be passed unchanged.
int faas_create_func_worker(
        void *caller_context,
        faas_invoke_func_fn_t invoke_func_fn,
        faas_append_output_fn_t append_output_fn,
        void **worker_handle);

// Destroy a function worker.
int faas_destroy_func_worker(void *worker_handle);

// Execute the function. `append_output_fn` can be called multiple
// times to append new data to the output buffer. invoke_func_fn
// can be used to invoke other functions in the system.
// For the same worker_handle, faas_func_call will never be called
// concurrently from different threads, i.e. the implementation
// does not need to be thread-safe for a single function worker.
int faas_func_call(
        void *worker_handle,
        const char *input, size_t input_length);

#endif  // LUMINE_WORKER_V1_INTERFACE_H_
