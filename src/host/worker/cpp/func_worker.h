#ifndef LUMINE_FUNC_WORKER_CPP_H
#define LUMINE_FUNC_WORKER_CPP_H

#include <utility>
#include <chrono>

#include "base/common.h"
#include "base/thread.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/config.h"
#include "utils/dynamic_library.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"
#include "ipc/shm_region.h"
#include "func_worker_interface.h"
#include "rdma/infinity.h"
#include "rdma/shared_memory.h"
#include "rdma/queue_pair.h"
namespace faas::worker_cpp {

    enum State {
        WAITING,
        RUNNING,
        IPC_PROCESSING,
        RDMA_REQUEST,
        RDMA_RESPONSE,
        RDMA_CREATING,
        RDMA_INVOKE,
        RDMA_SERVING,
        RDMA_PROCESSING,
        RDMA_RECLAIM,
        PROCESSING
    };

    class FuncWorker;

    class QueuePairMeta {
        friend class FuncWorker;

    public:
        QueuePairMeta(rdma::QueuePair *qp, int64_t recent_access) : qp(qp), recent_access(recent_access) {}

    private:
        rdma::QueuePair *qp;
        int64_t recent_access;
    };

    enum AllocMRType{
        FromSharedMemory = 0,
        NewCreation = 1
    };

    struct AllocMRInfo {
        AllocMRType type;
        uint64_t addr;
        uint64_t size;
    };

    class FuncWorker {
    public:
        static constexpr absl::Duration kDefaultFuncCallTimeout = absl::Milliseconds(100);

        FuncWorker();

        ~FuncWorker();

        void set_func_id(int value) { func_id_ = value; }

        void set_fprocess_id(int value) { fprocess_id_ = value; }

        void set_client_id(int value) { client_id_ = gsl::narrow_cast<uint16_t>(value); }

        void set_message_pipe_fd(int fd) { message_pipe_fd_ = fd; }

        void set_debug_file_path(std::string_view path) {
            debug_file_path_ = std::string(path);
            on_debug_ = true;
        }

        void set_faas_engine_host(std::string_view faas_engine_host) {
            faas_engine_host_ = faas_engine_host;
        }

        void enable_use_engine_socket() { use_engine_socket_ = true; }

        void set_engine_tcp_port(int port) { engine_tcp_port_ = port; }

        void set_rdma_device_name(std::string name) { rdma_device_name_ = std::move(name); }

        void set_rdma_device_port(int port) { rdma_device_port_ = port; }

        void set_rdma_device_gid_index(int gid_index) { rdma_device_gid_index_ = gid_index; }

        void Serve();

    private:
        int func_id_;
        int fprocess_id_;
        uint16_t client_id_;
        int message_pipe_fd_;
        std::string func_library_path_;
        std::string debug_file_path_;
        bool use_engine_socket_;
        int engine_tcp_port_;
        bool use_fifo_for_nested_call_;
        absl::Duration func_call_timeout_;
        std::string rdma_device_name_;
        int rdma_device_port_;
        int rdma_device_gid_index_;
        uint64_t timeout_as_ms_for_drc_;

        std::vector<AllocMRInfo> alloc_infos_;

        uint64_t tmp_id_;

        rdma::Infinity *infinity_;

        absl::flat_hash_map<std::string, std::shared_ptr<QueuePairMeta>> queue_pairs_;

        rdma::SharedMemory *shared_memory_;
        rdma::SharedMemoryInfo shared_memory_info_;

        uint32_t message_size_;

        std::atomic<State> state_;

        bool on_debug_;
        std::string faas_engine_host_;

        absl::Mutex mu_;
        absl::Mutex mu_for_rdc_;

        int engine_sock_fd_;
        int engine_sock_fd_debug_; // only will be used for debug
        int input_pipe_fd_;
        int output_pipe_fd_;

        config::Config config_;
        std::unique_ptr<utils::DynamicLibrary> func_library_;
        void *worker_handle_;

        struct InvokeFuncResource {
            protocol::FuncCall func_call;
            std::unique_ptr<ipc::ShmRegion> output_region;
            char *pipe_buffer;
        };

        std::vector<InvokeFuncResource> invoke_func_resources_ ABSL_GUARDED_BY(mu_);
        utils::BufferPool buffer_pool_for_pipes_ ABSL_GUARDED_BY(mu_);
        bool ongoing_invoke_func_ ABSL_GUARDED_BY(mu_);
        utils::AppendableBuffer func_output_buffer_;
        char main_pipe_buf_[PIPE_BUF];

        std::atomic<uint32_t> next_call_id_;
        std::atomic<uint64_t> current_func_call_id_;

        std::string GetUniqueTmpName();

        void MainServingLoop();

        void HandshakeWithEngine();

        int GetEngineSockFd();

        void ExecuteFunc(const protocol::Message &dispatch_func_call_message);

        bool InvokeFunc(const char *func_name,
                        const char *input_data, size_t input_length,
                        const char **output_data, size_t *output_length, PassingMethod method);

        bool WaitInvokeFunc(protocol::Message *invoke_func_message,
                            const char **output_data, size_t *output_length);

        bool FifoWaitInvokeFunc(protocol::Message *invoke_func_message,
                                const char **output_data, size_t *output_length);

        void ReclaimInvokeFuncResources();

        void ReclaimDRCResources(std::string_view guid_name);

        void PostRecvRequest(std::string_view guid_name);

        void PostSendRequest(std::string_view guid_name, const char **data, const size_t *data_size);

        AllocMRInfo WaitRecvRequest(std::string_view guid_name, const char **data, size_t *data_size, bool polling = false);

        // Assume caller_context is an instance of FuncWorker
        static void AppendOutputWrapper(void *caller_context, const char *data, size_t length);

        static int InvokeFuncWrapper(void *caller_context, const char *func_name,
                                     const char *input_data, size_t input_length,
                                     const char **output_data, size_t *output_length, PassingMethod method);

        DISALLOW_COPY_AND_ASSIGN(FuncWorker);

        //先轮询poll_timeout的时间，如果没有完成的CQE，再进入阻塞，等待cqe完成。fn为获取到cqe后的回调函数
        //考虑设置一个有效的返回值以进行错误处理。
//        static void poll_and_epoll_cqe(std::function<void(uint64_t)> fn, rdma::CompleteQueue queue,rdma::QueuePair * qp,int poll_timeout);
    };

}  // namespace faas

#endif //LUMINE_FUNC_WORKER_CPP_H