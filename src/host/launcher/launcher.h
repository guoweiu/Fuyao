#ifndef LUMINE_LAUNCHER_H
#define LUMINE_LAUNCHER_H

#include "base/common.h"
#include "base/thread.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/config.h"
#include "common/uv.h"
#include "utils/buffer_pool.h"
#include "engine_connection.h"
#include "func_process.h"

namespace faas::launcher {

    class Launcher : public uv::Base {
    public:
        static constexpr size_t kBufferSize = 4096;
        static_assert(sizeof(protocol::Message) <= kBufferSize, "kBufferSize is too small");

        enum Mode {
            kInvalidMode = 0,
            kCppMode = 1,
            kGoMode = 2,
            kNodeJsMode = 3,
            kPythonMode = 4
        };

        Launcher();

        ~Launcher();

        void set_func_id(int func_id) {
            func_id_ = func_id;
        }

        void set_fprocess(std::string_view fprocess) {
            fprocess_ = std::string(fprocess);
        }

        void set_fprocess_working_dir(std::string_view path) {
            fprocess_working_dir_ = std::string(path);
        }

        void set_fprocess_output_dir(std::string_view path) {
            fprocess_output_dir_ = std::string(path);
        }

        void set_fprocess_mode(Mode mode) {
            fprocess_mode_ = mode;
        }

        void set_engine_tcp_port(int port) {
            engine_tcp_port_ = port;
        }

        int func_id() const { return func_id_; }

        std::string_view fprocess() const { return fprocess_; }

        std::string_view fprocess_working_dir() const { return fprocess_working_dir_; }

        std::string_view fprocess_output_dir() const { return fprocess_output_dir_; }

        std::string_view rdma_device_name() const { return rdma_device_name_; }

        int rdma_device_port() const { return rdma_device_port_; }

        int rdma_device_gidx() const { return rdma_device_gid_index_; }

        int engine_tcp_port() const { return engine_tcp_port_; }

        std::string_view func_name() const {
            const config::FunctionEntry *entry = config_.FindFunctionByFuncId(func_id_);
            return entry->func_name;
        }

        std::string_view func_config_json() const { return func_config_json_; }

        bool func_worker_use_engine_socket() const { return func_worker_use_engine_socket_; }

        void Start();

        void ScheduleStop();

        void WaitForFinish();

        // Can only be called from uv_loop_
        void NewReadBuffer(size_t suggested_size, uv_buf_t *buf);

        void ReturnReadBuffer(const uv_buf_t *buf);

        void NewWriteBuffer(uv_buf_t *buf);

        void ReturnWriteBuffer(char *buf);

        uv_write_t *NewWriteRequest();

        void ReturnWriteRequest(uv_write_t *write_req);

        void OnEngineConnectionClose();

        void OnFuncProcessExit(FuncProcess *func_process);

        bool OnRecvHandshakeResponse(const protocol::Message &handshake_response,
                                     std::span<const char> payload);

        void OnRecvMessage(const protocol::Message &message);

        void set_rdma_device_name(std::string name) { rdma_device_name_ = std::move(name); }

        void set_rdma_device_port(int port) { rdma_device_port_ = port; }

        void set_rdma_device_gid_index(int gid_index) { rdma_device_gid_index_ = gid_index; }

    private:
        enum State {
            kCreated, kRunning, kStopping, kStopped
        };
        std::atomic<State> state_;

        int func_id_;
        std::string fprocess_;
        std::string fprocess_working_dir_;
        std::string fprocess_output_dir_;
        Mode fprocess_mode_;
        int engine_tcp_port_;
        std::string rdma_device_name_;
        int rdma_device_port_;
        int rdma_device_gid_index_;

        uv_loop_t uv_loop_;
        uv_async_t stop_event_;
        base::Thread event_loop_thread_;
        utils::BufferPool buffer_pool_;
        utils::SimpleObjectPool<uv_write_t> write_req_pool_;

        config::Config config_;
        std::string func_config_json_;
        bool func_worker_use_engine_socket_;
        EngineConnection engine_connection_;
        std::vector<std::unique_ptr<FuncProcess>> func_processes_;

        stat::StatisticsCollector<int32_t> engine_message_delay_stat_;

        void EventLoopThreadMain();

        DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

        DISALLOW_COPY_AND_ASSIGN(Launcher);
    };

}  // namespace faas

#endif //LUMINE_LAUNCHER_H