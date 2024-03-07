#include "base/common.h"
#include "base/thread.h"
#include "utils/env_variables.h"

namespace faas::base {

    thread_local Thread *Thread::current_ = nullptr;

    namespace {
        pid_t gettid() {
            return syscall(SYS_gettid);
        }
    }

    void Thread::Start(void *arg, bool non_blocking) {
        state_.store(kStarting);
        thread_tuple_ = new ThreadTuple{this, arg, non_blocking};
        CHECK_EQ(pthread_create(&pthread_, nullptr, &Thread::StartRoutine, thread_tuple_), 0);
        started_.WaitForNotification();
        DCHECK(state_.load() == kRunning || state_.load() == kFinished);
    }

    void Thread::Join() {
        State state = state_.load();
        if (state == kFinished) {
            return;
        }
        DCHECK(state == kRunning);
        CHECK_EQ(pthread_join(pthread_, nullptr), 0);
    }

    void Thread::Run(void *arg, bool non_blocking) {
        tid_ = gettid();
        non_blocking ? state_.store(kFinished) : state_.store(kRunning);
        started_.Notify();
        DLOG(INFO) << "Start thread: " << name_;
        arg ? fn_with_arg_(arg) : fn_();
        state_.store(kFinished);
    }

    void Thread::MarkThreadCategory(absl::string_view category) {
        CHECK(current_ == this);
        // Set cpuset
        std::string cpuset_var_name(fmt::format("FAAS_{}_THREAD_CPUSET", category));
        std::string cpuset_str(utils::GetEnvVariable(cpuset_var_name));
        if (!cpuset_str.empty()) {
            cpu_set_t set;
            CPU_ZERO(&set);
            for (const std::string_view &cpu_str: absl::StrSplit(cpuset_str, ",")) {
                int cpu;
                CHECK(absl::SimpleAtoi(cpu_str, &cpu));
                CPU_SET(cpu, &set);
            }
            if (sched_setaffinity(0, sizeof(set), &set) != 0) {
                PLOG(FATAL) << "Failed to set CPU affinity to " << cpuset_str;
            } else {
                LOG(INFO) << "Successfully set CPU affinity of current thread to " << cpuset_str;
            }
        } else {
            LOG(INFO) << "Does not find cpuset setting for " << category
                      << " threads (can be set by " << cpuset_var_name << ")";
        }
        // Set nice
        std::string nice_var_name(fmt::format("FAAS_{}_THREAD_NICE", category));
        std::string nice_str(utils::GetEnvVariable(nice_var_name));
        if (!nice_str.empty()) {
            int nice_value;
            CHECK(absl::SimpleAtoi(nice_str, &nice_value));
            int current_nice = nice(0);
            errno = 0;
            if (nice(nice_value - current_nice) == -1 && errno != 0) {
                PLOG(FATAL) << "Failed to set nice to " << nice_value;
            } else {
                CHECK_EQ(nice(0), nice_value);
                LOG(INFO) << "Successfully set nice of current thread to " << nice_value;
            }
        } else {
            LOG(INFO) << "Does not find nice setting for " << category
                      << " threads (can be set by " << nice_var_name << ")";
        }
    }

    void *Thread::StartRoutine(void *arg) {
        auto tuple = reinterpret_cast<ThreadTuple *>(arg);
        current_ = tuple->thread;
        current_->Run(tuple->arg, tuple->non_blocking);
        return nullptr;
    }

    void Thread::RegisterMainThread() {
        auto thread = new Thread("Main", std::function<void()>());
        thread->state_.store(kRunning);
        thread->tid_ = gettid();
        thread->pthread_ = pthread_self();
        current_ = thread;
        LOG(INFO) << "Register main thread: tid=" << thread->tid_;
    }

}  // namespace faas
