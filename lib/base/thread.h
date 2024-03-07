#ifndef LUMINE_THREAD_H
#define LUMINE_THREAD_H

#include <utility>

#include "base/common.h"
#include "macro.h"
#include "logging.h"

namespace faas::base {

    // forward declaration
    class Thread;

    struct ThreadTuple{
        Thread *thread;
        void *arg;
        bool non_blocking;
    };

    class Thread {
    public:
        Thread(std::string_view name, std::function<void()> fn)
                : state_(kCreated), name_(std::string(name)), fn_(std::move(fn)), tid_(-1) {}

        Thread(std::string_view name, std::function<void(void *)> fn)
                : state_(kCreated), name_(std::string(name)), fn_with_arg_(std::move(fn)), tid_(-1) {}

        virtual ~Thread() {
            State state = state_.load();
            DCHECK(state == kCreated || state == kFinished);

            delete thread_tuple_;
        }

        void Start(void *arg = nullptr, bool non_blocking = false);

        void Join();

        void MarkThreadCategory(absl::string_view category);

        const char *name() const { return name_.c_str(); }

        int tid() { return tid_; }

        static Thread *current() {
            DCHECK(current_ != nullptr);
            return current_;
        }

        static void RegisterMainThread();

    protected:
        enum State {
            kCreated, kStarting, kRunning, kFinished
        };

        std::atomic<State> state_;
        std::string name_;
        std::function<void()> fn_;
        std::function<void(void *)> fn_with_arg_;
        int tid_;
        ThreadTuple *thread_tuple_;

        absl::Notification started_;
        pthread_t pthread_;

        static thread_local Thread *current_;

        virtual void Run(void *arg, bool non_blocking);

        static void *StartRoutine(void *arg);

        DISALLOW_COPY_AND_ASSIGN(Thread);
    };

}  // namespace faas

#endif //LUMINE_THREAD_H