//
// Created by tank on 10/5/22.
//

#ifndef LUMINE_HOST_BENCH_POLLER_H
#define LUMINE_HOST_BENCH_POLLER_H

#include "base/logging.h"
#include "base/common.h"
#include "base/thread.h"

#define POLLER_ASSERT(B, MESSAGE) {if(!(B)){std::cerr << MESSAGE;}}

namespace faas::server {

    class Poller : public base::Thread {

        friend bool JudgeTimeoutNotExpired(void *arg);

    public:
        Poller(std::function<void()> &fn, uint64_t timeout_as_ms = 20000);

        void Start();

        void Pause();

        void Stop();

    private:

        absl::Mutex mu_;

        uint64_t timeout_as_ms_; // ms
        uint64_t old_time_ms_;

        bool create_thread_flag_;
        bool stop_flag_;

        void Run(void *arg, bool non_blocking) override;

    };
}

#endif //LUMINE_HOST_BENCH_POLLER_H
