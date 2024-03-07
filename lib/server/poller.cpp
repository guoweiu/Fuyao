//
// Created by tank on 10/5/22.
//

#include "poller.h"

namespace faas::server {

    int64_t GetCurTimeMillis() {
        struct timeval tv{};
        gettimeofday(&tv, nullptr);
        return tv.tv_sec * 1000 + tv.tv_usec / 1000;
    }

    Poller::Poller(std::function<void()> &fn, uint64_t timeout_as_ms) :
            base::Thread("Poller", fn),
            timeout_as_ms_(timeout_as_ms),
            old_time_ms_(0),
            create_thread_flag_(false),
            stop_flag_(false) {}

    void Poller::Start() {
        if (!create_thread_flag_) {
            create_thread_flag_ = true;
            old_time_ms_ = GetCurTimeMillis();
            this->base::Thread::Start();
        }
        mu_.Lock();
        old_time_ms_ = GetCurTimeMillis();
        mu_.Unlock();

    }

    void Poller::Pause() {
        mu_.Lock();
        old_time_ms_ -= timeout_as_ms_;
        mu_.Unlock();
    }

    bool JudgeTimeoutNotExpired(void *arg) {
        uint64_t current_time = GetCurTimeMillis();
        auto self = reinterpret_cast<Poller *>(arg);

        return (self->old_time_ms_ + self->timeout_as_ms_ >= current_time);
    }

    void Poller::Run(void *arg, bool non_blocking) {
        tid_ = gettid();
        state_.store(kRunning);
        started_.Notify();
        LOG(INFO) << "Start thread: " << name_;
        while (true) {
            mu_.Lock();
            if (stop_flag_) {
                mu_.Unlock();
                break;
            }
            mu_.Await(absl::Condition(JudgeTimeoutNotExpired, this));
            this->fn_();
            mu_.Unlock();
        }
        state_.store(kFinished);
    }

    void Poller::Stop() {
        mu_.Lock();
        stop_flag_ = true;
        mu_.Unlock();

        Start();
    }

}