#include "base/logging.h"
#include "base/common.h"
#include "base/thread.h"

static pid_t __gettid() {
    return syscall(SYS_gettid);
}

#define __PREDICT_FALSE(x) __builtin_expect(x, 0)
#define __ATTRIBUTE_UNUSED __attribute__((unused))

namespace faas::logging {

    namespace {

        int vlog_level = 0;

//        const char *GetBasename(const char *file_path) {
//
//            int cnt = 0;
//            const char *last_but_one, *the_last_one;
//            the_last_one = file_path;
//
//            for (size_t i = 0; i < strlen(file_path); i++) {
//                if (file_path[i] == '/') {
//                    cnt++;
//                    last_but_one = the_last_one;
//                    the_last_one = file_path + i;
//                }
//            }
//            return last_but_one == the_last_one ? last_but_one : last_but_one + 1;
//
//        }

        void Abort() {
            raise(SIGABRT);
        }

    }

    void set_vlog_level(int level) { vlog_level = level; }

    int get_vlog_level() { return vlog_level; }

    void Init(int level) {
        set_vlog_level(level);
    }

    __ATTRIBUTE_UNUSED static constexpr const char *kLogSeverityNames[4] = {
            "INFO", "WARN", "ERROR", "FATAL"
    };

    static constexpr const char *kLogSeverityShortNames = "IWEF";

    LogMessage::LogMessage(const char *file, int line, LogSeverity severity, bool append_err_str) {
        severity_ = severity;
        preserved_errno_ = errno;
        append_err_str_ = append_err_str;
        // const char *filename = GetBasename(file);
        // Write a prefix into the log message, including local date/time, severity
        // level, filename, and line number.
        struct timespec time_stamp;

        if (__PREDICT_FALSE(clock_gettime(CLOCK_REALTIME, &time_stamp) != 0)) {
            perror("clock_gettime failed");
            abort();
        }
        constexpr int kBufferSize = 1 /* severity char */ + 14 /* datetime */ + 6 /* usec */
                                    + 1 /* space */ + 12 /* thread name */ + 8 /* pid */ + 1 /* \0 */;
        char buffer[kBufferSize];
        buffer[0] = kLogSeverityShortNames[severity_];
        struct tm datetime;
        memset(&datetime, 0, sizeof(datetime));
        if (__PREDICT_FALSE(localtime_r(&time_stamp.tv_sec, &datetime) == NULL)) {
            perror("localtime_r failed");
            abort();
        }
        strftime(buffer + 1, 14, "%m%d %H:%M:%S", &datetime);

        buffer[14] = '.';
        sprintf(buffer + 15, "%06d", static_cast<int>(time_stamp.tv_nsec / 1000));

#ifdef __WORKER_PYTHON_BUILD
        sprintf(buffer + 21, " %-12.12s", "");
#else
        sprintf(buffer + 21, " %-12.12s", base::Thread::current()->name());
#endif
        sprintf(buffer + 34, "(%6d)", static_cast<int>(__gettid()));

        stream() << fmt::format("{} {}:{}] ", buffer, file, line);
    }

    LogMessage::~LogMessage() {
        AppendErrStrIfNecessary();
        std::string message_text = stream_.str();
        SendToLog(message_text);
        if (severity_ == FATAL) {
            Abort();
        }
    }

    LogMessageFatal::LogMessageFatal(const char *file, int line, const std::string &result)
            : LogMessage(file, line, FATAL) { stream() << fmt::format("Check failed: {} ", result); }

    LogMessageFatal::~LogMessageFatal() {
        AppendErrStrIfNecessary();
        std::string message_text = stream_.str();
        SendToLog(message_text);
        Abort();
        exit(EXIT_FAILURE);
    }

    absl::Mutex stderr_mu;

    void LogMessage::SendToLog(const std::string &message_text) {

        absl::MutexLock lk(&stderr_mu);

        FILE *standrad_stream = stderr;
        if (severity_ == INFO) {
            standrad_stream = stdout;
        }
        fprintf(standrad_stream, "%s\n", message_text.c_str());
        fflush(standrad_stream);
    }

    void LogMessage::AppendErrStrIfNecessary() {
        if (append_err_str_) {
            stream() << fmt::format(": {} [{}]", strerror(preserved_errno_), preserved_errno_);
        }
    }

    CheckOpMessageBuilder::CheckOpMessageBuilder(const char *exprtext)
            : stream_(new std::ostringstream) { *stream_ << exprtext << " ("; }

    CheckOpMessageBuilder::~CheckOpMessageBuilder() { delete stream_; }

    std::ostream *CheckOpMessageBuilder::ForVar2() {
        *stream_ << " vs ";
        return stream_;
    }

    std::string *CheckOpMessageBuilder::NewString() {
        *stream_ << ")";
        return new std::string(stream_->str());
    }

    template<>
    void MakeCheckOpValueString(std::ostream *os, const char &v) {
        if (v >= 32 && v <= 126) {
            (*os) << fmt::format("'{}'", v);
        } else {
            (*os) << fmt::format("char value {}", static_cast<int16_t>(v));
        }
    }

    template<>
    void MakeCheckOpValueString(std::ostream *os, const signed char &v) {
        if (v >= 32 && v <= 126) {
            (*os) << fmt::format("'{}'", static_cast<char>(v));
        } else {
            (*os) << fmt::format("signed char value {}", static_cast<int16_t>(v));
        }
    }

    template<>
    void MakeCheckOpValueString(std::ostream *os, const unsigned char &v) {
        if (v >= 32 && v <= 126) {
            (*os) << fmt::format("'{}'", static_cast<char>(v));
        } else {
            (*os) << fmt::format("unsigned value {}", static_cast<int16_t>(v));
        }
    }

    template<>
    void MakeCheckOpValueString(std::ostream *os, const std::nullptr_t &v) {
        (*os) << "nullptr";
    }

}  // namespace faas
