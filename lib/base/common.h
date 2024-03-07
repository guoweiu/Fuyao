#ifndef LUMINE_COMMON_H
#define LUMINE_COMMON_H

// C includes
#include <csignal>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <unistd.h>

// C++ includes
#include <algorithm>
#include <atomic>
#include <limits>
#include <functional>
#include <memory>
#include <utility>
#include <sstream>
#include <string>
#include <string_view>
#include <random>
#include <typeinfo>
#include <typeindex>

// STL containers
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <queue>
#include <vector>

// Linux /usr/include/
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <ftw.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <unistd.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/un.h>
#include <sys/poll.h>
#include <sys/timerfd.h>

// Fmtlib
#include <fmt/core.h>
#include <fmt/format.h>

// Guidelines Support Library (GSL)
#include <gsl/gsl>
#include <gsl/span>

// Nghttp2
#include <nghttp2/nghttp2.h>

// Absl
#include <absl/time/clock.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/match.h>
#include <absl/strings/strip.h>
#include <absl/strings/str_split.h>
#include <absl/strings/numbers.h>
#include <absl/strings/ascii.h>
#include <absl/random/random.h>
#include <absl/random/distributions.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>
#include <absl/functional/bind_front.h>
#include <absl/algorithm/container.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/debugging/symbolize.h>
#include <absl/debugging/failure_signal_handler.h>

// Libuv
#include <uv.h>

namespace std {
    using gsl::span;
}  // namespace std

#endif //LUMINE_COMMON_H
