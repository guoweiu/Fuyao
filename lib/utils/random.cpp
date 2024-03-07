#include "base/common.h"
#include "utils/random.h"

namespace faas::utils {

    namespace {
        static thread_local std::mt19937_64 rd_gen(syscall(SYS_gettid));
    }

    float GetRandomFloat(float a, float b) {
        std::uniform_real_distribution<float> distribution(a, b);
        return distribution(rd_gen);
    }

}  // namespace faas
