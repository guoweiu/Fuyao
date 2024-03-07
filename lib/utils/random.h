#ifndef LUMINE_RANDOM_H
#define LUMINE_RANDOM_H

#include "base/common.h"

namespace faas::utils {

// Thread-safe
    float GetRandomFloat(float a = 0.0f, float b = 1.0f);  // In [a,b]

}  // namespace faas

#endif //LUMINE_RANDOM_H