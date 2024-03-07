#ifndef LUMINE_UTILS_ENV_VARIABLES_H
#define LUMINE_UTILS_ENV_VARIABLES_H

#include "base/common.h"

namespace faas::utils {

    inline std::string_view GetEnvVariable(std::string_view name,
                                           std::string_view default_value = "") {
        char *value = getenv(std::string(name).c_str());
        return value != nullptr ? value : default_value;
    }

    inline int GetEnvVariableAsInt(std::string_view name, int default_value = 0) {
        char *value = getenv(std::string(name).c_str());
        if (value == nullptr) {
            return default_value;
        }
        return atoi(value);
    }

}  // namespace faas

#endif