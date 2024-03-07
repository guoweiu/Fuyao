#ifndef LUMINE_HTTP_STATUS
#define LUMINE_HTTP_STATUS

#include "base/common.h"

namespace faas {

    enum class HttpStatus : uint16_t {
        OK = 200,
        BAD_REQUEST = 400,
        NOT_FOUND = 404,
        INTERNAL_SERVER_ERROR = 500
    };

    std::string_view GetHttpStatusString(HttpStatus status);

}  // namespace faas

#endif //LUMINE_HTTP_STATUS