#ifndef LUMINE_INIT_H
#define LUMINE_INIT_H

#include "common.h"

namespace faas::base {

    void InitMain(int argc, char *argv[], std::vector<char *> *positional_args = nullptr);

}  // namespace faas

#endif //LUMINE_INIT_H