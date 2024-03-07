//#pragma once
#ifndef LUMINE_ASM_H
#define LUMINE_ASM_H

namespace faas {

    inline void asm_volatile_memory() {
        asm volatile("" : : : "memory");
    }

    inline void asm_volatile_pause() {
        asm volatile("pause");
    }

}  // namespace faas

#endif //LUMINE_ASM_H