//
// Created by tank on 9/27/22.
//

#ifndef LUMINE_HOST_BENCH_BIT_MAP_H
#define LUMINE_HOST_BENCH_BIT_MAP_H

#include <stdint.h>

namespace faas::rdma {
    class BitMap {
    public:
        BitMap(uint32_t bitmap_bytes_len);

        ~BitMap();

        uint32_t Scan(uint32_t cnt);

        void SetBitIdx(uint32_t bit_idx, int8_t value);
    private:
        uint32_t bitmap_bytes_len_;
        uint8_t *bits_;

        bool ScanTest(uint32_t bit_idx);
    };
}

#endif //LUMINE_HOST_BENCH_BIT_MAP_H
