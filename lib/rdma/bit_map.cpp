//
// Created by tank on 9/27/22.
//

#include "bit_map.h"
#include <cstring>

#define BITMAP_MASK 1

namespace faas::rdma {

    BitMap::BitMap(uint32_t bitmap_bytes_len)
            : bitmap_bytes_len_(bitmap_bytes_len),
              bits_(nullptr) {
        bits_ = new uint8_t[bitmap_bytes_len_];
        memset(bits_, 0, bitmap_bytes_len_);
    }

    BitMap::~BitMap() {
        delete bits_;
    }

    bool BitMap::ScanTest(uint32_t bit_idx) {
        uint32_t byte_idx = bit_idx / 8;
        uint32_t bit_odd = bit_idx % 8;
        return bits_[byte_idx] & (BITMAP_MASK << bit_odd);
    }

    void BitMap::SetBitIdx(uint32_t bit_idx, int8_t value) {
        uint32_t byte_idx = bit_idx / 8;
        uint32_t bit_odd = bit_idx % 8;

        if (value) {
            bits_[byte_idx] |= (BITMAP_MASK << bit_odd);
        } else {
            bits_[byte_idx] &= ~(BITMAP_MASK << bit_odd);
        }
    }

    uint32_t BitMap::Scan(uint32_t cnt) {
        uint32_t idx_byte, idx_bit, bit_idx_start, found_count, next_bit;

        // bytes
        for (idx_byte = 0; idx_byte < bitmap_bytes_len_ && bits_[idx_byte] == 0xFF; idx_byte++) {}

        if (idx_byte == bitmap_bytes_len_) return -1;

        // bits
        for (idx_bit = 0; bits_[idx_byte] & (BITMAP_MASK << idx_bit); idx_bit++) {}

        bit_idx_start = idx_byte * 8 + idx_bit;
        for (next_bit = bit_idx_start + 1, found_count = 1;
             (next_bit < bitmap_bytes_len_ * 8) && (found_count < cnt); next_bit++) {
            if (!ScanTest(next_bit)){
                if(found_count == 0){
                    bit_idx_start = next_bit;
                }
                found_count++;
                continue;
            }
            found_count = 0;
        }

        if (found_count != cnt) {
            return -1;
        }
        return bit_idx_start;
    }
}