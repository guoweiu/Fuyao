//
// Created by tank on 9/25/22.
//

#ifndef LUMINE_HOST_BENCH_SHARED_MEMORY_H
#define LUMINE_HOST_BENCH_SHARED_MEMORY_H

#ifndef __SHARED_MEMORY_TEST

#include "base/logging.h"

#endif

#include<iostream>
#include <cstdint>
#include "rdma_macro.h"
#include "bit_map.h"

namespace faas::rdma {

    struct MetaDataEntry {
        uint32_t offset;
        uint32_t size;
    };

    struct SharedMemoryInfo {
        uint64_t addr;
        uint64_t length;
    };

    class SharedMemory {

    public:

        explicit SharedMemory(uint64_t buf_size = MAX_MEMORY_REGION_SIZE);

        ~SharedMemory();

        SharedMemoryInfo GetSharedMemoryInfo();

        uint32_t PopMessage(uint64_t &addr, uint32_t &size);

        uint64_t AllocateMemory(uint64_t expected_size);

        void ReturnMemory(uint64_t addr, uint32_t size);

        bool PushMetaDataEntry(uint64_t addr, uint32_t size);

        void PopMetaDataEntry(uint32_t idx);

        // direction : false -> previous, true -> idx
        void GetPreviousOrNextIdx(uint32_t &idx, bool direction);

    private:
        std::string shared_memory_name_;
        SharedMemoryInfo *shared_memory_info_;

        uint64_t base_addr_;
        uint64_t metadata_start_addr_;

        uint32_t *head_ptr_, *tail_ptr_;

        // relate control region
        uint32_t max_meta_data_entry_num_;
        uint32_t old_head_idx_;

        // related memory control
        BitMap *bit_map_;

        void PopMetaDataEntry(uint32_t idx, bool update_head);

    };
}

#endif //LUMINE_HOST_BENCH_SHARED_MEMORY_H
