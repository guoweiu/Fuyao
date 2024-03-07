//
// Created by tank on 9/26/22.
//

#include "shared_memory.h"

namespace faas::rdma {

    SharedMemory::SharedMemory(uint64_t buf_size)
            : max_meta_data_entry_num_(MAX_META_DATA_ENTRY_NUM),
              old_head_idx_(0) {

        if (buf_size < MAX_MEMORY_REGION_SIZE || (buf_size % MAX_MEMORY_REGION_SIZE != 0)) {
#ifndef  __SHARED_MEMORY_TEST
            LOG(ERROR) << "buf_size is an illegal value";
#else
            std::cerr << "buf_size is an illegal value" << std::endl;
#endif
        } else {

            void *buf;
            int res = posix_memalign(&buf, PAGE_SIZE, buf_size);
            if (res != 0 && buf == nullptr) {
#ifndef  __SHARED_MEMORY_TEST
                LOG(ERROR) << "Could not allocate and align buffer";
#else
                std::cerr << "Could not allocate and align buffer" << std::endl;
#endif
            }

            memset(buf, 0, BASIC_ALLOCATE_SIZE);

            base_addr_ = reinterpret_cast<uint64_t>(buf);

            // shared memory info
            shared_memory_info_ = new SharedMemoryInfo;
            shared_memory_info_->addr = base_addr_;
            shared_memory_info_->length = buf_size;

            // Initial bit map
            uint32_t bit_map_byte_len = (MAX_MEMORY_REGION_SIZE / BASIC_ALLOCATE_SIZE) / 8;
            bit_map_ = new BitMap(bit_map_byte_len);

            // Initial pointer ptr
            head_ptr_ = reinterpret_cast<uint32_t *>(base_addr_);
            tail_ptr_ = reinterpret_cast<uint32_t *>(base_addr_ + CACHE_LINE);

            metadata_start_addr_ = base_addr_ + 2 * CACHE_LINE;

            bit_map_->SetBitIdx(0, 1);
        }

    }

    SharedMemory::~SharedMemory() {
        delete bit_map_;
        delete shared_memory_info_;

        void *buf = reinterpret_cast<void *>(base_addr_);
        free(buf);

    }

    SharedMemoryInfo SharedMemory::GetSharedMemoryInfo() {
        return *shared_memory_info_;
    }

    void SharedMemory::ReturnMemory(uint64_t addr, uint32_t size) {

        uint32_t cnt = (size % BASIC_ALLOCATE_SIZE)
                       ? (size / BASIC_ALLOCATE_SIZE + 1)
                       : (size / BASIC_ALLOCATE_SIZE);

        uint32_t bit_idx_start = (addr - base_addr_) / BASIC_ALLOCATE_SIZE;

        // identify unallocated
        for (uint32_t i = 0; i < cnt; i++) {
            bit_map_->SetBitIdx(bit_idx_start + i, 0);
        }

    }

    // will be exec in Agent, Engine
    uint32_t SharedMemory::PopMessage(uint64_t &addr, uint32_t &size) {
        bool is_empty = false;

        uint32_t current_head_idx = *head_ptr_;
        uint32_t current_tail_idx = *tail_ptr_;

        if (current_head_idx == current_tail_idx) {
#ifndef __SHARED_MEMORY_TEST
            //   LOG(INFO) << "No message in shared memory!";
#else
            std::cout << "No message in shared memory!" << std::endl;
#endif
            is_empty = true;
        }

        if (is_empty) {
            return -1;
        } else {
            auto entry = reinterpret_cast<MetaDataEntry *>(metadata_start_addr_ +
                                                           current_head_idx * META_DATA_ENTRY_SIZE);

            addr = entry->offset + base_addr_;
            size = entry->size;

            return current_head_idx;
        }
    }

    void SharedMemory::PopMetaDataEntry(uint32_t idx) {
        PopMetaDataEntry(idx, true);
    }

    void SharedMemory::PopMetaDataEntry(uint32_t idx, bool update_head) {
        auto entry = reinterpret_cast<MetaDataEntry *>(metadata_start_addr_ + idx * META_DATA_ENTRY_SIZE);

        uint64_t addr = base_addr_ + entry->offset;
        uint32_t size = entry->size;

        if (update_head) {
            *head_ptr_ = (*head_ptr_ + 1) % max_meta_data_entry_num_;
        }
        old_head_idx_ = (old_head_idx_ + 1) % max_meta_data_entry_num_;

        ReturnMemory(addr, size);
    }

    // will be exec in Engine
    uint64_t SharedMemory::AllocateMemory(uint64_t expected_size) {
        uint32_t cnt = (expected_size % BASIC_ALLOCATE_SIZE)
                       ? (expected_size / BASIC_ALLOCATE_SIZE + 1)
                       : (expected_size / BASIC_ALLOCATE_SIZE);

        uint32_t bit_idx_start = bit_map_->Scan(cnt);
        if (bit_idx_start == 0xffffffff) {
            return 0;
        } else {
            // identify allocated
            for (uint32_t i = 0; i < cnt; i++) {
                bit_map_->SetBitIdx(bit_idx_start + i, 1);
            }

            return base_addr_ + bit_idx_start * BASIC_ALLOCATE_SIZE;
        }
    }

    // will be exec in Engine
    bool SharedMemory::PushMetaDataEntry(uint64_t addr, uint32_t size) {
        uint32_t offset = addr - base_addr_;
        bool is_full = false;

        // clear useless metadata entry
        uint32_t current_head_idx = *head_ptr_;
        while (old_head_idx_ != current_head_idx) {
            PopMetaDataEntry(old_head_idx_, false);
#ifndef __SHARED_MEMORY_TEST
            DLOG(INFO) << "clear useless metadata entry, head_idx = " << old_head_idx_;
#else
            std::cout << "clear useless metadata entry, head_idx = " << old_head_idx_ << std::endl;
#endif
        }

        // insert new metadata entry
        uint32_t current_tail_idx_ = *tail_ptr_;
        if (((current_tail_idx_ + 1) % max_meta_data_entry_num_) == current_head_idx) {
#ifndef __SHARED_MEMORY_TEST
            LOG(ERROR) << "No space to store meta data entry!";
#else
            std::cout << "No space to store meta data entry!" << std::endl;
#endif
            is_full = true;
        } else {
            auto entry = reinterpret_cast<MetaDataEntry *>(metadata_start_addr_ +
                                                           current_tail_idx_ * META_DATA_ENTRY_SIZE);

            entry->offset = offset;
            entry->size = size;

            *tail_ptr_ = (current_tail_idx_ + 1) % max_meta_data_entry_num_;
        }

        if (is_full) {
            return false;
        }
        return true;
    }

    void SharedMemory::GetPreviousOrNextIdx(uint32_t &idx, bool direction) {

        if (direction) {
            // next
            idx = (idx + 1) % max_meta_data_entry_num_;
        } else {
            // previous
            idx = (idx == 0) ? max_meta_data_entry_num_ - 1 : idx - 1;
        }
    }
}