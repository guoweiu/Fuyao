//
// Created by tank on 9/5/22.
//

#ifndef LUMINE_HOST_BENCH_QUEUE_PAIR_H
#define LUMINE_HOST_BENCH_QUEUE_PAIR_H

#include <cstdint>

#include <infiniband/verbs.h>
#include "base/common.h"
#include "base/logging.h"
#include "common/time.h"

namespace faas::rdma {

    // queue pair information
    struct QueuePairInfo {
        uint16_t lid;
        uint32_t qp_num;
        uint32_t psn;
        union ibv_gid gid;
    };

    // memory region information
    struct MemoryRegionInfo {
        uint64_t addr;
        uint64_t length;
        uint32_t lkey;
        uint32_t rkey;
    };

    // worker request
    struct WorkRequest {
        uint64_t wr_id;
        struct {
            uint64_t mr_addr_start;
            uint32_t mr_lkey;
        } local;
        struct {
            uint64_t mr_addr_start;
            uint32_t mr_rkey;
        } remote;
        uint32_t size;
    };

    inline WorkRequest MakeWorkRequest(MemoryRegionInfo &local_mr_info,
                                       MemoryRegionInfo &remote_mr_info,
                                       uint64_t local_offset,
                                       uint64_t remote_offset,
                                       uint64_t wr_id,
                                       uint32_t size) {
        DCHECK(((local_offset <= local_mr_info.length) && (local_offset >= 0)));
        DCHECK(((remote_offset <= remote_mr_info.length) && (remote_offset >= 0)));
        DCHECK(((size >= 0) && (local_offset + size <= local_mr_info.length) &&
                (remote_offset + size <= remote_mr_info.length)));

        WorkRequest wr = {
                .wr_id = wr_id,
                .local = {
                        .mr_addr_start = local_mr_info.addr + local_offset,
                        .mr_lkey = local_mr_info.lkey
                },
                .remote = {
                        .mr_addr_start = remote_mr_info.addr + remote_offset,
                        .mr_rkey = remote_mr_info.rkey
                },
                .size = size
        };
        return wr;
    }

    // Work id
    enum WorkIDType : uint8_t {
        RDMA_READ_META_DATA_PULL,
        RDMA_READ_RAW_DATA_PULL,
        RDMA_UPDATE_HEAD_PULL,
        RDMA_WRITE_RAW_DATA_PUSH,
        RDMA_WRITE_META_DATA_PUSH,
        RDMA_UPDATE_TAIL_PUSH,
        RDMA_RECV,
        RDMA_SEND
    };

    struct WorkID {
        WorkIDType type: 4;
        uint32_t size: 28;
        union {
            uint32_t idx; // meta-data entry idx
            uint32_t offset;
        };
    } __attribute__((packed));

    static_assert(sizeof(WorkID) == 8, "illegal WorkID size");

    inline uint64_t MakeWorkID(WorkIDType type, uint64_t size, uint64_t offset) {
        return (offset << 32) + (size << 4) + (uint64_t(type));
    }

    inline WorkID ParseWorkID(uint64_t work_id) {
        uint8_t type = work_id & 0xf;
        uint32_t size = (work_id >> 4) & 0x3fffffff;
        uint32_t offset = work_id >> 32;
        return WorkID{WorkIDType(type), size, offset};
    }

    enum CompleteQueue : uint8_t {
        SEND_COMPLETE_QUEUE,
        RECV_COMPLETE_QUEUE
    };

    // forward declaration
    class Infinity;

    class QueuePair {

        friend class Infinity;

    public:

        QueuePair(const Infinity *infinity, bool used_shared_cq);

        ~QueuePair();

        // Get queue pair information
        QueuePairInfo GetQueuePairInfo();

        // Get Memory region information
        MemoryRegionInfo GetMemoryRegionInfo(std::string_view memory_region_name);

        // Register a memory region (need to pre allocate)
        void RegisterMemoryRegion(std::string_view memory_region_name, uint64_t buf, size_t buf_size);

        // Deregister a memory region
        void DeregisterMemoryRegion(std::string_view memory_region_name);

        // Fetch And Add
        uint64_t FetchAndAdd(WorkRequest &wr, uint64_t add);

        // Read (One-sided)
        uint64_t PostReadRequest(WorkRequest &wr);

        // Write (One-sided)
        uint64_t PostWriteRequest(WorkRequest &wr);

        // Send (Two-sided)
        uint64_t PostSendRequest(WorkRequest &wr);

        // Receive (Two-sided)
        uint64_t PostRecvRequest(WorkRequest &wr);

        // Poll the completion queue
        int PollCompletion(std::function<void(uint64_t)> fn, CompleteQueue queue = CompleteQueue::SEND_COMPLETE_QUEUE);

        void SetNotifyNonBlock(CompleteQueue queue);

        // Notify
        void NotifyCompletion(CompleteQueue queue, std::function<void(uint64_t)> fn = nullptr);

        //Notyfy with epoll
        void NotifyCompletionEpoll(CompleteQueue queue, std::function<void(uint64_t)> fn = nullptr);

        //consume a completion event
        //used when request a notify, but the cqe is polled out, but the notyfy event still exist
        void ConsumeCompletion(CompleteQueue queue);

        // Change queue pair state, Reset -> Init
        void StateFromResetToInit();

        // Change queue pair state, Init -> RTR(Ready to receive)
        void StateFromInitToRTR(uint16_t dest_lid,
                                uint32_t dest_qp_num,
                                uint32_t dest_psn,
                                union ibv_gid dest_gid);

        // Change queue pair state, RTR -> RTS(Ready to server)
        void StateFromRTRToRTS();

    private:

        // IB queue pair
        ibv_qp *ibv_queue_pair_;

        // IB send complete channel
        struct ibv_comp_channel *ibv_send_comp_channel_;

        // IB recv complete channel
        struct ibv_comp_channel *ibv_recv_comp_channel_;

        // IB send completion queue
        ibv_cq *ibv_send_completion_queue_;

        // IB recv completion queue
        ibv_cq *ibv_recv_completion_queue_;

        // Infinity (contains Send/Receive queue)
        const Infinity *infinity_;

        // Queue pair info
        QueuePairInfo queue_pair_info_;

        ibv_wc *wc_;

        // Will be used for memory management
        absl::flat_hash_map<std::string, ibv_mr *> memory_regions_;

        // Return wr_id
        uint64_t PostRequest(WorkRequest &wr, ibv_wr_opcode opcode, uint64_t add, std::function<void(void*)> error_cb = nullptr);

    };
}

#endif //LUMINE_HOST_BENCH_QUEUE_PAIR_H
