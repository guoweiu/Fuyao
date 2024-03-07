//
// Created by tank on 9/5/22.
//

#ifndef LUMINE_HOST_BENCH_CONFIGURE_H
#define LUMINE_HOST_BENCH_CONFIGURE_H

// For Debug
#ifdef __DEBUG
#define INFINITY_DEBUG(MESSAGE) {LOG(INFO) << MESSAGE;}
#else
#define INFINITY_DEBUG(MESSAGE) {while(false) LOG(INFO) << MESSAGE;}
#endif
#define INFINITY_ASSERT(B, MESSAGE) {if(!(B)){LOG(ERROR) << MESSAGE;}}

// Queue length settings
#define SEND_COMPLETION_QUEUE_LENGTH 1024
#define RECV_COMPLETION_QUEUE_LENGTH 1024
#define SHARED_RECV_QUEUE_LENGTH 1024
#define MAX_NUMBER_OF_OUTSTANDING_REQUESTS 16
#define MAX_NUMBER_OF_SGE_ELEMENTS 1
#define MAX_NUMBER_OF_WORKER_COMPLETION_QUEUE_LENGTH 1024
#define MAX_POLL_CQ_TIMEOUT_MS 2


// shared memory setting
#define CACHE_LINE 64
#define META_DATA_ENTRY_SIZE 8
#define MAX_MEMORY_REGION_SIZE 1048576
#define BASIC_ALLOCATE_SIZE 1024
#define MAX_META_DATA_ENTRY_NUM ((BASIC_ALLOCATE_SIZE - 2 * CACHE_LINE) / META_DATA_ENTRY_SIZE)
//#define MAX_META_DATA_ENTRY_NUM 3

// system setting
#define PAGE_SIZE 4096

#define MAKE_IBV_SRQ_INIT_ATTR(val, context)                                 \
    ibv_srq_init_attr val{};                                                 \
    memset(&val, 0, sizeof(ibv_srq_init_attr));                              \
    val.srq_context = context;                                               \
    val.attr.max_wr = SHARED_RECV_QUEUE_LENGTH;                              \
    val.attr.max_sge = 1;

#define MAKE_IBV_QP_ATTR_FOR_INIT(val, _port_num)                       \
    ibv_qp_attr val{};                                                  \
    memset(&val, 0, sizeof(ibv_qp_attr));                               \
    val.qp_state = IBV_QPS_INIT;                                        \
    val.pkey_index = 0;                                                 \
    val.port_num = _port_num;                                           \
    val.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

#define MAKE_IBV_QP_ATTR_FOR_RTS(val)               \
    ibv_qp_attr val{};                              \
    memset(&(val), 0, sizeof(ibv_qp_attr));         \
    val.qp_state = IBV_QPS_RTS;                     \
    val.timeout = 14;                               \
    val.retry_cnt = 7;                              \
    val.rnr_retry = 7;                              \
    val.sq_psn = 0;                                 \
    val.max_rd_atomic = 1;

#define MAKE_IBV_SGE(sge, _addr, _length, _lkey)    \
    struct ibv_sge sge{};                           \
    memset(&sge, 0, sizeof(ibv_sge));               \
    sge.addr = _addr;                               \
    sge.length = _length;                           \
    sge.lkey = _lkey;

#define MAKE_WORKER_REQUEST(wr, op_type, sge, _remote_addr, _remote_rkey)   \
    struct ibv_send_wr wr{};                                                \
    memset(&wr, 0, sizeof(ibv_send_wr));                                    \
    wr.sg_list = &sge;                                                      \
    wr.next = nullptr;                                                                        \
    wr.wr_id = 0;                                                                        \
    wr.num_sge = 1;                                                         \
    wr.opcode = op_type;                                                    \
    wr.send_flags = IBV_SEND_SIGNALED ;                    \
    wr.wr.rdma.remote_addr = _remote_addr;                                  \
    wr.wr.rdma.rkey = _remote_rkey;

#endif //LUMINE_HOST_BENCH_CONFIGURE_H
