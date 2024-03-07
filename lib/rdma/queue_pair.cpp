//
// Created by tank on 9/5/22.
//

#include "rdma/queue_pair.h"
#include "rdma/infinity.h"
#include <sys/epoll.h>
namespace faas::rdma {

    QueuePair::QueuePair(const Infinity *infinity, bool used_shared_cq) : infinity_(infinity), wc_(nullptr) {
        // create send complete channel
        ibv_send_comp_channel_ = ibv_create_comp_channel(this->infinity_->ibv_context_);
        INFINITY_ASSERT(ibv_send_comp_channel_ != nullptr, "infinity: Could not create send completion channel")

        // create recv complete channel
        ibv_recv_comp_channel_ = ibv_create_comp_channel(this->infinity_->ibv_context_);
        INFINITY_ASSERT(ibv_recv_comp_channel_ != nullptr, "infinity: Could not create recv completion channel :"+std::string(strerror(errno)))

        // used share completion queue
        if (used_shared_cq) {
            ibv_send_completion_queue_ = this->infinity_->ibv_send_completion_queue_;
            ibv_recv_completion_queue_ = this->infinity_->ibv_receive_completion_queue_;
        } else {
            // Allocate send completion queue
            ibv_send_completion_queue_ = ibv_create_cq(this->infinity_->ibv_context_, SEND_COMPLETION_QUEUE_LENGTH,
                                                       nullptr, ibv_send_comp_channel_, 0);
            // Allocate recv completion queue
            ibv_recv_completion_queue_ = ibv_create_cq(this->infinity_->ibv_context_, RECV_COMPLETION_QUEUE_LENGTH,
                                                       nullptr, ibv_recv_comp_channel_, 0);
        }
        INFINITY_ASSERT(ibv_send_completion_queue_ != nullptr, "infinity: Could not allocate send completion queue")
        INFINITY_ASSERT(ibv_recv_completion_queue_ != nullptr, "infinity: Could not allocate recv completion queue"+std::string(strerror(errno)))

        struct ibv_qp_init_attr qp_init_attr = {
                .send_cq = this->ibv_send_completion_queue_,
                .recv_cq = this->ibv_recv_completion_queue_,
                .srq = this->infinity_->ibv_shared_receive_queue_,
                .cap = {
                        .max_send_wr = SEND_COMPLETION_QUEUE_LENGTH,
                        .max_recv_wr = RECV_COMPLETION_QUEUE_LENGTH,
                        .max_send_sge = MAX_NUMBER_OF_SGE_ELEMENTS,
                        .max_recv_sge = MAX_NUMBER_OF_SGE_ELEMENTS
                },
                .qp_type = IBV_QPT_RC,
                .sq_sig_all = 1,
        };

        this->ibv_queue_pair_ = ibv_create_qp(this->infinity_->ibv_protection_domain_, &qp_init_attr);
        INFINITY_ASSERT(this->ibv_queue_pair_ != nullptr, "infinity:  Cannot create queue pair")
        if(this->ibv_queue_pair_== nullptr){
            std::printf("1");
        }
        uint32_t random_psn = lrand48() & 0xffffff;
        queue_pair_info_ = {
                .lid = this->infinity_->ibv_device_local_id_,
                .qp_num = this->ibv_queue_pair_->qp_num,
                .psn  = random_psn,
                .gid = this->infinity_->gid_
        };

        wc_ = new ibv_wc[MAX_NUMBER_OF_WORKER_COMPLETION_QUEUE_LENGTH];
    }

    QueuePair::~QueuePair() {
        int ret;
        // Deregister memory region and free buf
        for (auto &memory_region: memory_regions_) {
            ret = ibv_dereg_mr(memory_region.second);
            INFINITY_ASSERT(ret == 0, "infinity: Could not deregister memory")
        }

        if (wc_ != nullptr) {
            free(wc_);
        }

        // Destroy queue pair
        ret = ibv_destroy_qp(this->ibv_queue_pair_);
        INFINITY_ASSERT(ret == 0, "infinity: Cannot delete queue pair")

        // Destroy receive completion queue
        ret = ibv_destroy_cq(ibv_send_completion_queue_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy send completion queue")

        // Destroy receive completion queue
        ret = ibv_destroy_cq(ibv_recv_completion_queue_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy recv completion queue")

        // Destroy send completion channel
        ret = ibv_destroy_comp_channel(ibv_send_comp_channel_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy send completion channel")

        // Destroy recv completion channel
        ret = ibv_destroy_comp_channel(ibv_recv_comp_channel_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy recv completion channel")
    }

    QueuePairInfo QueuePair::GetQueuePairInfo() {
        return queue_pair_info_;
    }

    MemoryRegionInfo QueuePair::GetMemoryRegionInfo(std::string_view memory_region_name) {
        INFINITY_ASSERT(memory_regions_.contains(memory_region_name), "memory_region_name does not exist")

        auto memory_region = memory_regions_[memory_region_name];
        MemoryRegionInfo mr_info = {reinterpret_cast<uint64_t>(memory_region->addr),
                                    memory_region->length,
                                    memory_region->lkey,
                                    memory_region->rkey};
        return mr_info;
    }

    void QueuePair::RegisterMemoryRegion(std::string_view memory_region_name, uint64_t buf, size_t buf_size) {
        // Register memory on protection domain
        auto ibv_memory_region = ibv_reg_mr(this->infinity_->ibv_protection_domain_, reinterpret_cast<void *>(buf),
                                            buf_size,
                                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        INFINITY_ASSERT(ibv_memory_region != nullptr, "infinity: Could not register memory")

        INFINITY_ASSERT(memory_regions_.contains(memory_region_name) == false,
                        "infinity: memory region name already exists")
        memory_regions_[memory_region_name] = ibv_memory_region;

    }

    void QueuePair::DeregisterMemoryRegion(std::string_view memory_region_name) {
        DCHECK(memory_regions_.contains(memory_region_name));
        int ret = ibv_dereg_mr(memory_regions_[memory_region_name]);
        INFINITY_ASSERT(ret == 0, "infinity: Could not deregister memory")
        memory_regions_.erase(memory_region_name);
    }

    //post request. return -1 when error occuer, return wr.wr_id when succes
    uint64_t QueuePair::PostRequest(WorkRequest &wr, ibv_wr_opcode opcode, uint64_t add,std::function<void(void*)> error_cb) {
        struct ibv_send_wr *bad_wr = nullptr;

        struct ibv_sge sge = {
                .addr = wr.local.mr_addr_start,
                .length = wr.size,
                .lkey = wr.local.mr_lkey
        };

        struct ibv_send_wr send_wr = {
                .wr_id = wr.wr_id,
                .next = nullptr,
                .sg_list = &sge,
                .num_sge = 1,
                .opcode = opcode,
                .send_flags = IBV_SEND_SIGNALED
        };

        if (opcode == IBV_WR_ATOMIC_FETCH_AND_ADD) {
            send_wr.wr.atomic.remote_addr = wr.remote.mr_addr_start;
            send_wr.wr.atomic.rkey = wr.remote.mr_rkey;
            send_wr.wr.atomic.compare_add = add;
        } else if (opcode == IBV_WR_SEND) {
            // no operation
        } else {
            send_wr.wr.rdma.remote_addr = wr.remote.mr_addr_start;
            send_wr.wr.rdma.rkey = wr.remote.mr_rkey;
        }

        int ret = ibv_post_send(this->ibv_queue_pair_, &send_wr, &bad_wr);
        INFINITY_ASSERT(ret == 0, "infinity: posting read request failed"+std::string(strerror(errno)))
        if(ret != 0){
            return -1;
        }

        return wr.wr_id;
    }

    uint64_t QueuePair::FetchAndAdd(WorkRequest &wr, uint64_t add) {
        return PostRequest(wr, IBV_WR_ATOMIC_FETCH_AND_ADD, add);
    }

    uint64_t QueuePair::PostReadRequest(WorkRequest &wr) {
        return PostRequest(wr, IBV_WR_RDMA_READ, 0);
    }

    uint64_t QueuePair::PostWriteRequest(WorkRequest &wr) {
        return PostRequest(wr, IBV_WR_RDMA_WRITE, 0);
    }

    uint64_t QueuePair::PostSendRequest(WorkRequest &wr) {
        return PostRequest(wr, IBV_WR_SEND, 0);
    }

    uint64_t QueuePair::PostRecvRequest(WorkRequest &wr) {
        struct ibv_recv_wr *bad_wr = nullptr;

        struct ibv_sge sge = {
                .addr = wr.local.mr_addr_start,
                .length = wr.size,
                .lkey = wr.local.mr_lkey
        };

        struct ibv_recv_wr recv_wr = {
                .wr_id = wr.wr_id,
                .next = nullptr,
                .sg_list = &sge,
                .num_sge = 1
        };
        //comment: 这里默认使用srq了？
        int ret = ibv_post_srq_recv(this->ibv_queue_pair_->srq, &recv_wr, &bad_wr);
        INFINITY_ASSERT(ret == 0, "infinity: posting recv request failed")

        return wr.wr_id;
    }

    int QueuePair::PollCompletion(std::function<void(uint64_t)> fn, CompleteQueue queue) {
        auto cq = (queue == CompleteQueue::RECV_COMPLETE_QUEUE)
                  ? this->ibv_recv_completion_queue_
                  : this->ibv_send_completion_queue_;
        int poll_result = ibv_poll_cq(cq, MAX_NUMBER_OF_WORKER_COMPLETION_QUEUE_LENGTH, wc_);
        //DLOG(INFO) << "infinity: poll_result = " << poll_result;
        char info[64];
        for (int i = 0; i < poll_result; i++) {
            auto wc = wc_ + i;
            if (wc->status == IBV_WC_SUCCESS) {
                if (fn == nullptr) {
                    sprintf(info, "wr_id = %lu, wc_status = %x", wc->wr_id, wc->status);
                    INFINITY_DEBUG(info)
                } else {
                    fn(wc->wr_id);
                }

            } else {
                sprintf(info, "wr_id = %lu, wc_status = %x, wc_vendor_err = %x", wc->wr_id, wc->status, wc->vendor_err);
                INFINITY_ASSERT(false, info+std::string(strerror(errno)))
            }
        }

        return poll_result;
    }

    void QueuePair::SetNotifyNonBlock(CompleteQueue queue) {
        int ret;
        auto cq = (queue == CompleteQueue::RECV_COMPLETE_QUEUE)
                  ? this->ibv_recv_completion_queue_
                  : this->ibv_send_completion_queue_;
        ret = ibv_req_notify_cq(cq, 0);
        INFINITY_ASSERT(ret == 0, "infinity: Failed to set notify")
    }

    void QueuePair::NotifyCompletion(CompleteQueue queue, std::function<void(uint64_t)> fn) {
        int ret, cnt = 0;

        struct ibv_cq *ev_cq;
        struct ibv_wc wc{};
        void *ev_ctx;

        char info[64];

        // Block until a comp_event arrives
        auto channel = (queue == CompleteQueue::RECV_COMPLETE_QUEUE)
                       ? this->ibv_recv_comp_channel_
                       : this->ibv_send_comp_channel_;
        ret = ibv_get_cq_event(channel, &ev_cq, &ev_ctx);
        INFINITY_ASSERT(ret == 0, "infinity: Failed to get cq_event")

        for (; (ret=ibv_poll_cq(ev_cq, 1, &wc)) > 0; cnt++) {
            if (wc.status == IBV_WC_SUCCESS) {
                if (fn != nullptr) {
                    sprintf(info, "wr_id = %lu, wc_status = %x", wc.wr_id, wc.status);

                    DLOG(INFO) <<info;
                    fn(wc.wr_id);
                } else {
                    sprintf(info, "wr_id = %lu, wc_status = %x", wc.wr_id, wc.status);

                    INFINITY_DEBUG(info)
                }
            } else {
                sprintf(info, "wr_id = %lu, wc_status = %x, wc_vendor_err = %x", wc.wr_id, wc.status, wc.vendor_err);

                INFINITY_ASSERT(false, info)
            }
        }

        DLOG(INFO) <<"ibv_poll_cq finished, cnt is "<<cnt<<" ret is "<<ret;

        ibv_ack_cq_events(ev_cq, cnt);

    }

    void QueuePair::StateFromResetToInit() {
        struct ibv_qp_attr qp_attr = {
                .qp_state = IBV_QPS_INIT,
                .qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                   IBV_ACCESS_REMOTE_READ |
                                   IBV_ACCESS_LOCAL_WRITE |
                                   IBV_ACCESS_REMOTE_ATOMIC,
                .pkey_index = 0,
                .port_num = this->infinity_->ibv_device_port_
        };

        int ret = ibv_modify_qp(this->ibv_queue_pair_, &qp_attr,
                                IBV_QP_STATE |
                                IBV_QP_PKEY_INDEX |
                                IBV_QP_PORT |
                                IBV_QP_ACCESS_FLAGS
        );
        INFINITY_ASSERT(ret == 0, "infinity: Cannot transfer to INIT state")
    }

    void
    QueuePair::StateFromInitToRTR(uint16_t dest_lid, uint32_t dest_qp_num, uint32_t dest_psn, union ibv_gid dest_gid) {
        struct ibv_qp_attr qp_attr = {
                .qp_state = IBV_QPS_RTR,
                .path_mtu = IBV_MTU_512,
                .rq_psn = dest_psn,
                .dest_qp_num = dest_qp_num,
                .ah_attr = {
                        .grh = {
                                .dgid = dest_gid,
                                .flow_label = 0,
                                .sgid_index = this->infinity_->ibv_device_gid_index_,
                                .hop_limit = 1,
                                .traffic_class = 0
                        },
                        .dlid = dest_lid,
                        .sl = 0,
                        .src_path_bits = 0,
                        .is_global = 1,
                        .port_num = this->infinity_->ibv_device_port_
                },
                .max_dest_rd_atomic = 1,
                .min_rnr_timer = 0x12,
        };

        int ret = ibv_modify_qp(this->ibv_queue_pair_, &qp_attr,
                                IBV_QP_STATE |
                                IBV_QP_AV |
                                IBV_QP_PATH_MTU |
                                IBV_QP_DEST_QPN |
                                IBV_QP_RQ_PSN |
                                IBV_QP_MAX_DEST_RD_ATOMIC |
                                IBV_QP_MIN_RNR_TIMER
        );
        INFINITY_ASSERT(ret == 0, "infinity: Cannot transfer to RTR state")
    }

    void QueuePair::StateFromRTRToRTS() {
        struct ibv_qp_attr qp_attr = {
                .qp_state = IBV_QPS_RTS,
                .sq_psn = this->queue_pair_info_.psn,
                .max_rd_atomic = 1,
                .timeout = 0x12,
                .retry_cnt = 6,
                .rnr_retry  = 0
        };
        int ret = ibv_modify_qp(this->ibv_queue_pair_, &qp_attr,
                                IBV_QP_STATE |
                                IBV_QP_TIMEOUT |
                                IBV_QP_RETRY_CNT |
                                IBV_QP_RNR_RETRY |
                                IBV_QP_SQ_PSN |
                                IBV_QP_MAX_QP_RD_ATOMIC);
        INFINITY_ASSERT(ret == 0, "infinity: Cannot transfer to RTS state"+std::string(strerror(errno)))

        INFINITY_DEBUG("infinity: QueuePair enters RTS state")
    }

    void QueuePair::NotifyCompletionEpoll(CompleteQueue queue, std::function<void(uint64_t)> fn) {
        int ret, cnt = 0;

        struct ibv_cq *ev_cq;
        struct ibv_wc wc{};
        void *ev_ctx;

        char info[64];

        auto channel = (queue == CompleteQueue::RECV_COMPLETE_QUEUE)
                       ? this->ibv_recv_comp_channel_
                       : this->ibv_send_comp_channel_;
        int flags = fcntl(channel->fd, F_GETFL);
        int rc = fcntl(channel->fd, F_SETFL, flags | O_NONBLOCK);
        INFINITY_ASSERT(rc >= 0, "infinity: Failed to change file descriptor of Completion Event Channel")

        // 将完成事件文件描述符添加到 epoll 实例中
        int epoll_fd = epoll_create1(0);
        struct epoll_event event;
        event.events = EPOLLIN | EPOLLET;
        event.data.fd = channel->fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, channel->fd, &event);

        // Block until a comp_event arrives
        struct epoll_event events[10];
        int n = epoll_wait(epoll_fd, events, 10, -1);

        ret = ibv_get_cq_event(channel, &ev_cq, &ev_ctx);
        INFINITY_ASSERT(ret == 0, "infinity: Failed to get cq_event")

        for (; (ret=ibv_poll_cq(ev_cq, 1, &wc)) > 0; cnt++) {
            if (wc.status == IBV_WC_SUCCESS) {
                if (fn != nullptr) {
                    sprintf(info, "wr_id = %lu, wc_status = %x", wc.wr_id, wc.status);

                    DLOG(INFO) <<info;
                    fn(wc.wr_id);
                } else {
                    sprintf(info, "wr_id = %lu, wc_status = %x", wc.wr_id, wc.status);

                    INFINITY_DEBUG(info)
                }
            } else {
                sprintf(info, "wr_id = %lu, wc_status = %x, wc_vendor_err = %x", wc.wr_id, wc.status, wc.vendor_err);

                INFINITY_ASSERT(false, info)
            }
        }

        DLOG(INFO) <<"ibv_poll_cq finished, cnt is "<<cnt<<" ret is "<<ret;

        ibv_ack_cq_events(ev_cq, cnt);
    }

    void QueuePair::ConsumeCompletion(CompleteQueue queue) {
        auto channel = (queue == CompleteQueue::RECV_COMPLETE_QUEUE)
                       ? this->ibv_recv_comp_channel_
                       : this->ibv_send_comp_channel_;
        int ret, cnt = 0;
    }

}
