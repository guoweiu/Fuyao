//
// Created by tank on 9/5/22.
//

#ifndef LUMINE_HOST_BENCH_INFINITY_H
#define LUMINE_HOST_BENCH_INFINITY_H

#include <infiniband/verbs.h>

#include <string>

#include "base/logging.h"
#include "rdma/rdma_macro.h"
#include "rdma/queue_pair.h"

namespace faas::rdma {

    class Infinity {

        friend class QueuePair;

    public:

        Infinity(const std::string &device_name, uint8_t ibv_device_port, uint8_t ibv_device_gid_index);

        ~Infinity();

        QueuePair *CreateQueuePair(bool used_shared_cq = false);

    private:
        // IB device
        struct ibv_device *ibv_device_;

        // IB context
        struct ibv_context *ibv_context_;

        // IB send complete channel
        struct ibv_comp_channel *ibv_send_comp_channel_;

        // IB recv complete channel
        struct ibv_comp_channel *ibv_recv_comp_channel_;

        // protection domain
        struct ibv_pd *ibv_protection_domain_;

        // local device id
        uint16_t ibv_device_local_id_;

        // device port
        uint8_t ibv_device_port_;

        uint8_t ibv_device_gid_index_;

        union ibv_gid gid_;

        // IB send completion queue
        ibv_cq *ibv_send_completion_queue_;

        // IB receive completion queue
        ibv_cq *ibv_receive_completion_queue_;

        // Shared receive queue
        ibv_srq *ibv_shared_receive_queue_;
    };

}

#endif //LUMINE_HOST_BENCH_INFINITY_H
