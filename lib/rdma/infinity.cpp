//
// Created by tank on 9/5/22.
//

#include "infinity.h"

namespace faas::rdma {

    Infinity::Infinity(const std::string &device_name, uint8_t ibv_device_port, uint8_t ibv_device_gid_index) {

        this->ibv_device_port_ = ibv_device_port;
        this->ibv_device_gid_index_ = ibv_device_gid_index;

        // Get the list of IB devices
        int num_devices;
        struct ibv_device **device_list = ibv_get_device_list(&num_devices);

        // Get IB device by device_name
        for (int i = 0; i < num_devices; i++) {
            auto device_name_tmp = ibv_get_device_name(device_list[i]);
            if (device_name == device_name_tmp) {
                ibv_device_ = device_list[i];
                break;
            }
        }
        ibv_free_device_list(device_list);
        INFINITY_ASSERT(ibv_device_ != nullptr, "infinity: Requested IB device is illegal")

        // Open IB device
        ibv_context_ = ibv_open_device(ibv_device_);
        INFINITY_ASSERT(ibv_context_ != nullptr, "infinity: Could not open device")

        // create send complete channel
        ibv_send_comp_channel_ = ibv_create_comp_channel(this->ibv_context_);
        INFINITY_ASSERT(ibv_send_comp_channel_ != nullptr, "infinity: Could not create send completion channel")

        // create recv complete channel
        ibv_recv_comp_channel_ = ibv_create_comp_channel(this->ibv_context_);
        INFINITY_ASSERT(ibv_recv_comp_channel_ != nullptr, "infinity: Could not create recv completion channel")

        int ret;

        ret = ibv_query_gid(ibv_context_, ibv_device_port, ibv_device_gid_index_, &gid_);
        INFINITY_ASSERT(ret == 0, "infinity: local port gid index is illegal")

        ibv_device_attr_ex attr{};
        ret = ibv_query_device_ex(ibv_context_, nullptr, &attr);
        INFINITY_ASSERT(ret == 0, "infinity: Could not query device for its features")

        // Ability of the device to support atomic operations
        std::string atomic_cap_info;
        switch (attr.orig_attr.atomic_cap) {
            case IBV_ATOMIC_NONE:
                atomic_cap_info = "infinity: Atomic operations aren't supported at all";
                break;
            case IBV_ATOMIC_HCA:
                atomic_cap_info = "infinity: Atomicity is guaranteed between QPs on this device only";
                break;
            default:
                atomic_cap_info = "infinity: Atomicity is guaranteed between this device and any other component, such as CPUs, IO devices and other RDMA devices";
                break;
        }
        INFINITY_DEBUG(atomic_cap_info)

        // Allocate protection domain
        ibv_protection_domain_ = ibv_alloc_pd(ibv_context_);
        INFINITY_ASSERT(ibv_protection_domain_ != nullptr, "infinity: Could not allocate protection domain")

        // Get the LID
        ibv_port_attr port_attributes{};
        ret = ibv_query_port(ibv_context_, ibv_device_port_, &port_attributes);
        INFINITY_ASSERT(ret == 0, "infinity: Requested IB port is illegal")
        ibv_device_local_id_ = port_attributes.lid;

        // Allocate send completion queue
        ibv_send_completion_queue_ = ibv_create_cq(ibv_context_, SEND_COMPLETION_QUEUE_LENGTH,
                                                   nullptr, ibv_send_comp_channel_, 0);
        INFINITY_ASSERT(ibv_send_completion_queue_ != nullptr, "infinity: Could not allocate send completion queue")

        // Allocate receive completion queue
        ibv_receive_completion_queue_ = ibv_create_cq(ibv_context_, RECV_COMPLETION_QUEUE_LENGTH,
                                                      nullptr, ibv_recv_comp_channel_, 0);
        INFINITY_ASSERT(ibv_receive_completion_queue_ != nullptr,
                        "infinity: Could not allocate receive completion queue")

        // Allocate shared receive queue
        MAKE_IBV_SRQ_INIT_ATTR(sia, ibv_context_)
        ibv_shared_receive_queue_ = ibv_create_srq(ibv_protection_domain_, &sia);
        INFINITY_ASSERT(ibv_shared_receive_queue_ != nullptr,
                        "infinity: Could not allocate receive shared receive queue")
    }

    Infinity::~Infinity() {
        int ret;

        // Destroy shared completion queue
        ret = ibv_destroy_srq(ibv_shared_receive_queue_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy shared completion queue")

        // Destroy receive completion queue
        ret = ibv_destroy_cq(ibv_receive_completion_queue_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy receive completion queue")

        // Destroy send completion queue
        ret = ibv_destroy_cq(ibv_send_completion_queue_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy send completion queue")

        // Destroy send completion channel
        ret = ibv_destroy_comp_channel(ibv_send_comp_channel_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy send completion channel")

        // Destroy recv completion channel
        ret = ibv_destroy_comp_channel(ibv_recv_comp_channel_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not destroy recv completion channel")

        // Destroy protection domain
        ret = ibv_dealloc_pd(ibv_protection_domain_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not dealloc protection domain")

        // Close device
        ret = ibv_close_device(this->ibv_context_);
        INFINITY_ASSERT(ret == 0, "infinity: Could not close IB device");
    }

    QueuePair *Infinity::CreateQueuePair(bool used_shared_cq) {
        auto queue_pair = new QueuePair(this, used_shared_cq);
        return queue_pair;
    }

};
