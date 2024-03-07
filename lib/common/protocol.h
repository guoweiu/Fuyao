#ifndef LUMINE_PROTOCOL_H
#define LUMINE_PROTOCOL_H

#include "base/common.h"
#include "common/time.h"
#include "infiniband/verbs.h"

namespace faas::protocol {

    constexpr int kFuncIdBits = 8;
    constexpr int kMethodIdBits = 6;
    constexpr int kClientIdBits = 14;

    constexpr int kMaxFuncId = (1 << kFuncIdBits) - 1;
    constexpr int kMaxMethodId = (1 << kMethodIdBits) - 1;
    constexpr int kMaxClientId = (1 << kClientIdBits) - 1;

    enum class RDMAFlag : uint16_t {
        IGNORE = 0,
        REQUEST = 1,
        RESPONSE = 2,
        RECLAIM = 3,
    };

    enum class DRCType : uint16_t {
        GENERAL = 0,
        METADATA = 1,
    };

    struct HeadForDRC {
        uint16_t drc_type: 4;
        uint64_t data_size: 60;
    } __attribute__ ((packed));
    static_assert(sizeof(HeadForDRC) == 8, "Unexpected HeadForDRC size");

    inline bool IsDRCMetaData(const HeadForDRC &message) {
        return static_cast<DRCType>(message.drc_type) == DRCType::METADATA;
    }

    inline bool IsDRCGeneral(const HeadForDRC &message) {
        return static_cast<DRCType>(message.drc_type) == DRCType::GENERAL;
    }

    struct FuncCall {
        union {
            struct {
                uint16_t func_id: 8;
                uint16_t method_id: 6;
                uint16_t client_id: 14;
                uint32_t call_id: 32;
                uint16_t padding: 4;
            } __attribute__ ((packed));
            uint64_t full_call_id;
        };
        uint16_t engine_guid; /* used to distinguish engine */
        uint16_t rdma_flag;
    };
    static_assert(sizeof(FuncCall) == 16, "Unexpected FuncCall size");

    constexpr FuncCall kInvalidFuncCall = {.full_call_id = 0};

#define NEW_EMPTY_FUNC_CALL(var)      \
    FuncCall var;                     \
    memset(&var, 0, sizeof(FuncCall))

    inline FuncCall NewFuncCall(uint16_t func_id, uint16_t client_id, uint32_t call_id, uint64_t engine_guid = 0) {
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = func_id;
        func_call.client_id = client_id;
        func_call.call_id = call_id;
        func_call.engine_guid = engine_guid;
        return func_call;
    }

    inline FuncCall NewFuncCallWithMethod(uint16_t func_id, uint16_t method_id,
                                          uint16_t client_id, uint32_t call_id, uint64_t engine_guid = 0) {
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = func_id;
        func_call.method_id = method_id;
        func_call.client_id = client_id;
        func_call.call_id = call_id;
        func_call.engine_guid = engine_guid;
        return func_call;
    }

    inline std::string FuncCallDebugString(const FuncCall &func_call) {
        if (func_call.method_id == 0) {
            return fmt::format("func_id={}, client_id={}, call_id={}",
                               func_call.func_id, func_call.client_id, func_call.call_id);
        } else {
            return fmt::format("func_id={}, method_id={}, client_id={}, call_id={}",
                               func_call.func_id, func_call.method_id,
                               func_call.client_id, func_call.call_id);
        }
    }

    enum class MessageType : uint16_t {
        INVALID = 0,
        ENGINE_HANDSHAKE = 1,
        LAUNCHER_HANDSHAKE = 2,
        FUNC_WORKER_HANDSHAKE = 3,
        HANDSHAKE_RESPONSE = 4,
        CREATE_FUNC_WORKER = 5,
        INVOKE_FUNC = 6,
        DISPATCH_FUNC_CALL = 7,
        FUNC_CALL_COMPLETE = 8,
        FUNC_CALL_FAILED = 9,
        AGENT_HANDSHAKE = 10,
        AGENT_HANDSHAKE_RESPONSE = 11,
        MEMORY_REGION_REGISTER = 12,
        INVOKE_FUNC_ACROSS_HOSTS = 13,
        POLLER_NOTIFICATION = 14
    };

    constexpr uint32_t kFuncWorkerUseEngineSocketFlag = 1;
    constexpr uint32_t kUseFifoForNestedCallFlag = 2;

    struct Message {
        struct {
            uint16_t message_type: 4;
            uint16_t func_id: 8;
            uint16_t method_id: 6;
            uint16_t client_id: 14;
            uint32_t call_id;
        }  __attribute__ ((packed));
        union {
            uint64_t parent_call_id;  // Used in INVOKE_FUNC, saved as full_call_id
            struct {
                int32_t dispatch_delay;   // Used in FUNC_CALL_COMPLETE, FUNC_CALL_FAILED
                int32_t processing_time;  // Used in FUNC_CALL_COMPLETE
            } __attribute__ ((packed));
        };
        int64_t send_timestamp;
        int32_t payload_size;  // Used in HANDSHAKE_RESPONSE, INVOKE_FUNC, FUNC_CALL_COMPLETE
        uint32_t flags;
        uint16_t engine_guid;
        struct {
            uint16_t rdma_flag: 2;
            uint16_t source_func_id: 14;
        } __attribute__ ((packed));
        uint64_t timeout_for_drc; // Only will be used in the response that engine sends to func worker
        char padding[__FAAS_CACHE_LINE_SIZE - 50];
        char inline_data[__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE]
                __attribute__ ((aligned (__FAAS_CACHE_LINE_SIZE)));
    };

    struct SourceFuncWorkerInfo {
        uint16_t func_id;
        uint16_t client_id;
    };

    struct MessageTuple {
        union {
            SourceFuncWorkerInfo source;
            uint32_t identifier;
        };
        uint32_t call_id;

        bool operator<(const MessageTuple &x) const {
            return this->identifier < x.identifier;
        }
    };


#define MESSAGE_INLINE_DATA_SIZE (__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE)
    static_assert(sizeof(Message) == __FAAS_MESSAGE_SIZE, "Unexpected Message size");

    struct GatewayMessage {
        struct {
            uint16_t message_type: 4;
            uint16_t func_id: 8;
            uint16_t method_id: 6;
            uint16_t client_id: 14;
            uint32_t call_id;
        }  __attribute__ ((packed));
        union {
            // Used in ENGINE_HANDSHAKE, AGENT_HANDSHAKE
            struct {
                uint16_t node_id;
                uint16_t conn_id;
            } __attribute__ ((packed));
            int32_t processing_time; // Used in FUNC_CALL_COMPLETE
            int32_t status_code;     // Used in FUNC_CALL_FAILED
        };
        int32_t payload_size;        // Used in INVOKE_FUNC, FUNC_CALL_COMPLETE
    } __attribute__ ((packed));

    struct AgentMessage {
        struct {
            uint16_t message_type: 4;
            uint16_t engine_guid: 12; // used in AGENT_HANDSHAKE
        } __attribute__((packed));
        union {
            // Queue pair Info
            struct {
                uint16_t lid;
                uint32_t qp_num;
                uint32_t psn;
                union ibv_gid gid;
            };
            // Memory Region Info
            struct {
                uint64_t addr;
                uint64_t length;
                uint32_t lkey;
                uint32_t rkey;
                uint16_t padding;
            };
        };
    } __attribute__ ((packed));

    static_assert(sizeof(GatewayMessage) == 16, "Unexpected GatewayMessage size");
    static_assert(sizeof(Message) == 1024, "Unexpected Message size");
    static_assert(sizeof(AgentMessage) == 34, "Unexpected AgentMessage size");
    static_assert(sizeof(MessageTuple) == 8, "Unexpected MessageTuple size");

    inline bool IsEngineHandshakeMessage(const GatewayMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::ENGINE_HANDSHAKE;
    }

    inline bool IsLauncherHandshakeMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::LAUNCHER_HANDSHAKE;
    }

    inline bool IsFuncWorkerHandshakeMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_WORKER_HANDSHAKE;
    }

    inline bool IsHandshakeResponseMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::HANDSHAKE_RESPONSE;
    }

    inline bool IsCreateFuncWorkerMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::CREATE_FUNC_WORKER;
    }

    inline bool IsInvokeFuncMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::INVOKE_FUNC;
    }

    inline bool IsInvokeFuncMessageAcrossHosts(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::INVOKE_FUNC_ACROSS_HOSTS;
    }

    inline bool IsDispatchFuncCallMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
    }

    inline bool IsDispatchFuncCallMessage(const GatewayMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
    }

    inline bool IsFuncCallCompleteMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
    }

    inline bool IsFuncCallCompleteMessage(const GatewayMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
    }

    inline bool IsFuncCallFailedMessage(const Message &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
    }

    inline bool IsFuncCallFailedMessage(const GatewayMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
    }

    inline bool IsAgentHandshakeMessage(const AgentMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::AGENT_HANDSHAKE;
    }

    inline bool IsAgentHandshakeResponseMessage(const AgentMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::AGENT_HANDSHAKE_RESPONSE;
    }

    inline bool IsMemoryRegionRegisterMessage(const AgentMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::MEMORY_REGION_REGISTER;
    }

    inline bool IsRDMARequestMessage(const Message &message) {
        return static_cast<RDMAFlag>(message.rdma_flag) == RDMAFlag::REQUEST;
    }

    inline bool IsRDMAResponseCompleteMessage(const Message &message) {
        return static_cast<RDMAFlag>(message.rdma_flag) == RDMAFlag::RESPONSE;
    }

    inline bool IsRDMAReclaimMessage(const Message &message) {
        return static_cast<RDMAFlag>(message.rdma_flag) == RDMAFlag::RECLAIM;
    }

    inline bool IsRDMAReclaimCompletionMessage(const Message &message) {
        return static_cast<RDMAFlag>(message.rdma_flag) == RDMAFlag::RECLAIM;
    }

    inline bool IsRDMARequestCompleteMessage(const Message &message) {
        return static_cast<RDMAFlag>(message.rdma_flag) == RDMAFlag::REQUEST;
    }

    inline bool IsPollerNotification(const AgentMessage &message) {
        return static_cast<MessageType>(message.message_type) == MessageType::POLLER_NOTIFICATION;
    }

    inline void SetFuncCallInMessage(Message *message, const FuncCall &func_call) {
        message->func_id = func_call.func_id;
        message->method_id = func_call.method_id;
        message->client_id = func_call.client_id;
        message->call_id = func_call.call_id;
        message->engine_guid = func_call.engine_guid;
        message->rdma_flag = func_call.rdma_flag;
    }

    inline void SetFuncCallInMessage(GatewayMessage *message, const FuncCall &func_call) {
        message->func_id = func_call.func_id;
        message->method_id = func_call.method_id;
        message->client_id = func_call.client_id;
        message->call_id = func_call.call_id;
    }

    inline FuncCall GetFuncCallFromMessage(const Message &message) {
        DCHECK(IsInvokeFuncMessage(message) || IsInvokeFuncMessageAcrossHosts(message)
               || IsDispatchFuncCallMessage(message)
               || IsFuncCallCompleteMessage(message)
               || IsFuncCallFailedMessage(message));
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = message.func_id;
        func_call.method_id = message.method_id;
        func_call.client_id = message.client_id;
        func_call.call_id = message.call_id;
        func_call.engine_guid = message.engine_guid;
        func_call.rdma_flag = message.rdma_flag;
        return func_call;
    }

    inline MessageTuple GetMessageTupleFromMessage(const Message &message) {
        MessageTuple message_tuple = {
                .source = {
                        .func_id = message.source_func_id,
                        .client_id = message.client_id
                },
                .call_id = message.call_id
        };
        return message_tuple;
    }

    inline FuncCall GetFuncCallFromMessage(const GatewayMessage &message, uint64_t engine_guid = 0) {
        DCHECK(IsDispatchFuncCallMessage(message)
               || IsFuncCallCompleteMessage(message)
               || IsFuncCallFailedMessage(message));
        NEW_EMPTY_FUNC_CALL(func_call);
        func_call.func_id = message.func_id;
        func_call.method_id = message.method_id;
        func_call.client_id = message.client_id;
        func_call.call_id = message.call_id;
        func_call.rdma_flag = 0;
        func_call.engine_guid = engine_guid;
        return func_call;
    }

#undef NEW_EMPTY_FUNC_CALL

    inline void SetInlineDataInMessage(Message *message, gsl::span<const char> data) {
        message->payload_size = gsl::narrow_cast<int32_t>(data.size());
        DCHECK(data.size() <= MESSAGE_INLINE_DATA_SIZE);
        if (data.size() > 0) {
            memcpy(message->inline_data, data.data(), data.size());
        }
    }

    inline gsl::span<const char> GetInlineDataFromMessage(const Message &message) {
        if (IsInvokeFuncMessage(message)
            || IsDispatchFuncCallMessage(message)
            || IsFuncCallCompleteMessage(message)
            || IsLauncherHandshakeMessage(message)) {
            if (message.payload_size > 0) {
                return gsl::span<const char>(
                        message.inline_data, gsl::narrow_cast<size_t>(message.payload_size));
            }
        }
        return gsl::span<const char>();
    }

    inline int32_t ComputeMessageDelay(const Message &message) {
        if (message.send_timestamp > 0) {
            return gsl::narrow_cast<int32_t>(GetMonotonicMicroTimestamp() - message.send_timestamp);
        } else {
            return -1;
        }
    }

#define NEW_EMPTY_MESSAGE(var)       \
    Message var;                     \
    memset(&var, 0, sizeof(Message))

    inline Message NewLauncherHandshakeMessage(uint16_t func_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::LAUNCHER_HANDSHAKE);
        message.func_id = func_id;
        return message;
    }

    inline Message NewFuncWorkerHandshakeMessage(uint16_t func_id, uint16_t client_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_WORKER_HANDSHAKE);
        message.func_id = func_id;
        message.client_id = client_id;
        return message;
    }

    inline Message NewHandshakeResponseMessage(uint32_t payload_size) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::HANDSHAKE_RESPONSE);
        message.payload_size = payload_size;
        return message;
    }

    inline Message NewCreateFuncWorkerMessage(uint16_t client_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::CREATE_FUNC_WORKER);
        message.client_id = client_id;
        return message;
    }

    inline Message NewInvokeFuncMessage(const FuncCall &func_call, uint64_t parent_call_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
        SetFuncCallInMessage(&message, func_call);
        message.parent_call_id = parent_call_id;
        return message;
    }

    inline Message NewRDMAReclaimMessage(MessageTuple message_tuple) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        message.func_id = message_tuple.source.func_id;
        message.client_id = message_tuple.source.client_id;
        message.rdma_flag = static_cast<uint16_t>(RDMAFlag::RECLAIM);
        return message;
    }

    inline Message NewRDMAReclaimCompleteMessage(const FuncCall &func_call, uint16_t source_func_id, uint16_t flag) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        message.func_id = func_call.func_id;
        message.client_id = func_call.client_id;
        message.rdma_flag = static_cast<uint16_t>(RDMAFlag::RECLAIM);
        message.source_func_id = source_func_id;
        message.flags = flag; // 1: has DRCs that have not been released
        return message;
    }

    inline Message NewRDMAResponseCompleteMessage(const FuncCall &func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        message.func_id = func_call.func_id;
        message.client_id = func_call.client_id;
        message.call_id = func_call.call_id;
        message.method_id = func_call.method_id;
        message.rdma_flag = static_cast<uint16_t>(RDMAFlag::RESPONSE);
        return message;
    }

    inline Message NewInvokeFuncAcrossHostsMessage(const FuncCall &func_call, uint64_t parent_call_id) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC_ACROSS_HOSTS);
        SetFuncCallInMessage(&message, func_call);
        message.parent_call_id = parent_call_id;
        return message;
    }

    inline Message NewDispatchFuncCallMessage(const FuncCall &func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        SetFuncCallInMessage(&message, func_call);
        return message;
    }

    inline Message NewFuncCallCompleteMessage(const FuncCall &func_call, int32_t processing_time) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        SetFuncCallInMessage(&message, func_call);
        message.processing_time = processing_time;
        return message;
    }

    inline Message NewFuncCallFailedMessage(const FuncCall &func_call) {
        NEW_EMPTY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
        SetFuncCallInMessage(&message, func_call);
        return message;
    }

#undef NEW_EMPTY_MESSAGE

#define NEW_EMPTY_GATEWAY_MESSAGE(var)       \
    GatewayMessage var;                      \
    memset(&var, 0, sizeof(GatewayMessage))

#define NEW_EMPTY_AGENT_MESSAGE(var) \
    AgentMessage var;                \
    memset(&var, 0, sizeof(AgentMessage))

    inline GatewayMessage NewEngineHandshakeGatewayMessage(uint16_t node_id, uint16_t conn_id) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::ENGINE_HANDSHAKE);
        message.node_id = node_id;
        message.conn_id = conn_id;
        return message;
    }

    inline AgentMessage
    NewAgentHandshakeResponseMessage(uint16_t lid, uint32_t qp_num, uint32_t psn, union ibv_gid gid) {
        NEW_EMPTY_AGENT_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::AGENT_HANDSHAKE_RESPONSE);
        message.lid = lid;
        message.qp_num = qp_num;
        message.psn = psn;
        message.gid = gid;
        return message;
    }

    inline AgentMessage
    NewAgentHandshakeMessage(uint16_t engine_guid, uint16_t lid, uint32_t qp_num, uint32_t psn, union ibv_gid gid) {
        NEW_EMPTY_AGENT_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::AGENT_HANDSHAKE);
        message.engine_guid = engine_guid;
        message.lid = lid;
        message.qp_num = qp_num;
        message.psn = psn;
        message.gid = gid;
        return message;
    }

    inline AgentMessage
    NewMemoryRegionRegisterMessage(uint64_t addr, uint64_t length, uint32_t lkey, uint32_t rkey) {
        NEW_EMPTY_AGENT_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::MEMORY_REGION_REGISTER);
        message.addr = addr;
        message.length = length;
        message.lkey = lkey;
        message.rkey = rkey;
        return message;
    }

    inline AgentMessage
    NewPollerNotificationMessage() {
        NEW_EMPTY_AGENT_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::POLLER_NOTIFICATION);
        return message;
    }

    inline GatewayMessage NewDispatchFuncCallGatewayMessage(const FuncCall &func_call) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
        SetFuncCallInMessage(&message, func_call);
        return message;
    }

    inline GatewayMessage NewFuncCallCompleteGatewayMessage(const FuncCall &func_call,
                                                            int32_t processing_time) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
        SetFuncCallInMessage(&message, func_call);
        message.processing_time = processing_time;
        return message;
    }

    inline GatewayMessage NewFuncCallFailedGatewayMessage(const FuncCall &func_call,
                                                          int32_t status_code = 0) {
        NEW_EMPTY_GATEWAY_MESSAGE(message);
        message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
        SetFuncCallInMessage(&message, func_call);
        message.status_code = status_code;
        return message;
    }

#undef NEW_EMPTY_GATEWAY_MESSAGE

}  // namespace faas

#endif //LUMINE_PROTOCOL_H