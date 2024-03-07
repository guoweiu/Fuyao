#ifndef LUMINE_BUFFER_POOL_H
#define LUMINE_BUFFER_POOL_H

#include "base/common.h"
#include "common/uv.h"

namespace faas::utils {

// BufferPool is NOT thread-safe
    class BufferPool {
    public:
        BufferPool(std::string_view pool_name, size_t buffer_size)
                : pool_name_(std::string(pool_name)), buffer_size_(buffer_size) {}

        ~BufferPool() {}

        void Get(char **buf, size_t *size) {
            if (available_buffers_.empty()) {
                std::unique_ptr<char[]> new_buffer(new char[buffer_size_]);
                available_buffers_.push_back(new_buffer.get());
                all_buffers_.push_back(std::move(new_buffer));
            }
            *buf = available_buffers_.back();
            available_buffers_.pop_back();
            *size = buffer_size_;

//            LOG(INFO) << "BufferPool[" << pool_name_ << "]: Allocate buffer, "
//                      << fmt::format("available/all : {}/{} ", available_buffers_.size(), all_buffers_.size());
        }

        void Get(uv_buf_t *buf) {
            Get(&buf->base, &buf->len);
        }

        void Return(char *buf) {
            available_buffers_.push_back(buf);

//            LOG(INFO) << "BufferPool[" << pool_name_ << "]: Reclaim buffer, "
//                      << fmt::format("available/all : {}/{} ", available_buffers_.size(), all_buffers_.size());
        }

        void Return(const uv_buf_t *buf) {
            DCHECK_EQ(buf->len, buffer_size_);
            Return(buf->base);
        }

    private:
        std::string pool_name_;
        size_t buffer_size_;
        absl::InlinedVector<char *, 16> available_buffers_;
        absl::InlinedVector<std::unique_ptr<char[]>, 16> all_buffers_;

        DISALLOW_COPY_AND_ASSIGN(BufferPool);
    };

}  // namespace faas

#endif //LUMINE_BUFFER_POOL_H