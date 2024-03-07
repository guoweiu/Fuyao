#ifndef LUMINE_SHM_REGION_H
#define LUMINE_SHM_REGION_H

#include "base/common.h"
#include "base/macro.h"
#include "ipc/base.h"

namespace faas::ipc {

    class ShmRegion;

// Shm{Create, Open} returns nullptr on failure
    std::unique_ptr<ShmRegion> ShmCreate(std::string_view name, size_t size);

    std::unique_ptr<ShmRegion> ShmOpen(std::string_view name, bool readonly = true);

    class ShmRegion {
    public:
        ~ShmRegion();

        void EnableRemoveOnDestruction() { remove_on_destruction_ = true; }

        void DisableRemoveOnDestruction() { remove_on_destruction_ = false; }

        char *base() { return base_; }

        const char *base() const { return base_; }

        size_t size() const { return size_; }

        gsl::span<const char> to_span() const {
            return gsl::span<const char>(base_, size_);
        }

    private:
        // base_ is the ptr of shared memory region
        ShmRegion(std::string_view name, char *base, size_t size)
                : name_(name), base_(base), size_(size), remove_on_destruction_(false) {}

        std::string name_;
        char *base_;
        size_t size_;
        bool remove_on_destruction_;

        friend std::unique_ptr<ShmRegion> ShmCreate(std::string_view name, size_t size);

        friend std::unique_ptr<ShmRegion> ShmOpen(std::string_view name, bool readonly);

        DISALLOW_COPY_AND_ASSIGN(ShmRegion);
    };

}  // namespace faas

#endif