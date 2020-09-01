#pragma once

#include <Disks/VolumeRAID1.h>

namespace DB
{

class MultiDiskVolume : public IVolume
{
public:
    MultiDiskVolume(const String & name_, Disks disks_);

    ReservationPtr reserve(UInt64 bytes) override;

    VolumeType getType() const override;

    void setAllowMergesUserOverride(bool /*allow*/) override;

    bool areMergesAllowed() const override;

    size_t getMaxDataPartSize() const override;
};

using VolumeMultiDiskPtr = std::shared_ptr<MultiDiskVolume>;
using VolumesMultiDiskPtr = std::vector<VolumeMultiDiskPtr>;

}
