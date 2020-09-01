#pragma once

#include <Disks/IVolume.h>

namespace DB
{

class SingleDiskVolume : public IVolume
{
public:
    SingleDiskVolume(const String & name_, DiskPtr disk);

    ReservationPtr reserve(UInt64 bytes) override;

    VolumeType getType() const override;

    void setAllowMergesUserOverride(bool /*allow*/) override;

    bool areMergesAllowed() const override;

    size_t getMaxDataPartSize() const override;
};

using VolumeSingleDiskPtr = std::shared_ptr<SingleDiskVolume>;
using VolumesSingleDiskPtr = std::vector<VolumeSingleDiskPtr>;

}
