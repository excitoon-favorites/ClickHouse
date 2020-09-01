#include <Disks/SingleDiskVolume.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


SingleDiskVolume::SingleDiskVolume(const String & name_, DiskPtr disk)
    : IVolume(name_, {disk})
{
}


ReservationPtr SingleDiskVolume::reserve(UInt64 bytes)
{
    return disks[0]->reserve(bytes);
}


VolumeType SingleDiskVolume::getType() const
{
    return VolumeType::SINGLE_DISK;
}


void SingleDiskVolume::setAllowMergesUserOverride(bool /*allow*/)
{
    throw Exception("Attempt to access `SingleDiskVolume::setAllowMergesUserOverride()`", ErrorCodes::LOGICAL_ERROR);
}


bool SingleDiskVolume::areMergesAllowed() const
{
    throw Exception("Attempt to access `SingleDiskVolume::areMergesAllowed()`", ErrorCodes::LOGICAL_ERROR);
}


size_t SingleDiskVolume::getMaxDataPartSize() const
{
    throw Exception("Attempt to access `SingleDiskVolume::getMaxDataPartSize()`", ErrorCodes::LOGICAL_ERROR);
}

}
