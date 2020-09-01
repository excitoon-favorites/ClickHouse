#include <Disks/MultiDiskVolume.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MultiDiskVolume::MultiDiskVolume(const String & name_, Disks disks_)
    : IVolume(name_, disks_)
{
}


ReservationPtr MultiDiskVolume::reserve(UInt64 bytes)
{
    /// FIXME. We don't check `max_data_part_size` here.
    /// That is possibly okay.

    Reservations res(disks.size());
    for (size_t i = 0; i < disks.size(); ++i)
    {
        res[i] = disks[i]->reserve(bytes);

        if (!res[i])
            return {};
    }

    return std::make_unique<MultiDiskReservation>(res, bytes);
}


VolumeType MultiDiskVolume::getType() const
{
    return VolumeType::MULTI_DISK;
}


void MultiDiskVolume::setAllowMergesUserOverride(bool /*allow*/)
{
    throw Exception("Attempt to access `MultiDiskVolume::setAllowMergesUserOverride()`", ErrorCodes::LOGICAL_ERROR);
}


bool MultiDiskVolume::areMergesAllowed() const
{
    throw Exception("Attempt to access `MultiDiskVolume::areMergesAllowed()`", ErrorCodes::LOGICAL_ERROR);
}


size_t MultiDiskVolume::getMaxDataPartSize() const
{
    throw Exception("Attempt to access `MultiDiskVolume::getMaxDataPartSize()`", ErrorCodes::LOGICAL_ERROR);
}

}
