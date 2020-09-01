#include "createVolume.h"

#include <Common/quoteString.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/MultiDiskVolume.h>
#include <Disks/VolumeJBOD.h>
#include <Disks/VolumeRAID1.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_RAID_TYPE;
    extern const int INVALID_RAID_TYPE;
}

VolumePtr createVolumeFromReservation(const ReservationPtr & reservation, VolumePtr original_volume)
{
    if (original_volume->getType() == VolumeType::JBOD || original_volume->getType() == VolumeType::SINGLE_DISK)
    {
        /// Since reservation on JBOD chooses one of disks and makes reservation there, volume
        /// for such type of reservation will be with one disk.
        return std::make_shared<SingleDiskVolume>(original_volume->getName(), reservation->getDisk());
    }
    if (original_volume->getType() == VolumeType::RAID1 || original_volume->getType() == VolumeType::MULTI_DISK)
    {
        auto volume = std::dynamic_pointer_cast<VolumeRAID1>(original_volume);
        return std::make_shared<MultiDiskVolume>(volume->getName(), reservation->getDisks());
    }
    return nullptr;
}

VolumePtr createVolumeFromConfig(
    String name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector
)
{
    String raid_type = config.getString(config_prefix + ".raid_type", "JBOD");
    if (raid_type == "JBOD")
    {
        return std::make_shared<VolumeJBOD>(name, config, config_prefix, disk_selector);
    }
    throw Exception("Unknown RAID type '" + raid_type + "'", ErrorCodes::UNKNOWN_RAID_TYPE);
}

VolumePtr updateVolumeFromConfig(
    VolumePtr volume,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr & disk_selector
)
{
    String raid_type = config.getString(config_prefix + ".raid_type", "JBOD");
    if (raid_type == "JBOD")
    {
        VolumeJBODPtr volume_jbod = std::dynamic_pointer_cast<VolumeJBOD>(volume);
        if (!volume_jbod)
            throw Exception("Invalid RAID type " + backQuote(raid_type) + ", shall be JBOD", ErrorCodes::INVALID_RAID_TYPE);

        return std::make_shared<VolumeJBOD>(*volume_jbod, config, config_prefix, disk_selector);
    }
    throw Exception("Unknown RAID type " + backQuote(raid_type), ErrorCodes::UNKNOWN_RAID_TYPE);
}

}
