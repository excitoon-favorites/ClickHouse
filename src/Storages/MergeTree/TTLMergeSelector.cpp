#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <algorithm>
#include <cmath>


namespace DB
{

const String & getPartitionIdForPart(const TTLMergeSelector::Part & part_info)
{
    const MergeTreeData::DataPartPtr & part = *static_cast<const MergeTreeData::DataPartPtr *>(part_info.data);
    return part->info.partition_id;
}


IMergeSelector::PartsInPartition TTLMergeSelector::select(
    const Partitions & partitions,
    const size_t max_total_size_to_merge)
{
    using Iterator = IMergeSelector::PartsInPartition::const_iterator;
    Iterator best_begin;
    ssize_t partition_to_merge_index = -1;
    time_t partition_to_merge_min_ttl = 0;

    /// Find most old TTL.
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        const auto & mergeable_parts_in_partition = partitions[i];
        if (mergeable_parts_in_partition.empty())
            continue;

        const auto & partition_id = getPartitionIdForPart(mergeable_parts_in_partition.front());
        const auto & next_merge_time_for_partition = merge_due_times[partition_id];
        if (next_merge_time_for_partition > current_time)
            continue;

        for (Iterator part_it = mergeable_parts_in_partition.cbegin(); part_it != mergeable_parts_in_partition.cend(); ++part_it)
        {
            time_t ttl = only_drop_parts ? part_it->max_ttl : part_it->min_ttl;

            if (ttl && (partition_to_merge_index == -1 || ttl < partition_to_merge_min_ttl))
            {
                if (only_drop_parts || part_it->can_participate_in_merges)
                {
                    partition_to_merge_min_ttl = ttl;
                    partition_to_merge_index = i;
                    best_begin = part_it;
                }
            }
        }
    }

    if (partition_to_merge_index == -1 || partition_to_merge_min_ttl > current_time)
        return {};

    const auto & best_partition = partitions[partition_to_merge_index];
    Iterator best_end = best_begin + 1;
    size_t total_size = 0;

    /// Find begin of range with most old TTL.
    while (true)
    {
        time_t ttl = only_drop_parts ? best_begin->max_ttl : best_begin->min_ttl;

        if (!ttl || ttl > current_time
            || (max_total_size_to_merge && total_size > max_total_size_to_merge)
            || (!only_drop_parts && !best_begin->can_participate_in_merges))
        {
            /// This condition can not be satisfied on first iteration.
            ++best_begin;
            break;
        }

        total_size += best_begin->size;
        if (best_begin == best_partition.begin())
            break;

        --best_begin;
    }

    /// Find end of range with most old TTL.
    while (best_end != best_partition.end())
    {
        time_t ttl = only_drop_parts ? best_end->max_ttl : best_end->min_ttl;

        if (!ttl || ttl > current_time
            || (max_total_size_to_merge && total_size > max_total_size_to_merge)
            || (!only_drop_parts && !best_end->can_participate_in_merges))
            break;

        total_size += best_end->size;
        ++best_end;
    }

    const auto & best_partition_id = getPartitionIdForPart(best_partition.front());
    merge_due_times[best_partition_id] = current_time + merge_cooldown_time;

    return PartsInPartition(best_begin, best_end);
}

}
