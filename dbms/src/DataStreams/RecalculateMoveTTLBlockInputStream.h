#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Core/Block.h>

#include <common/DateLUT.h>

namespace DB
{

class RecalculateMoveTTLBlockInputStream : public IBlockInputStream
{
public:
    RecalculateMoveTTLBlockInputStream(
        const BlockInputStreamPtr & input_,
        const MergeTreeData & storage_,
        const MergeTreeData::MutableDataPartPtr & data_part_
    );

    String getName() const override { return "RecalculateMoveTTL"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

    /// Finalizes ttl infos and updates data part
    void readSuffixImpl() override;

private:
    const MergeTreeData & storage;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;

    IMergeTreeDataPart::TTLInfos old_ttl_infos;
    IMergeTreeDataPart::TTLInfos new_ttl_infos;
    NameSet empty_columns;

    size_t rows_removed = 0;
    Logger * log;
    DateLUTImpl date_lut;

    std::unordered_map<String, String> defaults_result_column;
    ExpressionActionsPtr defaults_expression;

    Block header;
private:
    /// Updates TTL for moves
    void updateMovesTTL(Block & block);

    UInt32 getTimestampByIndex(const IColumn * column, size_t ind);
};

}
