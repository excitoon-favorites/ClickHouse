#include <DataStreams/RecalculateMoveTTLBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RecalculateMoveTTLBlockInputStream::RecalculateMoveTTLBlockInputStream(
    const BlockInputStreamPtr & input_,
    const MergeTreeData & storage_,
    const MergeTreeData::MutableDataPartPtr & data_part_)
    : storage(storage_)
    , data_part(data_part_)
    , log(&Logger::get(storage.getLogName() + " (RecalculateMoveTTLBlockInputStream)"))
    , date_lut(DateLUT::instance())
{
    children.push_back(input_);
    header = children.at(0)->getHeader();

    const auto & column_defaults = storage.getColumns().getDefaults();
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    if (!default_expr_list->children.empty())
    {
        auto syntax_result = SyntaxAnalyzer(storage.global_context).analyze(
            default_expr_list, storage.getColumns().getAllPhysical());
        defaults_expression = ExpressionAnalyzer{default_expr_list, syntax_result, storage.global_context}.getActions(true);
    }
}

Block RecalculateMoveTTLBlockInputStream::readImpl()
{
    Block block = children.at(0)->read();
    if (!block)
        return block;

    updateMovesTTL(block);

    return block;
}

void RecalculateMoveTTLBlockInputStream::readSuffixImpl()
{
    for (const auto & elem : new_ttl_infos.columns_ttl)
        new_ttl_infos.updatePartMinMaxTTL(elem.second.min, elem.second.max);

    new_ttl_infos.updatePartMinMaxTTL(new_ttl_infos.table_ttl.min, new_ttl_infos.table_ttl.max);

    data_part->ttl_infos = std::move(new_ttl_infos);
    data_part->expired_columns = std::move(empty_columns);
}

void RecalculateMoveTTLBlockInputStream::updateMovesTTL(Block & block)
{
    std::vector<String> columns_to_remove;
    for (const auto & ttl_entry : storage.move_ttl_entries)
    {
        auto & new_ttl_info = new_ttl_infos.moves_ttl[ttl_entry.result_column];

        if (!block.has(ttl_entry.result_column))
        {
            columns_to_remove.push_back(ttl_entry.result_column);
            ttl_entry.expression->execute(block);
        }

        const IColumn * ttl_column = block.getByName(ttl_entry.result_column).column.get();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column, i);
            new_ttl_info.update(cur_ttl);
        }
    }

    for (const String & column : columns_to_remove)
        block.erase(column);
}

UInt32 RecalculateMoveTTLBlockInputStream::getTimestampByIndex(const IColumn * column, size_t ind)
{
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
        return date_lut.fromDayNum(DayNum(column_date->getData()[ind]));
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
        return column_date_time->getData()[ind];
    else if (const ColumnConst * column_const = typeid_cast<const ColumnConst *>(column))
    {
        if (typeid_cast<const ColumnUInt16 *>(&column_const->getDataColumn()))
            return date_lut.fromDayNum(DayNum(column_const->getValue<UInt16>()));
        else if (typeid_cast<const ColumnUInt32 *>(&column_const->getDataColumn()))
            return column_const->getValue<UInt32>();
    }

    throw Exception("Unexpected type of result TTL column", ErrorCodes::LOGICAL_ERROR);
}

}
