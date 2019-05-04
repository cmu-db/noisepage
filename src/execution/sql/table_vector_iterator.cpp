#include "execution/sql/table_vector_iterator.h"

#include <numeric>
#include <utility>
#include <vector>

#include "execution/logging/logger.h"

namespace tpl::sql {

// Iterate over the table and select all columns
TableVectorIterator::TableVectorIterator(const u16 table_id)
    : block_iterator_(table_id), initialized_(false) {}

// Iterate over the table, but only select the given columns
TableVectorIterator::TableVectorIterator(const u16 table_id,
                                         std::vector<u32> column_indexes)
    : column_indexes_(std::move(column_indexes)),
      block_iterator_(table_id),
      initialized_(false) {}

bool TableVectorIterator::Init() {
  // No-op if already initialized
  if (initialized_) {
    return true;
  }

  // If we can't initialize the block iterator, fail
  if (!block_iterator_.Init()) {
    return false;
  }

  // The table schema
  const auto &table_schema = block_iterator_.table()->schema();

  // If the column indexes vector is empty, select all the columns
  if (column_indexes_.empty()) {
    column_indexes_.resize(table_schema.num_columns());
    std::iota(column_indexes_.begin(), column_indexes_.end(), u32{0});
  }

  // Collect column metadata for the iterators
  std::vector<const Schema::ColumnInfo *> col_infos(column_indexes_.size());
  for (u32 idx = 0; idx < column_indexes_.size(); idx++) {
    col_infos[idx] = table_schema.GetColumnInfo(idx);
  }

  // Configure the vector projection
  vector_projection_.Setup(col_infos, kDefaultVectorSize);

  // Create the column iterators
  column_iterators_.reserve(col_infos.size());
  for (const auto *col_info : col_infos) {
    column_iterators_.emplace_back(col_info);
  }

  // All good
  initialized_ = true;
  return true;
}

void TableVectorIterator::RefreshVectorProjection() {
  //
  // Setup the column's data in the vector projection with new data from the
  // column iterators
  //

  for (u32 col_idx = 0; col_idx < column_iterators_.size(); col_idx++) {
    vector_projection_.ResetColumn(column_iterators_, col_idx);
  }

  // Insert our vector projection instance into the vector projection iterator
  vector_projection_iterator_.SetVectorProjection(&vector_projection_);
}

bool TableVectorIterator::Advance() {
  // Cannot advance if not initialized
  if (!initialized_) {
    return false;
  }

  // First, we try to advance all the column iterators. We issue Advance()
  // calls to **all** column iterators to make sure they're consistent. If we're
  // able to advance all the column iterators, then we're certain there is
  // another vector of input; in this case, we just need to set up the
  // vector projection iterator with the new data and finish.
  //
  // Typically, either all column iterators can advance or non advance. If any
  // one of the column iterators says they're out of data, we advance the
  // table's block iterator looking for another block of input data. If there is
  // another block, we refresh the column iterators with the new block and
  // notify the vector projection of the new column data.
  //
  // If we are unable to advance the column iterators and the table's block
  // iterator, there isn't any more data to iterate over.

  bool advanced = true;
  for (auto &col_iter : column_iterators_) {
    advanced &= col_iter.Advance();
  }

  if (advanced) {
    RefreshVectorProjection();
    return true;
  }

  // Check block iterator
  if (block_iterator_.Advance()) {
    const Table::Block *block = block_iterator_.current_block();
    for (u32 i = 0; i < column_iterators_.size(); i++) {
      const ColumnSegment *col = block->GetColumnData(i);
      column_iterators_[i].Reset(col);
    }
    RefreshVectorProjection();
    return true;
  }

  return false;
}

}  // namespace tpl::sql
