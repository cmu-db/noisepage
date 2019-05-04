#pragma once

#include <vector>

#include "execution/sql/column_vector_iterator.h"
#include "execution/sql/table.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"

namespace tpl::sql {

/// An iterator over a table's data in vector-wise fashion
class TableVectorIterator {
 public:
  /// Create an iterator over the table with ID \a table_id and project in all
  /// columns from the logical schema for the table
  /// \param table_id The ID of the table
  explicit TableVectorIterator(u16 table_id);

  /// Create an iterator over the table with ID \a table_id and project columns
  /// at the indexes in \a column_indexes from the logical schema for the table
  /// \param table_id The ID of the table
  /// \param column_indexes The indexes of the columns to select
  TableVectorIterator(u16 table_id, std::vector<u32> column_indexes);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /// Initialize the iterator, returning true if the initialization succeeded
  /// \return True if the initialization succeeded; false otherwise
  bool Init();

  /// Advance the iterator by a vector of input
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance();

  /// Access the table this iterator is scanning
  /// \return The table if the iterator has been initialized; null otherwise
  const Table *table() const { return block_iterator_.table(); }

  /// Return the iterator over the current active vector projection
  VectorProjectionIterator *vector_projection_iterator() {
    return &vector_projection_iterator_;
  }

 private:
  // When the column iterators receive new vectors of input, we need to
  // refresh the vector projection with new data too
  void RefreshVectorProjection();

 private:
  // The indexes in the column to read
  std::vector<u32> column_indexes_;

  // The iterate over the blocks stored in the table
  TableBlockIterator block_iterator_;

  // The vector-wise iterators over each column in the table
  std::vector<ColumnVectorIterator> column_iterators_;

  // The active vector projection
  VectorProjection vector_projection_;

  // An iterator over the currently active projection
  VectorProjectionIterator vector_projection_iterator_;

  // Has the iterator been initialized?
  bool initialized_;
};

}  // namespace tpl::sql
