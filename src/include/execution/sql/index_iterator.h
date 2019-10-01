#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {
/**
 * Allows iteration for indices from TPL.
 */
class EXPORT IndexIterator {
 public:
  /**
   * Constructor
   * @param table_oid oid of the table
   * @param index_oid oid of the index to iterate over.
   * @param exec_ctx execution containing of this query
   * @param col_oids oids of the table columns
   * @param num_oids number of oids
   */
  explicit IndexIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t index_oid, uint32_t *col_oids,
                         uint32_t num_oids);

  /**
   * Initialize the projected row and begin scanning.
   */
  void Init();

  /**
   * Frees allocated resources.
   */
  ~IndexIterator();

  /**
   * Wrapper around the index's ScanKey
   */
  void ScanKey();

  /**
   * Advances the iterator. Return true if successful
   * @return whether the iterator was advanced or not.
   */
  bool Advance();

  storage::ProjectedRow *PR() { return index_pr_; }

  storage::ProjectedRow *TablePR();

  storage::TupleSlot CurrentSlot() { return tuples_[curr_index_ - 1]; }

 private:
  exec::ExecutionContext *exec_ctx_;
  std::vector<catalog::col_oid_t> col_oids_;
  common::ManagedPointer<storage::index::Index> index_;
  common::ManagedPointer<storage::SqlTable> table_;

  uint32_t curr_index_ = 0;
  void *index_buffer_;
  void *table_buffer_;
  storage::ProjectedRow *index_pr_;
  storage::ProjectedRow *table_pr_;
  std::vector<storage::TupleSlot> tuples_{};
};

}  // namespace terrier::execution::sql
