#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/vector_projection_iterator.h"
#include "storage/index/index.h"

namespace terrier::storage {
class SqlTable;
}  // namespace terrier::storage

namespace terrier::execution::sql {
/**
 * Allows iteration for indices from TPL.
 */
class EXPORT IndexIterator {
 public:
  /**
   * Constructor
   * @param exec_ctx execution containing of this query
   * @param num_attrs number of attributes set in key
   * @param table_oid oid of the table
   * @param index_oid oid of the index to iterate over.
   * @param col_oids oids of the table columns
   * @param num_oids number of oids
   */
  explicit IndexIterator(exec::ExecutionContext *exec_ctx, uint32_t num_attrs, uint32_t table_oid, uint32_t index_oid,
                         uint32_t *col_oids, uint32_t num_oids);

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
   * Perform an ascending scan
   * @param scan_type Type of Scan
   * @param limit number of tuples to limit
   */
  void ScanAscending(storage::index::ScanType scan_type, uint32_t limit);

  /**
   * Perfrom a descending scan
   */
  void ScanDescending();

  /**
   * Perform an descending scan with a limit
   */
  void ScanLimitDescending(uint32_t limit);

  /**
   * Advances the iterator. Return true if successful
   * @return whether the iterator was advanced or not.
   */
  bool Advance();

  /**
   * Return the index PR
   */
  storage::ProjectedRow *PR() { return index_pr_; }

  /**
   * Return the lower bound PR
   */
  storage::ProjectedRow *LoPR() { return index_pr_; }

  /**
   * Return the upper bound PR
   */
  storage::ProjectedRow *HiPR() { return hi_index_pr_; }

  /**
   * Perform a select.
   * @return The resulting projected row.
   */
  storage::ProjectedRow *TablePR();

  /**
   * @return The current tuple slot of the iterator.
   */
  storage::TupleSlot CurrentSlot() { return tuples_[curr_index_ - 1]; }

  /**
   * @return The size of the index.
   * TODO(WAN): This should be a uint64_t, #1049.
   */
  uint32_t GetIndexSize() const { return index_->GetSize(); }

 private:
  exec::ExecutionContext *exec_ctx_;
  uint32_t num_attrs_;
  std::vector<catalog::col_oid_t> col_oids_;
  common::ManagedPointer<storage::index::Index> index_;
  common::ManagedPointer<storage::SqlTable> table_;

  uint32_t curr_index_ = 0;
  void *index_buffer_;
  void *hi_index_buffer_;
  void *table_buffer_;
  storage::ProjectedRow *index_pr_;
  storage::ProjectedRow *hi_index_pr_;
  storage::ProjectedRow *table_pr_;
  std::vector<storage::TupleSlot> tuples_{};
};

}  // namespace terrier::execution::sql
