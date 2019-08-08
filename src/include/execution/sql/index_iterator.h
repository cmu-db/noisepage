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
class IndexIterator {
 public:
  /**
   * Constructor
   * @param table_oid oid of the table
   * @param index_oid oid of the index to iterate over.
   * @param exec_ctx execution containing of this query
   */
  explicit IndexIterator(uint32_t table_oid, uint32_t index_oid, exec::ExecutionContext *exec_ctx);

  /**
   * Initialize the projected row and begin scanning.
   */
  void Init();

  /**
   * Frees allocated resources.
   */
  ~IndexIterator();

  /**
   * Add a column to the list of columns to select
   * @param col_oid oid of the column to select
   */
  void AddCol(u32 col_oid) { col_oids_.emplace_back(col_oid); }

  /**
   * Wrapper around the index's ScanKey
   */
  void ScanKey() {
    // Scan the index
    tuples_.clear();
    curr_index_ = 0;
    index_->ScanKey(*exec_ctx_->GetTxn(), *index_pr_, &tuples_);
  }

  /**
   * Advances the iterator. Return true if successful
   * @return whether the iterator was advanced or not.
   */
  bool Advance() {
    if (curr_index_ < tuples_.size()) {
      table_->Select(exec_ctx_->GetTxn(), tuples_[curr_index_], table_pr_);
      ++curr_index_;
      return true;
    }
    return false;
  }

  /**
   * Get a pointer to the value in the column at index @em col_idx
   * @tparam T The desired data type stored in the vector projection
   * @tparam Nullable Whether the column is NULLable
   * @param col_idx The index of the column to read from
   * @param[out] null null Whether the given column is null
   * @return The typed value at the current iterator position in the column
   */
  template <typename T, bool Nullable>
  const T *Get(u16 col_idx, bool *null) const {
    if constexpr (Nullable) {
      TPL_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
      *null = table_pr_->IsNull(col_idx);
    }
    return reinterpret_cast<T *>(table_pr_->AccessWithNullCheck(col_idx));
  }

  /**
   * Sets the index key value at the given index
   * @tparam T type of value
   * @param col_idx index of the key
   * @param value value to write
   * @param null whether the value is null
   */
  template <typename T>
  void SetKey(u16 col_idx, T value, bool null) {
    if (null) {
      index_pr_->SetNull(static_cast<u16>(col_idx));
    } else {
      *reinterpret_cast<T *>(index_pr_->AccessForceNotNull(col_idx)) = value;
    }
  }

 private:
  exec::ExecutionContext *exec_ctx_;
  uint32_t curr_index_ = 0;
  byte *index_buffer_;
  byte *table_buffer_;
  storage::ProjectedRow *index_pr_;
  storage::ProjectedRow *table_pr_;
  common::ManagedPointer<storage::index::Index> index_;
  common::ManagedPointer<storage::SqlTable> table_;
  const catalog::Schema &schema_;
  std::vector<catalog::col_oid_t> col_oids_;
  std::vector<storage::TupleSlot> tuples_{};
};

}  // namespace terrier::execution::sql
