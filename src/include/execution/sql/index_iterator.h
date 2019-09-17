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

  /**
   * Get a pointer to the value in the column at index @em col_idx
   * @tparam T The desired data type stored in the vector projection
   * @tparam Nullable Whether the column is NULLable
   * @param col_idx The index of the column to read from
   * @param[out] null null Whether the given column is null
   * @return The typed value at the current iterator position in the column
   */
  template <typename T, bool Nullable>
  const T *Get(uint16_t col_idx, bool *null) const {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Nullable) {
      TERRIER_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
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
  template <typename T, bool Nullable>
  void SetKey(uint16_t col_idx, T value, bool null) {
    if constexpr (Nullable) {
      if (null) {
        index_pr_->SetNull(static_cast<uint16_t>(col_idx));
      } else {
        *reinterpret_cast<T *>(index_pr_->AccessForceNotNull(col_idx)) = value;
      }
    } else {  // NOLINT
      *reinterpret_cast<T *>(index_pr_->AccessForceNotNull(col_idx)) = value;
    }
  }

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
