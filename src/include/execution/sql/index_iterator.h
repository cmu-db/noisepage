#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_index.h"
#include "catalog/catalog_sql_table.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/storage_defs.h"

namespace tpl::sql {
using terrier::catalog::CatalogIndex;
using terrier::catalog::SqlTableRW;
using terrier::storage::ProjectedRow;
using terrier::storage::TupleSlot;
using terrier::transaction::TransactionContext;

/**
 * Allows iteration for indices from TPL.
 */
class IndexIterator {
 public:
  /**
   * Constructor
   * @param index_oid oid of the index to over.
   * @param txn running transaction
   */
  explicit IndexIterator(uint32_t index_oid, TransactionContext *txn = nullptr);

  /**
   * Frees allocated resources.
   */
  ~IndexIterator();

  /**
   * Wrapper around the index's ScanKey
   * @param sql_key key to scan.
   */
  void ScanKey(byte *sql_key);

  /**
   * Advances the iterator.
   */
  void Advance();

  /**
   * @return true iff there are TupleSlots left
   */
  bool HasNext();

  /// Get a pointer to the value in the column at index col_idx
  /// \tparam T The desired data type stored in the ProjectedColumns
  /// \tparam nullable Whether the column is NULLable
  /// \param col_idx The index of the column to read from
  /// \param[out] null Whether the given column is null
  /// \return The typed value at the current iterator position in the column
  template <typename T, bool nullable>
  const T *Get(u32 col_idx, bool *null) const;

 private:
  TransactionContext *txn_;
  std::vector<TupleSlot> index_values_;
  uint32_t curr_index_ = 0;
  byte *index_buffer_ = nullptr;
  byte *row_buffer_ = nullptr;
  ProjectedRow *index_pr_ = nullptr;
  ProjectedRow *row_pr_ = nullptr;
  std::shared_ptr<CatalogIndex> catalog_index_ = nullptr;
  std::shared_ptr<SqlTableRW> catalog_table_ = nullptr;
};

template <typename T, bool Nullable>
inline const T *IndexIterator::Get(u32 col_idx, bool *null) const {
  if constexpr (Nullable) {
    TPL_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
    *null = row_pr_->IsNull(static_cast<u16>(col_idx));
  }
  return reinterpret_cast<T *>(row_pr_->AccessWithNullCheck(static_cast<u16>(col_idx)));
}
}  // namespace tpl::sql
