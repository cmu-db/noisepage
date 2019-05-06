#pragma once

#include "catalog/catalog_defs.h"
#include "catalog/catalog_index.h"
#include "catalog/catalog_sql_table.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/storage_defs.h"

namespace tpl::sql {
using namespace terrier;
class IndexIterator {
 public:
  // Constructs the iterator for the given index
  explicit IndexIterator(uint32_t index_oid,
                         transaction::TransactionContext *txn = nullptr);

  // Frees allocated resources.
  ~IndexIterator();

  // Wrapper around the index's ScanKey
  void ScanKey(byte *sql_key);

  // Advances the iterator.
  void Advance();

  // Returns true iff there are TupleSlots left
  bool HasNext();

  /// Get a pointer to the value in the column at index \ref col_idx
  /// \tparam T The desired data type stored in the ProjectedColumns
  /// \tparam nullable Whether the column is NULLable
  /// \param col_idx The index of the column to read from
  /// \param[out] null Whether the given column is null
  /// \return The typed value at the current iterator position in the column
  template <typename T, bool nullable>
  const T *Get(u32 col_idx, bool *null) const;

 private:
  transaction::TransactionContext *txn_;
  std::vector<storage::TupleSlot> index_values_;
  uint32_t curr_index_ = 0;
  byte *index_buffer_ = nullptr;
  byte *row_buffer_ = nullptr;
  storage::ProjectedRow *index_pr_ = nullptr;
  storage::ProjectedRow *row_pr_ = nullptr;
  std::shared_ptr<catalog::CatalogIndex> catalog_index_ = nullptr;
  std::shared_ptr<catalog::SqlTableRW> catalog_table_ = nullptr;
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