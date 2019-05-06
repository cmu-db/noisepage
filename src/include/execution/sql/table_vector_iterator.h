#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/sql_table.h"

namespace tpl::sql {
using namespace terrier;
/// An iterator over a table's data in vector-wise fashion
class TableVectorIterator {
 public:
  /// Create a new vectorized iterator over the given table
  explicit TableVectorIterator(
      u32 db_oid, u32 table_oid,
      terrier::transaction::TransactionContext *txn = nullptr);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /// Advance the iterator by a vector of input
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance();

  // TODO(Amadou): Ask Prashant what Init is for.
  bool Init();

  /// Return the iterator over the current active ProjectedColumns
  ProjectedColumnsIterator *projected_columns_iterator() { return &pci_; }

 private:
  const catalog::db_oid_t db_oid_;
  const catalog::table_oid_t table_oid_;
  transaction::TransactionContext *txn_ = nullptr;

  // TODO(Amadou): These are temporary variables until transactions are in
  // And until the Init() logic is in the bytecode emitter.
  bool null_txn_;
  bool initialized = false;

  // SqlTable to iterate over
  std::shared_ptr<terrier::catalog::SqlTableRW> catalog_table_;

  // The PCI
  ProjectedColumnsIterator pci_;

  // A PC and its buffer of the PC.
  byte *buffer_ = nullptr;
  terrier::storage::ProjectedColumns *projected_columns_ = nullptr;
  // Iterator of the slots in the PC
  std::unique_ptr<storage::DataTable::SlotIterator> iter_ = nullptr;
};

}  // namespace tpl::sql
