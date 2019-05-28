#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/sql_table.h"

namespace tpl::sql {
using terrier::catalog::db_oid_t;
using terrier::catalog::table_oid_t;
using terrier::storage::DataTable;
using terrier::transaction::TransactionContext;

/// An iterator over a table's data in vector-wise fashion
class TableVectorIterator {
 public:
  /// Create a new vectorized iterator over the given table
  explicit TableVectorIterator(u32 db_oid, u32 table_oid, terrier::transaction::TransactionContext *txn = nullptr);

  // Destructor
  ~TableVectorIterator();

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /// Advance the iterator by a vector of input
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance();

  /// Initialize the iteration
  bool Init();

  /// Return the iterator over the current active ProjectedColumns
  ProjectedColumnsIterator *projected_columns_iterator() { return &pci_; }

 private:
  const db_oid_t db_oid_;
  const table_oid_t table_oid_;
  TransactionContext *txn_ = nullptr;

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
  std::unique_ptr<DataTable::SlotIterator> iter_ = nullptr;
};

}  // namespace tpl::sql
