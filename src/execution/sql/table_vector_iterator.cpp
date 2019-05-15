#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/execution_structures.h"

namespace tpl::sql {

using terrier::transaction::TransactionContext;
using terrier::catalog::Schema;
using terrier::catalog::col_oid_t;
using terrier::common::AllocationUtil;
using terrier::storage::DataTable;

TableVectorIterator::TableVectorIterator(u32 db_oid, u32 table_oid,
                                         TransactionContext *txn)
    : db_oid_(db_oid),
      table_oid_(table_oid),
      txn_(txn),
      null_txn_(txn == nullptr) {}


TableVectorIterator::~TableVectorIterator() {
  delete [] buffer_;
}

bool TableVectorIterator::Init() {
  // Find the table
  auto *exec = ExecutionStructures::Instance();
  catalog_table_ = exec->GetCatalog()->GetCatalogTable(db_oid_, table_oid_);
  if (catalog_table_ == nullptr) return false;

  // Initialize the projected column
  const auto &schema = catalog_table_->GetSqlTable()->GetSchema();
  std::vector<col_oid_t> col_oids;
  const std::vector<Schema::Column> &columns = schema.GetColumns();
  col_oids.reserve(columns.size());
  for (const auto &col : columns) {
    col_oids.emplace_back(col.GetOid());
  }
  auto pri_map = catalog_table_->GetSqlTable()->InitializerForProjectedColumns(
      col_oids, kDefaultVectorSize);
  buffer_ = AllocationUtil::AllocateAligned(
      pri_map.first.ProjectedColumnsSize());
  projected_columns_ = pri_map.first.Initialize(buffer_);
  initialized = true;

  // Begin iterating
  iter_ = std::make_unique<DataTable::SlotIterator>(
      catalog_table_->GetSqlTable()->begin());
  return true;
}

bool TableVectorIterator::Advance() {
  if (!initialized) return false;
  // First check if the iterator ended.
  if (*iter_ == catalog_table_->GetSqlTable()->end()) {
    // If so, try to commit if we were the ones who created this transaction.
    if (null_txn_ && txn_ != nullptr) {
      auto *exec = ExecutionStructures::Instance();
      exec->GetTxnManager()->Commit(txn_, [](void *) { return; }, nullptr);
      txn_ = nullptr;
    }
    // End the iteration.
    return false;
  }
  // TODO(Amadou): This is a temporary fix until transactions are part of the
  // language.
  // Begin a new transaction if none was passed in.
  if (txn_ == nullptr) {
    auto *exec = ExecutionStructures::Instance();
    txn_ = exec->GetTxnManager()->BeginTransaction();
  }
  // Scan the table a set the projected column.
  catalog_table_->GetSqlTable()->Scan(txn_, iter_.get(), projected_columns_);
  pci_.SetProjectedColumn(projected_columns_);
  return true;
}

}  // namespace tpl::sql
