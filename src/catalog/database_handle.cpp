#include "catalog/database_handle.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/main_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                oid_t oid) {
  // Each database handle can only see entry with the same oid
  if (oid_ != oid) return nullptr;

  // TODO(yangjun): we can cache this
  std::vector<col_oid_t> cols;
  for (const auto &c : pg_database_->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = pg_database_->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  // Find the row using sequential scan
  auto tuple_iter = pg_database_->begin();
  for (; tuple_iter != pg_database_->end(); tuple_iter++) {
    pg_database_->Select(txn, *tuple_iter, read);
    if ((*reinterpret_cast<oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid_) {
      return std::make_shared<DatabaseEntry>(oid_, read, row_pair.second);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

}  // namespace terrier::catalog
