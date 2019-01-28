#include "catalog/tablespace_handle.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
namespace terrier::catalog {

std::shared_ptr<TablespaceHandle::TablespaceEntry> TablespaceHandle::GetTablespaceEntry(
    transaction::TransactionContext *txn, tablespace_oid_t oid) {
  storage::ProjectedRow *row = pg_tablespace_->FindRow(txn, 0, !oid);
  if (row == nullptr) {
    return nullptr;
  }

  return std::make_shared<TablespaceEntry>(pg_tablespace_, oid, row, *pg_tablespace_->GetPRMap());
}

std::shared_ptr<TablespaceHandle::TablespaceEntry> TablespaceHandle::GetTablespaceEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  storage::ProjectedRow *row = pg_tablespace_->FindRow(txn, 1, name.c_str());
  if (row == nullptr) {
    return nullptr;
  }

  // now recover the oid
  auto offset = pg_tablespace_->ColNumToOffset(0);
  tablespace_oid_t oid(*reinterpret_cast<tablespace_oid_t *>(row->AccessForceNotNull(offset)));
  return std::make_shared<TablespaceEntry>(pg_tablespace_, oid, row, *pg_tablespace_->GetPRMap());
}

}  // namespace terrier::catalog
