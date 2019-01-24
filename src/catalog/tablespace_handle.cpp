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
  // TODO(yangjun): we can cache this
  CATALOG_LOG_TRACE("inside tablespace handle ... ");
  std::vector<col_oid_t> cols;
  // std::cout << pg_tablespace_ << std::endl;

  std::shared_ptr<storage::SqlTable> sql_tbl = pg_tablespace_->GetSqlTable();
  for (const auto &c : sql_tbl->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = sql_tbl->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  // Find the row using sequential scan
  auto tuple_iter = sql_tbl->begin();
  for (; tuple_iter != sql_tbl->end(); tuple_iter++) {
    sql_tbl->Select(txn, *tuple_iter, read);
    if ((*reinterpret_cast<tablespace_oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid) {
      return std::make_shared<TablespaceEntry>(pg_tablespace_, oid, read, row_pair.second);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

std::shared_ptr<TablespaceHandle::TablespaceEntry> TablespaceHandle::GetTablespaceEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  std::shared_ptr<storage::SqlTable> sql_tbl = pg_tablespace_->GetSqlTable();  
  uint32_t temp_name = 0;
  if (name == "pg_global") temp_name = 20001;
  if (name == "pg_default") temp_name = 20002;
  // TODO(yangjun): we can cache this
  std::vector<col_oid_t> cols;
  for (const auto &c : sql_tbl->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = sql_tbl->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  // Find the row using sequential scan
  auto tuple_iter = sql_tbl->begin();
  for (; tuple_iter != sql_tbl->end(); tuple_iter++) {
    sql_tbl->Select(txn, *tuple_iter, read);
    // TODO(yangjuns): we don't support strings at the moment
    if ((*reinterpret_cast<uint32_t *>(read->AccessForceNotNull(row_pair.second[cols[1]]))) == temp_name) {
      tablespace_oid_t oid(*reinterpret_cast<tablespace_oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]])));
      return std::make_shared<TablespaceEntry>(pg_tablespace_, oid, read, row_pair.second);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

}  // namespace terrier::catalog
