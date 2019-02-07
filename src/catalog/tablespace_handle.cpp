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
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  ret_row = pg_tablespace_->FindRow(txn, search_vec);
  return std::make_shared<TablespaceEntry>(oid, ret_row);
}

std::shared_ptr<TablespaceHandle::TablespaceEntry> TablespaceHandle::GetTablespaceEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetNullValue());
  search_vec.push_back(type::ValueFactory::GetStringValue(name.c_str()));
  ret_row = pg_tablespace_->FindRow(txn, search_vec);
  tablespace_oid_t oid(ret_row[0].GetIntValue());
  return std::make_shared<TablespaceEntry>(oid, ret_row);
}

}  // namespace terrier::catalog
