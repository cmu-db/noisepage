#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "catalog/schema.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
namespace terrier::catalog {

std::shared_ptr<TableHandle::TableEntry> TableHandle::GetTableEntry(transaction::TransactionContext *txn,
                                                                    const std::string &name) {
  CATALOG_LOG_TRACE("Getting table entry with name {}", name);
  uint32_t table_name = 0;
  if (name == "pg_database") table_name = 10001;
  if (name == "pg_tablespace") table_name = 10002;
  if (name == "pg_namespace") table_name = 10003;
  if (name == "pg_class") table_name = 10004;
  return std::make_shared<TableEntry>(table_name, txn, pg_class_, pg_namespace_, pg_tablespace_);
}

}  // namespace terrier::catalog
