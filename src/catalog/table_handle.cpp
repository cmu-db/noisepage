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
  // TODO(yangjuns): if the table is not under the namespace then we should not provide the table
  return std::make_shared<TableEntry>(name, txn, pg_class_, pg_namespace_, pg_tablespace_);
}

}  // namespace terrier::catalog
