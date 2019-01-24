#include "catalog/namespace_handle.h"
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

std::shared_ptr<NamespaceHandle::NamespaceEntry> NamespaceHandle::GetNamespaceEntry(
    transaction::TransactionContext *txn, namespace_oid_t oid) {
  // TODO(yangjun): we can cache this
  CATALOG_LOG_TRACE("inside namepsace handle ... ");
  std::vector<col_oid_t> cols;
  std::cout << pg_namespace_ << std::endl;
  for (const auto &c : pg_namespace_->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  CATALOG_LOG_TRACE("before initializer...");
  auto row_pair = pg_namespace_->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  CATALOG_LOG_TRACE("Before interate ..");
  // Find the row using sequential scan
  auto tuple_iter = pg_namespace_->begin();
  for (; tuple_iter != pg_namespace_->end(); tuple_iter++) {
    pg_namespace_->Select(txn, *tuple_iter, read);
    if ((*reinterpret_cast<namespace_oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid) {
      return std::make_shared<NamespaceEntry>(oid, read, row_pair.second, pg_namespace_);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

std::shared_ptr<NamespaceHandle::NamespaceEntry> NamespaceHandle::GetNamespaceEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  uint32_t temp_name = 0;
  if (name == "pg_catalog") temp_name = 30001;
  // TODO(yangjun): we can cache this
  std::vector<col_oid_t> cols;
  for (const auto &c : pg_namespace_->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = pg_namespace_->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  // Find the row using sequential scan
  auto tuple_iter = pg_namespace_->begin();
  for (; tuple_iter != pg_namespace_->end(); tuple_iter++) {
    pg_namespace_->Select(txn, *tuple_iter, read);
    // TODO(yangjuns): we don't support strings at the moment
    if ((*reinterpret_cast<uint32_t *>(read->AccessForceNotNull(row_pair.second[cols[1]]))) == temp_name) {
      namespace_oid_t oid(*reinterpret_cast<namespace_oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]])));
      return std::make_shared<NamespaceEntry>(oid, read, row_pair.second, pg_namespace_);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

TableHandle NamespaceHandle::GetTableHandle(const std::string &nsp_name) {
  CATALOG_LOG_TRACE("Getting the table handle ...");
  std::string pg_class("pg_class");
  std::string pg_namespace("pg_namespace");
  std::string pg_tablespace("pg_tablespace");
  return TableHandle(nsp_name, catalog_->GetDatabaseCatalog(db_oid_, pg_class),
                     catalog_->GetDatabaseCatalog(db_oid_, pg_namespace),
                     catalog_->GetDatabaseCatalog(db_oid_, pg_tablespace));
}

}  // namespace terrier::catalog
