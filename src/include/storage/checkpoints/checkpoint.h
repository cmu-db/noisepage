#pragma once

#include <string>
#include <mutex>
#include <catalog/catalog.h>
#include <common/managed_pointer.h>
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

class Checkpoint {
 public:
  Checkpoint(const common::ManagedPointer<catalog::Catalog> catalog,
             const common::ManagedPointer<BlockStore> store,
             const common::ManagedPointer<transaction::TransactionContext> txn)
  :catalog_(catalog), block_store_(store), txn_(txn){
    // Initialize catalog_table_schemas_ map
    catalog_table_schemas_[catalog::postgres::CLASS_TABLE_OID] = catalog::postgres::Builder::GetClassTableSchema();
    catalog_table_schemas_[catalog::postgres::NAMESPACE_TABLE_OID] =
        catalog::postgres::Builder::GetNamespaceTableSchema();
    catalog_table_schemas_[catalog::postgres::COLUMN_TABLE_OID] = catalog::postgres::Builder::GetColumnTableSchema();
    catalog_table_schemas_[catalog::postgres::CONSTRAINT_TABLE_OID] =
        catalog::postgres::Builder::GetConstraintTableSchema();
    catalog_table_schemas_[catalog::postgres::INDEX_TABLE_OID] = catalog::postgres::Builder::GetIndexTableSchema();
    catalog_table_schemas_[catalog::postgres::TYPE_TABLE_OID] = catalog::postgres::Builder::GetTypeTableSchema();
  }


  bool TakeCheckpoint(const std::string &path);

  static std::string GenFileName(catalog::db_oid_t db_oid, catalog::table_oid_t tb_oid){
    return std::to_string((uint32_t)db_oid) + "-" + std::to_string((uint32_t)tb_oid);
  }


 private:
  // Catalog to fetch table pointers
  const common::ManagedPointer<catalog::Catalog> catalog_;
  const common::ManagedPointer<BlockStore> block_store_;
  const common::ManagedPointer<transaction::TransactionContext> txn_;
  std::unordered_map<catalog::table_oid_t, catalog::Schema> catalog_table_schemas_;
  std::vector<catalog::table_oid_t> queue; // for multithreading
  std::mutex queue_latch;

  void WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor, catalog::db_oid_t db_oid);


}; // class checkpoint



} // namespace terrier::storage