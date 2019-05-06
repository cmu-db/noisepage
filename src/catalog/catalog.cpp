#include "catalog/catalog.h"

#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "common/exception.h"
#include "loggers/catalog_logger.h"

namespace terrier::catalog {
table_oid_t Catalog::CreateTable(transaction::TransactionContext *txn,
                                 db_oid_t db_oid, const std::string &table_name,
                                 const Schema &schema) {
  table_oid_t table_oid(GetNextOid());

  // creates the storage table and adds to pg_class
  auto tbl_rw =
      std::make_shared<catalog::SqlTableRW>(schema, table_oid, block_store_);
  if (tbl_rw == nullptr) {
    CATALOG_EXCEPTION("Table Should not be null");
  }

  // add to maps
  AddToMaps(db_oid, table_oid, table_name, tbl_rw);
  CATALOG_LOG_TRACE("Created Table {}", table_name);

  return tbl_rw->Oid();
}

void Catalog::DeleteTable(transaction::TransactionContext *txn, db_oid_t db_oid,
                          table_oid_t table_oid) {
  // name_map_[db_oid].erase(table_oid);
  map_[db_oid].erase(table_oid);
}

std::shared_ptr<catalog::SqlTableRW> Catalog::GetCatalogTable(
    db_oid_t db_oid, table_oid_t table_oid) {
  return map_[db_oid][table_oid];
}

std::shared_ptr<catalog::SqlTableRW> Catalog::GetCatalogTable(
    db_oid_t db_oid, const std::string &table_name) {
  return map_[db_oid][name_map_[db_oid][table_name]];
}

index_oid_t Catalog::CreateIndex(transaction::TransactionContext *txn,
                                 storage::index::ConstraintType constraint_type,
                                 const storage::index::IndexKeySchema &schema,
                                 const std::string &name) {
  index_oid_t index_oid(GetNextOid());
  auto catalog_index =
      std::make_shared<CatalogIndex>(txn, index_oid, constraint_type, schema);
  index_map_[index_oid] = catalog_index;
  index_names_[name] = index_oid;
  return index_oid;
}

std::shared_ptr<CatalogIndex> Catalog::GetCatalogIndex(index_oid_t index_oid) {
  return index_map_.at(index_oid);
}

index_oid_t Catalog::GetCatalogIndexOid(const std::string &name) {
  return index_names_.at(name);
}

}  // namespace terrier::catalog