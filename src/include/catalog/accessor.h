#pragma once
#include <memory>
#include <string>
#include "catalog/catalog.h"

namespace terrier::catalog {

/**
 * Wrapper around catalog restricted to a specific database.
 * TODO(Amadou): Replace this by the real accessor
 */
class CatalogAccessor {
 public:
  /**
   * Constructor
   */
  CatalogAccessor(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid, namespace_oid_t ns_oid)
      : txn_(txn), catalog_(catalog), db_oid_(db_oid), ns_oid_(ns_oid) {}

  /**
   * Wrapper around Catalog::CreateUserTable
   */
  table_oid_t CreateUserTable(const std::string &table_name, const Schema &schema) {
    return catalog_->CreateUserTable(txn_, db_oid_, ns_oid_, table_name, schema);
  }

  /**
   * Wrapper around Catalog::GetUserTable
   */
  SqlTableHelper *GetUserTable(table_oid_t table_oid) {
    return catalog_->GetUserTable(txn_, db_oid_, ns_oid_, table_oid);
  }

  /**
   * Wrapper around Catalog::GetUserTable
   */
  SqlTableHelper *GetUserTable(const std::string &table_name) {
    return catalog_->GetUserTable(txn_, db_oid_, ns_oid_, table_name);
  }

  /**
   * Wrapper around Catalog::GetNextOid
   */
  uint32_t GetNextOid() { return catalog_->GetNextOid(); }

  /**
   * Wrapper around Catalog::DeleteUserTable
   */
  void DeleteUserTable(table_oid_t table_oid) { catalog_->DeleteUserTable(txn_, db_oid_, ns_oid_, table_oid); }

  /**
   * Wrapper around Catalog::DeleteUserTable
   */
  void DeleteUserTable(const std::string &table_name) { catalog_->DeleteUserTable(txn_, db_oid_, ns_oid_, table_name); }

  /**
   * Wrapper around Catalog::CreateIndex
   */
  index_oid_t CreateIndex(storage::index::ConstraintType constraint_type, const storage::index::IndexKeySchema &schema,
                          const std::string &index_name) {
    return catalog_->CreateIndex(txn_, constraint_type, schema, index_name);
  }

  /**
   * Wrapper around Catalog::GetCatalogIndex
   */
  std::shared_ptr<CatalogIndex> GetCatalogIndex(index_oid_t index_oid) { return catalog_->GetCatalogIndex(index_oid); }

  /**
   * Wrapper around Catalog::GetCatalogIndexOid
   */
  index_oid_t GetCatalogIndexOid(const std::string &name) { return catalog_->GetCatalogIndexOid(name); }

 private:
  transaction::TransactionContext *txn_;
  Catalog *catalog_;
  db_oid_t db_oid_;
  namespace_oid_t ns_oid_;
};

}  // namespace terrier::catalog
