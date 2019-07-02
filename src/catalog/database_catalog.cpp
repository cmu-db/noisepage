#pragma once

#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"


namespace terrier::catalog {

namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name);

bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns);

namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name);

table_oid_t DatabaseCatalog::CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          const Schema &schema);

bool DatabaseCatalog::DeleteTable(transaction::TransactionContext *txn, table_oid_t table);

table_oid_t DatabaseCatalog::GetTableOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

bool DatabaseCatalog::RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

bool DatabaseCatalog::UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

const Schema &DatabaseCatalog::GetSchema(transaction::TransactionContext *txn, table_oid_t table);

std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(transaction::TransactionContext *txn, table_oid_t);

std::vector<index_oid_t> DatabaseCatalog::GetIndexes(transaction::TransactionContext *txn, table_oid_t);

index_oid_t DatabaseCatalog::CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          table_oid_t table, IndexSchema *schema);

bool DatabaseCatalog::DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

index_oid_t DatabaseCatalog::GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

const IndexSchema &DatabaseCatalog::GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

} // namespace terrier::catalog
