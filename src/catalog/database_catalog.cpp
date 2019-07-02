#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/schema.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"


namespace terrier::catalog {

// namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name);

// bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns);

// namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name);

// table_oid_t DatabaseCatalog::CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
//                           const Schema &schema);

// bool DatabaseCatalog::DeleteTable(transaction::TransactionContext *txn, table_oid_t table);

// table_oid_t DatabaseCatalog::GetTableOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

// bool DatabaseCatalog::RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

// bool DatabaseCatalog::UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

// const Schema &DatabaseCatalog::GetSchema(transaction::TransactionContext *txn, table_oid_t table);

// std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(transaction::TransactionContext *txn, table_oid_t);

// std::vector<index_oid_t> DatabaseCatalog::GetIndexes(transaction::TransactionContext *txn, table_oid_t);

// index_oid_t DatabaseCatalog::CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
//                                          table_oid_t table, IndexSchema *schema);

// bool DatabaseCatalog::DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

// index_oid_t DatabaseCatalog::GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

// const IndexSchema &DatabaseCatalog::GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

void DatabaseCatalog::TearDown(transaction::TransactionContext *txn) {
  std::vector<parser::AbstractExpression *> expressions;
  std::vector<Schema *> table_schemas;
  std::vector<storage::SqlTable *> tables;
  std::vector<IndexSchema *> index_schemas;
  std::vector<storage::index::Index *> indexes;

  std::vector<col_oid_t> col_oids;

  // pg_class (schemas & objects) [this is the largest projection]
  col_oids.emplace_back(RELKIND_COL_OID);
  col_oids.emplace_back(REL_SCHEMA_COL_OID);
  col_oids.emplace_back(REL_PTR_COL_OID);
  [pci, pm] = classes_->InitializerForProjectedColumns(col_oids, 100);

  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  // Fetch pointers to the start each in the projected columns
  auto classes = reinterpret_cast<postgres::ClassKind *>pc->ColumnStart(pm[RELKIND_COL_OID]);
  auto schemas = reinterpret_cast<void **>pc->ColumnStart(pm[REL_SCHEMA_COL_OID]);
  auto objects = reinterpret_cast<void **>pc->ColumnStart(pm[REL_PTR_COL_OID]);

  // Scan the table
  auto table_iter = classes_->begin();
  while (table_iter != classes_->end()) {
    classes_->Scan(txn, table_iter, pc);

    for (int i = 0; i < pc->NumTuples()) {
      switch(classes[i]) {
        case postgres::ClassKind::REGULAR_TABLE:
          table_schemas.emplace_back(reinterpret_cast<Schema *>schemas[i]);
          tables.emplace_back(reinterpret_cast<storage::SqlTable *>objects[i]);
          break;
        case postgres::ClassKind::INDEX:
          index_schemas.emplace_back(reinterpret_cast<IndexSchema *>schemas[i]);
          indexes.emplace_back(reinterpret_cast<storage::index::Index *>objects[i]);
          break;
        default:
          throw std::runtime_error("Unimplemented destructor needed");
      }
    }
  }

  // pg_attribute (expressions)
  col_oids.clear();
  col_oids.emplace_back(ADBIN_COL_OID);
  [pci, pm] = columns_->InitializerForProjectedColumns(col_oids, 100);
  pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>pc->ColumnStart(0);

  table_iter = columns_->begin();
  while (table_iter != columns_->end()) {
    columns_->Scan(txn, table_iter, pc);

    for (int i = 0; i < pc->NumTuples()) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // pg_constraint (expressions)
  col_oids.clear();
  col_oids.emplace_back(CONBIN_COL_OID);
  [pci, pm] = constraints->InitializerForProjectedColumns(col_oids, 100);
  pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>pc->ColumnStart(0);

  table_iter = constraints->begin();
  while (table_iter != constraints->end()) {
    constraints->Scan(txn, table_iter, pc);

    for (int i = 0; i < pc->NumTuples()) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // No new transactions can see these object but there may be deferred index
  // and other operation.  Therefore, we need to defer the deallocation on delete
  txn->RegisterCommitAction([=, tables{std::move(tables)}, indexes{std::move(indexes)},
                             table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schema)},
                             expressions{std::move(expressions)}] {
    txn->GetTransactionManager()->DeferAction([=, tables{std::move(tables)}, indexes{std::move(indexes)},
                             table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schema)},
                             expressions{std::move(expressions)}] {
      for (auto table : tables)
        delete table;

      for (auto index : indexes)
        delete index;

      for (auto schema : table_schemas)
        delete schema;

      for (auto schema : index_schemas)
        delete schema;

      for (auto expr : expressions)
        delete expr;
    });
  });
}
} // namespace terrier::catalog
