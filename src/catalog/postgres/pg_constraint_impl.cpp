#include "catalog/postgres/pg_constraint_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "storage/sql_table.h"

namespace noisepage::catalog::postgres {

PgConstraintImpl::PgConstraintImpl(db_oid_t db_oid) : db_oid_(db_oid) {}

void PgConstraintImpl::BootstrapPRIs() {}

void PgConstraintImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                                 common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgConstraint::CONSTRAINT_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                      "pg_constraint", Builder::GetConstraintTableSchema(), constraints_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgConstraint::CONSTRAINT_TABLE_OID,
                      PgConstraint::CONSTRAINT_OID_INDEX_OID, "pg_constraint_oid_index",
                      Builder::GetConstraintOidIndexSchema(db_oid_), constraints_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgConstraint::CONSTRAINT_TABLE_OID,
                      PgConstraint::CONSTRAINT_NAME_INDEX_OID, "pg_constraint_name_index",
                      Builder::GetConstraintNameIndexSchema(db_oid_), constraints_name_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgConstraint::CONSTRAINT_TABLE_OID,
                      PgConstraint::CONSTRAINT_NAMESPACE_INDEX_OID, "pg_constraint_namespace_index",
                      Builder::GetConstraintNamespaceIndexSchema(db_oid_), constraints_namespace_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgConstraint::CONSTRAINT_TABLE_OID,
                      PgConstraint::CONSTRAINT_TABLE_INDEX_OID, "pg_constraint_table_index",
                      Builder::GetConstraintTableIndexSchema(db_oid_), constraints_table_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgConstraint::CONSTRAINT_TABLE_OID,
                      PgConstraint::CONSTRAINT_INDEX_INDEX_OID, "pg_constraint_index_index",
                      Builder::GetConstraintIndexIndexSchema(db_oid_), constraints_index_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgConstraint::CONSTRAINT_TABLE_OID,
                      PgConstraint::CONSTRAINT_FOREIGNTABLE_INDEX_OID, "pg_constraint_foreigntable_index",
                      Builder::GetConstraintForeignTableIndexSchema(db_oid_), constraints_foreigntable_index_);
}

std::function<void(void)> PgConstraintImpl::GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn) {
  std::vector<parser::AbstractExpression *> expressions;

  const std::vector<col_oid_t> pg_constraint_oids{PgConstraint::CONBIN.oid_};
  auto pci = constraints_->InitializerForProjectedColumns(pg_constraint_oids, DatabaseCatalog::TEARDOWN_MAX_TUPLES);
  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>(pc->ColumnStart(0));

  auto table_iter = constraints_->begin();
  while (table_iter != constraints_->end()) {
    constraints_->Scan(txn, &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) {
      expressions.emplace_back(exprs[i]);
    }
  }

  delete[] buffer;
  return [expressions{std::move(expressions)}]() {
    for (const auto expression : expressions) {
      delete expression;
    }
  };
}

}  // namespace noisepage::catalog::postgres
