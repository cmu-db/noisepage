#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/class_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

// Find entry with (row) oid and return it
std::shared_ptr<ClassHandle::ClassEntry> ClassHandle::GetClassEntry(transaction::TransactionContext *txn,
                                                                    col_oid_t oid) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  ret_row = pg_class_rw_->FindRow(txn, search_vec);
  return std::make_shared<ClassEntry>(oid, ret_row);
}

void ClassHandle::AddEntry(transaction::TransactionContext *txn, const int64_t tbl_ptr, const int32_t entry_oid,
                           const std::string &name, const int32_t ns_oid, const int32_t ts_oid) {
  std::vector<type::Value> row;

  row.emplace_back(type::ValueFactory::GetBigIntValue(tbl_ptr));
  row.emplace_back(type::ValueFactory::GetIntegerValue(entry_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue(name.c_str()));
  row.emplace_back(type::ValueFactory::GetIntegerValue(ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(ts_oid));

  catalog_->SetUnusedColumns(&row, ClassHandle::unused_schema_cols_);
  pg_class_rw_->InsertRow(txn, row);
}

std::shared_ptr<catalog::SqlTableRW> ClassHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                         db_oid_t db_oid, const std::string &name) {
  std::shared_ptr<catalog::SqlTableRW> pg_class;

  // get an oid
  table_oid_t pg_class_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_class = std::make_shared<catalog::SqlTableRW>(pg_class_oid);

  // columns we use
  for (auto col : ClassHandle::schema_cols_) {
    pg_class->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // columns we don't use
  for (auto col : ClassHandle::unused_schema_cols_) {
    pg_class->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // now actually create, with the provided schema
  pg_class->Create();
  catalog->AddToMaps(db_oid, pg_class_oid, name, pg_class);
  // catalog->AddColumnsToPGAttribute(txn, db_oid, pg_class->GetSqlTable());
  return pg_class;
}

// Postgres has additional columns interspersed within these.
const std::vector<SchemaCols> ClassHandle::schema_cols_ = {{0, "__ptr", type::TypeId::BIGINT},
                                                           {1, "oid", type::TypeId::INTEGER},
                                                           {2, "relname", type::TypeId::VARCHAR},
                                                           {3, "relnamespace", type::TypeId::INTEGER},
                                                           {4, "reltablespace", type::TypeId::INTEGER}};

// TODO(pakhtar): there are quite a number of unused columns...
// Review and define, because some of them we'll probably use.

const std::vector<SchemaCols> ClassHandle::unused_schema_cols_ = {};

}  // namespace terrier::catalog
