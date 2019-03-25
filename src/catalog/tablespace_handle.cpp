#include "catalog/tablespace_handle.h"
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

std::shared_ptr<TablespaceHandle::TablespaceEntry> TablespaceHandle::GetTablespaceEntry(
    transaction::TransactionContext *txn, tablespace_oid_t oid) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  ret_row = pg_tablespace_->FindRow(txn, search_vec);
  return std::make_shared<TablespaceEntry>(oid, ret_row);
}

std::shared_ptr<TablespaceHandle::TablespaceEntry> TablespaceHandle::GetTablespaceEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetVarcharValue(name.c_str()));
  ret_row = pg_tablespace_->FindRow(txn, search_vec);
  tablespace_oid_t oid(ret_row[0].GetIntValue());
  return std::make_shared<TablespaceEntry>(oid, ret_row);
}

void TablespaceHandle::AddEntry(transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::Value> row;

  row.emplace_back(type::ValueFactory::GetIntegerValue(catalog_->GetNextOid()));
  row.emplace_back(type::ValueFactory::GetVarcharValue(name.c_str()));
  catalog_->SetUnusedColumns(&row, TablespaceHandle::unused_schema_cols_);
  pg_tablespace_->InsertRow(txn, row);
}

std::shared_ptr<catalog::SqlTableRW> TablespaceHandle::Create(Catalog *catalog, db_oid_t db_oid,
                                                              const std::string &name) {
  std::shared_ptr<catalog::SqlTableRW> storage_table;

  // get an oid
  table_oid_t storage_table_oid(catalog->GetNextOid());

  // uninitialized storage
  storage_table = std::make_shared<catalog::SqlTableRW>(storage_table_oid);

  // columns we use
  for (auto col : TablespaceHandle::schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // columns we don't use
  for (auto col : TablespaceHandle::unused_schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // now actually create, with the provided schema
  storage_table->Create();
  catalog->AddToMaps(db_oid, storage_table_oid, name, storage_table);
  return storage_table;
}

const std::vector<SchemaCol> TablespaceHandle::schema_cols_ = {{0, "oid", type::TypeId::INTEGER},
                                                               {1, "spcname", type::TypeId::VARCHAR}};

const std::vector<SchemaCol> TablespaceHandle::unused_schema_cols_ = {{2, "spcowner", type::TypeId::INTEGER},
                                                                      {3, "spcacl", type::TypeId::VARCHAR},
                                                                      {4, "spcoptions", type::TypeId::VARCHAR}};

}  // namespace terrier::catalog
