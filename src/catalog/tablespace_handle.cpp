#include "catalog/tablespace_handle.h"
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
namespace terrier::catalog {

const std::vector<SchemaCol> TablespaceCatalogTable::schema_cols_ = {{0, true, "oid", type::TypeId::INTEGER},
                                                                     {1, true, "spcname", type::TypeId::VARCHAR},
                                                                     {2, false, "spcowner", type::TypeId::INTEGER},
                                                                     {3, false, "spcacl", type::TypeId::VARCHAR},
                                                                     {4, false, "spcoptions", type::TypeId::VARCHAR}};

std::shared_ptr<TablespaceCatalogEntry> TablespaceCatalogTable::GetTablespaceEntry(transaction::TransactionContext *txn,
                                                                                   tablespace_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_tablespace_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<TablespaceCatalogEntry>(oid, pg_tablespace_, std::move(ret_row));
}

std::shared_ptr<TablespaceCatalogEntry> TablespaceCatalogTable::GetTablespaceEntry(transaction::TransactionContext *txn,
                                                                                   const std::string &name) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(name));
  ret_row = pg_tablespace_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  tablespace_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<TablespaceCatalogEntry>(oid, pg_tablespace_, std::move(ret_row));
}

void TablespaceCatalogTable::AddEntry(transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::TransientValue> row;

  row.emplace_back(type::TransientValueFactory::GetInteger(catalog_->GetNextOid()));
  row.emplace_back(type::TransientValueFactory::GetVarChar(name));
  catalog_->SetUnusedColumns(&row, TablespaceCatalogTable::schema_cols_);
  pg_tablespace_->InsertRow(txn, row);
}

SqlTableHelper *TablespaceCatalogTable::Create(Catalog *catalog, db_oid_t db_oid, const std::string &name) {
  catalog::SqlTableHelper *storage_table;

  // get an oid
  table_oid_t storage_table_oid(catalog->GetNextOid());

  // uninitialized storage
  storage_table = new catalog::SqlTableHelper(storage_table_oid);

  // columns we use
  for (auto col : TablespaceCatalogTable::schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // now actually create, with the provided schema
  storage_table->Create();
  catalog->AddToMap(db_oid, CatalogTableType::TABLESPACE, storage_table);
  return storage_table;
}

}  // namespace terrier::catalog
