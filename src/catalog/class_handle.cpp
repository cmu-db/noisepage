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

// Postgres has additional columns interspersed within these.
const std::vector<SchemaCol> ClassCatalogTable::schema_cols_ = {{0, true, "__ptr", type::TypeId::BIGINT},
                                                                {1, true, "oid", type::TypeId::INTEGER},
                                                                {2, true, "relname", type::TypeId::VARCHAR},
                                                                {3, true, "relnamespace", type::TypeId::INTEGER},
                                                                {4, true, "reltablespace", type::TypeId::INTEGER}};

// TODO(pakhtar): there are quite a number of unused columns...
// Review and define, because some of them we'll probably use.

// Find entry with (row) oid and return it
std::shared_ptr<ClassCatalogEntry> ClassCatalogTable::GetClassEntry(transaction::TransactionContext *txn,
                                                                    col_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_class_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<ClassCatalogEntry>(oid, pg_class_rw_, std::move(ret_row));
}

std::shared_ptr<ClassCatalogEntry> ClassCatalogTable::GetClassEntry(transaction::TransactionContext *txn,
                                                                    const char *name) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(name));
  ret_row = pg_class_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  col_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[1]));
  return std::make_shared<ClassCatalogEntry>(oid, pg_class_rw_, std::move(ret_row));
}

std::shared_ptr<ClassCatalogEntry> ClassCatalogTable::GetClassEntry(transaction::TransactionContext *txn,
                                                                    namespace_oid_t ns_oid, const char *name) {
  std::vector<type::TransientValue> search_vec, ret_row;
  // ptr
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  // oid
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(name));
  search_vec.push_back(type::TransientValueFactory::GetInteger(!ns_oid));
  ret_row = pg_class_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  col_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[1]));
  return std::make_shared<ClassCatalogEntry>(oid, pg_class_rw_, std::move(ret_row));
}

void ClassCatalogTable::AddEntry(transaction::TransactionContext *txn, const int64_t tbl_ptr, const int32_t entry_oid,
                                 const std::string &name, const int32_t ns_oid, const int32_t ts_oid) {
  std::vector<type::TransientValue> row;

  row.emplace_back(type::TransientValueFactory::GetBigInt(tbl_ptr));
  row.emplace_back(type::TransientValueFactory::GetInteger(entry_oid));
  row.emplace_back(type::TransientValueFactory::GetVarChar(name));
  row.emplace_back(type::TransientValueFactory::GetInteger(ns_oid));
  row.emplace_back(type::TransientValueFactory::GetInteger(ts_oid));

  catalog_->SetUnusedColumns(&row, ClassCatalogTable::schema_cols_);
  pg_class_rw_->InsertRow(txn, row);
}

bool ClassCatalogTable::DeleteEntry(transaction::TransactionContext *txn,
                                    const std::shared_ptr<ClassCatalogEntry> &entry) {
  std::vector<type::TransientValue> search_vec;

  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  // get the oid of this row
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(entry->GetIntegerColumn("oid")));

  // lookup and get back the projected column. Recover the tuple_slot
  auto proj_col_p = pg_class_rw_->FindRowProjCol(txn, search_vec);
  auto tuple_slot_p = proj_col_p->TupleSlots();
  // delete
  bool status = pg_class_rw_->GetSqlTable()->Delete(txn, *tuple_slot_p);
  delete[] reinterpret_cast<byte *>(proj_col_p);
  return status;
}

bool ClassCatalogTable::DeleteEntry(transaction::TransactionContext *txn, namespace_oid_t ns_oid, col_oid_t col_oid) {
  // find the entry
  auto class_entry = GetClassEntry(txn, col_oid);
  if (class_entry == nullptr) {
    return false;
  }
  // verify namespace_oid matches
  uint32_t found_ns_oid = class_entry->GetIntegerColumn("relnamespace");
  if (found_ns_oid != !ns_oid) {
    CATALOG_LOG_ERROR("Error deleting class entry, found ns oid {} != entry ns oid {}", found_ns_oid, !ns_oid);
    TERRIER_ASSERT(found_ns_oid == !ns_oid, "non-matching namespace oid");
  }
  DeleteEntry(txn, class_entry);

  return true;
}

SqlTableHelper *ClassCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                          const std::string &name) {
  catalog::SqlTableHelper *pg_class;

  // get an oid
  table_oid_t pg_class_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_class = new catalog::SqlTableHelper(pg_class_oid);

  for (auto col : ClassCatalogTable::schema_cols_) {
    pg_class->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // now actually create, with the provided schema
  pg_class->Create();
  catalog->AddToMap(db_oid, CatalogTableType::CLASS, pg_class);
  return pg_class;
}

}  // namespace terrier::catalog
