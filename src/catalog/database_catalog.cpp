#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/schema.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "type/type_id.h"

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

type_oid_t DatabaseCatalog::GetTypeOidForType(type::TypeId type) { return type_oid_t(static_cast<uint8_t>(type)); }

void DatabaseCatalog::InsertType(transaction::TransactionContext *txn, type::TypeId internal_type,
                                 const std::string &name, namespace_oid_t namespace_oid, int16_t len, bool by_val,
                                 postgres::Type type_category) {
  std::vector<col_oid_t> table_col_oids;
  table_col_oids.emplace_back(TYPOID_COL_OID);
  table_col_oids.emplace_back(TYPNAME_COL_OID);
  table_col_oids.emplace_back(TYPNAMESPACE_COL_OID);
  table_col_oids.emplace_back(TYPLEN_COL_OID);
  table_col_oids.emplace_back(TYPBYVAL_COL_OID);
  table_col_oids.emplace_back(TYPTYPE_COL_OID);
  auto initializer_pair = types_->InitializerForProjectedRow(table_col_oids);
  auto initializer = initializer_pair.first;
  auto col_map = initializer_pair.second;

  // Stage the write into the table
  auto redo_record = txn->StageWrite(db_oid_, TYPE_TABLE_OID, initializer);
  auto *delta = redo_record->Delta();

  // Populate oid
  auto offset = col_map[TYPOID_COL_OID];
  auto type_oid = GetTypeOidForType(internal_type);
  memcpy(delta->AccessForceNotNull(offset), &type_oid, sizeof(type_oid_t));

  // Populate type name
  offset = col_map[TYPNAME_COL_OID];
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, name.size(), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<byte *>(name.data()), name.size());
  }
  *(reinterpret_cast<storage::VarlenEntry *>(delta->AccessForceNotNull(offset))) = name_varlen;

  // Populate namespace
  offset = col_map[TYPNAMESPACE_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &namespace_oid, sizeof(namespace_oid_t));

  // Populate len
  offset = col_map[TYPLEN_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &len, sizeof(int16_t) /* SMALLINT */);

  // Populate byval
  offset = col_map[TYPBYVAL_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &by_val, sizeof(bool));

  // Populate type
  offset = col_map[TYPTYPE_COL_OID];
  // TODO(Gus): make sure this cast works
  uint8_t type = reinterpret_cast<uint8_t>(type_category);
  memcpy(delta->AccessForceNotNull(offset), &type, sizeof(uint8_t) /* TINYINT */);

  // Insert into table
  auto tuple_slot = types_->Insert(txn, redo_record);

  // Allocate buffer of largest size needed
  uint32_t buffer_size = 0;
  buffer_size = std::max(buffer_size, types_oid_index_->GetProjectedRowInitializer().ProjectedRowSize());
  buffer_size = std::max(buffer_size, types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());
  buffer_size = std::max(buffer_size, types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize());
  byte *buffer = common::AllocationUtil::AllocateAligned(buffer_size);

  // Insert into oid index
  auto oid_index_delta = types_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto oid_index_offset = types_oid_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(oid_index_delta->AccessForceNotNull(oid_index_offset), &type_oid, sizeof(type_oid_t));
  auto result UNUSED_ATTRIBUTE = types_oid_index_->InsertUnique(txn, *oid_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type oid index should always succeed");

  // Insert into (namespace_oid, name) index
  buffer = common::AllocationUtil::AllocateAligned(types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());
  auto name_index_delta = types_name_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  // Populate namespace
  auto name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(name_index_delta->AccessForceNotNull(name_index_offset), &namespace_oid, sizeof(namespace_oid_t));
  // Populate type name
  name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(2));
  *(reinterpret_cast<storage::VarlenEntry *>(name_index_delta->AccessForceNotNull(name_index_offset))) = name_varlen;
  result = types_name_index_->InsertUnique(txn, *name_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type name index should always succeed");

  // Insert into (non-unique) namespace oid index
  buffer =
      common::AllocationUtil::AllocateAligned(types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize());
  auto namespace_index_delta = types_namespace_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto namespace_index_offset = types_namespace_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(namespace_index_delta->AccessForceNotNull(namespace_index_offset), &namespace_oid, sizeof(namespace_oid_t));
  result = types_namespace_index_->Insert(txn, *name_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type namespace index should always succeed");

  // Clean up buffer
  delete[] buffer;
}

void DatabaseCatalog::BootstrapTypes(transaction::TransactionContext *txn) {
  InsertType(txn, type::TypeId::INVALID, "INVALID", NAMESPACE_CATALOG_NAMESPACE_OID, 1, true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::BOOLEAN, "BOOLEAN", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(bool), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TINYINT, "TINYINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int8_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::SMALLINT, "SMALLINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int16_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::INTEGER, "INTEGER", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int32_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::BIGINT, "BIGINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int64_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::DECIMAL, "DECIMAL", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(double), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TIMESTAMP, "TIMESTAMP", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::timestamp_t),
             true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::DATE, "DATE", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::date_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARCHAR, "VARCHAR", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false, postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARBINARY, "VARBINARY", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             postgres::Type::BASE);
}

}  // namespace terrier::catalog

} // namespace terrier::catalog
