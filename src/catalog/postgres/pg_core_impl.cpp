#include "catalog/postgres/pg_core_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "common/json.h"
#include "storage/garbage_collector.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/deferred_action_manager.h"

namespace noisepage::catalog::postgres {

PgCoreImpl::PgCoreImpl(const db_oid_t db_oid) : db_oid_(db_oid) {}

// --------------------------------------------------------------------------------------------------------------------
// BootstrapPRIs
// --------------------------------------------------------------------------------------------------------------------

void PgCoreImpl::BootstrapPRIsPgNamespace() {
  const std::vector<col_oid_t> pg_namespace_all_oids{PgNamespace::PG_NAMESPACE_ALL_COL_OIDS.cbegin(),
                                                     PgNamespace::PG_NAMESPACE_ALL_COL_OIDS.cend()};
  pg_namespace_all_cols_pri_ = namespaces_->InitializerForProjectedRow(pg_namespace_all_oids);
  pg_namespace_all_cols_prm_ = namespaces_->ProjectionMapForOids(pg_namespace_all_oids);

  const std::vector<col_oid_t> delete_namespace_oids{PgNamespace::NSPNAME_COL_OID};
  delete_namespace_pri_ = namespaces_->InitializerForProjectedRow(delete_namespace_oids);

  const std::vector<col_oid_t> get_namespace_oids{PgNamespace::NSPOID_COL_OID};
  get_namespace_pri_ = namespaces_->InitializerForProjectedRow(get_namespace_oids);
}

void PgCoreImpl::BootstrapPRIsPgClass() {
  const std::vector<col_oid_t> pg_class_all_oids{PgClass::PG_CLASS_ALL_COL_OIDS.cbegin(),
                                                 PgClass::PG_CLASS_ALL_COL_OIDS.cend()};
  pg_class_all_cols_pri_ = classes_->InitializerForProjectedRow(pg_class_all_oids);
  pg_class_all_cols_prm_ = classes_->ProjectionMapForOids(pg_class_all_oids);

  const std::vector<col_oid_t> get_class_oid_kind_oids{PgClass::RELOID_COL_OID, PgClass::RELKIND_COL_OID};
  get_class_oid_kind_pri_ = classes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> set_class_pointer_oids{PgClass::REL_PTR_COL_OID};
  set_class_pointer_pri_ = classes_->InitializerForProjectedRow(set_class_pointer_oids);

  const std::vector<col_oid_t> set_class_schema_oids{PgClass::REL_SCHEMA_COL_OID};
  set_class_schema_pri_ = classes_->InitializerForProjectedRow(set_class_schema_oids);

  const std::vector<col_oid_t> get_class_pointer_kind_oids{PgClass::REL_PTR_COL_OID, PgClass::RELKIND_COL_OID};
  get_class_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_schema_pointer_kind_oids{PgClass::REL_SCHEMA_COL_OID,
                                                                  PgClass::RELKIND_COL_OID};
  get_class_schema_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_schema_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_object_and_schema_oids{PgClass::REL_PTR_COL_OID, PgClass::REL_SCHEMA_COL_OID};
  get_class_object_and_schema_pri_ = classes_->InitializerForProjectedRow(get_class_object_and_schema_oids);
  get_class_object_and_schema_prm_ = classes_->ProjectionMapForOids(get_class_object_and_schema_oids);
}

void PgCoreImpl::BootstrapPRIsPgIndex() {
  const std::vector<col_oid_t> pg_index_all_oids{PgIndex::PG_INDEX_ALL_COL_OIDS.cbegin(),
                                                 PgIndex::PG_INDEX_ALL_COL_OIDS.cend()};
  pg_index_all_cols_pri_ = indexes_->InitializerForProjectedRow(pg_index_all_oids);
  pg_index_all_cols_prm_ = indexes_->ProjectionMapForOids(pg_index_all_oids);

  const std::vector<col_oid_t> get_indexes_oids{PgIndex::INDOID_COL_OID};
  get_indexes_pri_ = indexes_->InitializerForProjectedRow(get_indexes_oids);

  const std::vector<col_oid_t> delete_index_oids{PgIndex::INDOID_COL_OID, PgIndex::INDRELID_COL_OID};
  delete_index_pri_ = indexes_->InitializerForProjectedRow(delete_index_oids);
  delete_index_prm_ = indexes_->ProjectionMapForOids(delete_index_oids);
}

void PgCoreImpl::BootstrapPRIsPgAttribute() {
  const std::vector<col_oid_t> pg_attribute_all_oids{PgAttribute::PG_ATTRIBUTE_ALL_COL_OIDS.cbegin(),
                                                     PgAttribute::PG_ATTRIBUTE_ALL_COL_OIDS.end()};
  pg_attribute_all_cols_pri_ = columns_->InitializerForProjectedRow(pg_attribute_all_oids);
  pg_attribute_all_cols_prm_ = columns_->ProjectionMapForOids(pg_attribute_all_oids);

  const std::vector<col_oid_t> get_columns_oids{PgAttribute::ATTNUM_COL_OID,     PgAttribute::ATTNAME_COL_OID,
                                                PgAttribute::ATTTYPID_COL_OID,   PgAttribute::ATTLEN_COL_OID,
                                                PgAttribute::ATTNOTNULL_COL_OID, PgAttribute::ADSRC_COL_OID};
  get_columns_pri_ = columns_->InitializerForProjectedRow(get_columns_oids);
  get_columns_prm_ = columns_->ProjectionMapForOids(get_columns_oids);

  const std::vector<col_oid_t> delete_columns_oids{PgAttribute::ATTNUM_COL_OID, PgAttribute::ATTNAME_COL_OID};
  delete_columns_pri_ = columns_->InitializerForProjectedRow(delete_columns_oids);
  delete_columns_prm_ = columns_->ProjectionMapForOids(delete_columns_oids);
}

void PgCoreImpl::BootstrapPRIs() {
  BootstrapPRIsPgNamespace();
  BootstrapPRIsPgClass();
  BootstrapPRIsPgIndex();
  BootstrapPRIsPgAttribute();
}

// --------------------------------------------------------------------------------------------------------------------
// Bootstrap
// --------------------------------------------------------------------------------------------------------------------

void PgCoreImpl::BootstrapPgNamespace(common::ManagedPointer<transaction::TransactionContext> txn,
                                      common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgNamespace::NAMESPACE_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                                 "pg_namespace", Builder::GetNamespaceTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgNamespace::NAMESPACE_TABLE_OID, namespaces_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgNamespace::NAMESPACE_TABLE_OID,
                                 PgNamespace::NAMESPACE_OID_INDEX_OID, "pg_namespace_oid_index",
                                 Builder::GetNamespaceOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgNamespace::NAMESPACE_OID_INDEX_OID, namespaces_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgNamespace::NAMESPACE_TABLE_OID,
                                 PgNamespace::NAMESPACE_NAME_INDEX_OID, "pg_namespace_name_index",
                                 Builder::GetNamespaceNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgNamespace::NAMESPACE_NAME_INDEX_OID, namespaces_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgCoreImpl::BootstrapPgClass(common::ManagedPointer<transaction::TransactionContext> txn,
                                  common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgClass::CLASS_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                                 "pg_class", Builder::GetClassTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgClass::CLASS_TABLE_OID, classes_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgClass::CLASS_TABLE_OID,
                                 PgClass::CLASS_OID_INDEX_OID, "pg_class_oid_index",
                                 Builder::GetClassOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgClass::CLASS_OID_INDEX_OID, classes_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgClass::CLASS_TABLE_OID,
                                 PgClass::CLASS_NAME_INDEX_OID, "pg_class_name_index",
                                 Builder::GetClassNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgClass::CLASS_NAME_INDEX_OID, classes_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgClass::CLASS_TABLE_OID,
                                 PgClass::CLASS_NAMESPACE_INDEX_OID, "pg_class_namespace_index",
                                 Builder::GetClassNamespaceIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgClass::CLASS_NAMESPACE_INDEX_OID, classes_namespace_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgCoreImpl::BootstrapPgIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                                  common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgIndex::INDEX_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                                 "pg_index", Builder::GetIndexTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgIndex::INDEX_TABLE_OID, indexes_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgIndex::INDEX_TABLE_OID,
                                 PgIndex::INDEX_OID_INDEX_OID, "pg_index_oid_index",
                                 Builder::GetIndexOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgIndex::INDEX_OID_INDEX_OID, indexes_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgIndex::INDEX_TABLE_OID,
                                 PgIndex::INDEX_TABLE_INDEX_OID, "pg_index_table_index",
                                 Builder::GetIndexTableIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgIndex::INDEX_TABLE_INDEX_OID, indexes_table_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgCoreImpl::BootstrapPgAttribute(common::ManagedPointer<transaction::TransactionContext> txn,
                                      common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgAttribute::COLUMN_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                                 "pg_attribute", Builder::GetColumnTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgAttribute::COLUMN_TABLE_OID, columns_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgAttribute::COLUMN_TABLE_OID,
                                 PgAttribute::COLUMN_OID_INDEX_OID, "pg_attribute_oid_index",
                                 Builder::GetColumnOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgAttribute::COLUMN_OID_INDEX_OID, columns_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgAttribute::COLUMN_TABLE_OID,
                                 PgAttribute::COLUMN_NAME_INDEX_OID, "pg_attribute_name_index",
                                 Builder::GetColumnNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgAttribute::COLUMN_NAME_INDEX_OID, columns_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void PgCoreImpl::Bootstrap(const common::ManagedPointer<transaction::TransactionContext> txn,
                           common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = CreateNamespace(txn, "pg_catalog", PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = CreateNamespace(txn, "public", PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapPgNamespace(txn, dbc);
  BootstrapPgClass(txn, dbc);
  BootstrapPgIndex(txn, dbc);
  BootstrapPgAttribute(txn, dbc);
}

// --------------------------------------------------------------------------------------------------------------------
// Other functions
// --------------------------------------------------------------------------------------------------------------------

std::function<void(void)> PgCoreImpl::GetTearDownFn(
    common::ManagedPointer<transaction::TransactionContext> txn,
    const common::ManagedPointer<storage::GarbageCollector> garbage_collector) {
  std::vector<Schema *> table_schemas;
  std::vector<storage::SqlTable *> tables;
  std::vector<IndexSchema *> index_schemas;
  std::vector<storage::index::Index *> indexes;

  // pg_class (schemas & objects)
  const std::vector<col_oid_t> pg_class_oids{PgClass::RELKIND_COL_OID, PgClass::REL_SCHEMA_COL_OID,
                                             PgClass::REL_PTR_COL_OID};

  auto pci = classes_->InitializerForProjectedColumns(pg_class_oids, DatabaseCatalog::TEARDOWN_MAX_TUPLES);
  auto pm = classes_->ProjectionMapForOids(pg_class_oids);

  uint64_t buffer_len = pci.ProjectedColumnsSize();
  byte *buffer = common::AllocationUtil::AllocateAligned(buffer_len);
  auto pc = pci.Initialize(buffer);

  // Fetch pointers to the start each in the projected columns
  auto classes = reinterpret_cast<PgClass::RelKind *>(pc->ColumnStart(pm[PgClass::RELKIND_COL_OID]));
  auto schemas = reinterpret_cast<void **>(pc->ColumnStart(pm[PgClass::REL_SCHEMA_COL_OID]));
  auto objects = reinterpret_cast<void **>(pc->ColumnStart(pm[PgClass::REL_PTR_COL_OID]));

  // Scan the table
  auto table_iter = classes_->begin();
  while (table_iter != classes_->end()) {
    classes_->Scan(txn, &table_iter, pc);
    for (uint i = 0; i < pc->NumTuples(); i++) {
      NOISEPAGE_ASSERT(objects[i] != nullptr, "Pointer to objects in pg_class should not be nullptr");
      NOISEPAGE_ASSERT(schemas[i] != nullptr, "Pointer to schemas in pg_class should not be nullptr");
      switch (classes[i]) {
        case PgClass::RelKind::REGULAR_TABLE:
          table_schemas.emplace_back(reinterpret_cast<Schema *>(schemas[i]));
          tables.emplace_back(reinterpret_cast<storage::SqlTable *>(objects[i]));
          break;
        case PgClass::RelKind::INDEX:
          index_schemas.emplace_back(reinterpret_cast<IndexSchema *>(schemas[i]));
          indexes.emplace_back(reinterpret_cast<storage::index::Index *>(objects[i]));
          break;
        default:
          throw std::runtime_error("Unimplemented destructor needed");
      }
    }
  }

  delete[] buffer;
  return [garbage_collector{garbage_collector}, tables{std::move(tables)}, indexes{std::move(indexes)},
          table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schemas)}]() {
    for (auto table : tables) delete table;
    for (auto index : indexes) {
      if (index->Type() == storage::index::IndexType::BWTREE) {
        garbage_collector->UnregisterIndexForGC(common::ManagedPointer(index));
      }
      delete index;
    }
    for (auto table_schema : table_schemas) delete table_schema;
    for (auto index_schema : index_schemas) delete index_schema;
  };
}

// --------------------------------------------------------------------------------------------------------------------
// pg_namespace
// --------------------------------------------------------------------------------------------------------------------

bool PgCoreImpl::CreateNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const std::string &name, const namespace_oid_t ns_oid) {
  // Step 1: Insert into table
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);
  // Get & Fill Redo Record
  auto *const redo = txn->StageWrite(db_oid_, PgNamespace::NAMESPACE_TABLE_OID, pg_namespace_all_cols_pri_);
  // Write the attributes in the Redo Record
  *(reinterpret_cast<namespace_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_namespace_all_cols_prm_[PgNamespace::NSPOID_COL_OID]))) = ns_oid;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_namespace_all_cols_prm_[PgNamespace::NSPNAME_COL_OID]))) = name_varlen;
  // Finally, insert into the table to get the tuple slot
  const auto tuple_slot = namespaces_->Insert(txn, redo);

  // Step 2: Insert into name index
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto *index_pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;

  if (!namespaces_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  // Step 3: Insert into oid index
  auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  // Reuse buffer since an u32 column is smaller than a varlen column
  index_pr = oid_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(0))) = ns_oid;
  const bool UNUSED_ATTRIBUTE result = namespaces_oid_index_->InsertUnique(txn, *index_pr, tuple_slot);
  NOISEPAGE_ASSERT(result, "Assigned namespace OID failed to be unique.");

  // Finish
  delete[] buffer;
  return true;
}

bool PgCoreImpl::DeleteNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const common::ManagedPointer<DatabaseCatalog> dbc, const namespace_oid_t ns_oid) {
  // Step 1: Read the oid index
  // Buffer is large enough for all prs because it's meant to hold 1 VarlenEntry
  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_namespace_pri_.ProjectedRowSize());
  const auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  auto *pr = oid_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0))) = ns_oid;
  // Scan index
  std::vector<storage::TupleSlot> index_results;
  namespaces_oid_index_->ScanKey(*txn, *pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense. "
      "Was a DROP plan node reused twice? IF EXISTS should be handled in the Binder, rather than pushing logic here.");
  const auto tuple_slot = index_results[0];

  // Step 2: Select from the table to get the name
  pr = delete_namespace_pri_.InitializeRow(buffer);
  auto UNUSED_ATTRIBUTE result = namespaces_->Select(txn, tuple_slot, pr);
  NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
  const auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));

  // Step 3: Delete from table
  txn->StageDelete(db_oid_, PgNamespace::NAMESPACE_TABLE_OID, tuple_slot);
  if (!namespaces_->Delete(txn, tuple_slot)) {
    // Someone else has a write-lock. Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  // Step 4: Cascading deletes
  // Get the objects in this namespace
  auto ns_objects = GetNamespaceClassOids(txn, ns_oid);
  for (const auto &object : ns_objects) {
    // Delete all of the tables. This should get most of the indexes
    if (object.second == PgClass::RelKind::REGULAR_TABLE) {
      result = DeleteTable(txn, dbc, static_cast<table_oid_t>(object.first));
      if (!result) {
        // Someone else has a write-lock. Free the buffer and return false to indicate failure
        delete[] buffer;
        return false;
      }
    }
  }

  // Get the objects in the namespace again, just in case there were any indexes that don't belong to a table in this
  // namespace. We could do all of this cascading cleanup with a more complex single index scan, but we're taking
  // advantage of existing PRIs and indexes and expecting that deleting a namespace isn't that common of an operation,
  // so we can be slightly less efficient than optimal.
  ns_objects = GetNamespaceClassOids(txn, ns_oid);
  for (const auto &object : ns_objects) {
    // Delete all of the straggler indexes that may have been built on tables in other namespaces. We shouldn't get any
    // double-deletions because indexes on tables will already be invisible to us (logically deleted already).
    if (object.second == PgClass::RelKind::INDEX) {
      result = DeleteIndex(txn, dbc, static_cast<index_oid_t>(object.first));
      if (!result) {
        // Someone else has a write-lock. Free the buffer and return false to indicate failure
        delete[] buffer;
        return false;
      }
    }
  }
  NOISEPAGE_ASSERT(GetNamespaceClassOids(txn, ns_oid).empty(), "Failed to drop all of the namespace objects.");

  // Step 5: Delete from oid index
  pr = oid_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0))) = ns_oid;
  namespaces_oid_index_->Delete(txn, *pr, tuple_slot);

  // Step 6: Delete from name index
  const auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;
  namespaces_name_index_->Delete(txn, *pr, tuple_slot);

  // Finish
  delete[] buffer;
  return true;
}

namespace_oid_t PgCoreImpl::GetNamespaceOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const std::string &name) {
  // Step 1: Read the name index
  const auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  // Buffer is large enough for all prs because it's meant to hold 1 VarlenEntry
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto *pr = name_pri.InitializeRow(buffer);
  // Scan the name index
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;
  std::vector<storage::TupleSlot> index_results;
  namespaces_name_index_->ScanKey(*txn, *pr, &index_results);

  // Clean up the varlen's buffer in the case it wasn't inlined.
  if (!name_varlen.IsInlined()) {
    delete[] name_varlen.Content();
  }

  if (index_results.empty()) {
    // namespace not found in the index, so namespace doesn't exist. Free the buffer and return false to indicate
    // failure
    delete[] buffer;
    return INVALID_NAMESPACE_OID;
  }
  NOISEPAGE_ASSERT(index_results.size() == 1, "Namespace name not unique in index");
  const auto tuple_slot = index_results[0];

  // Step 2: Scan the table to get the oid
  pr = get_namespace_pri_.InitializeRow(buffer);

  const auto UNUSED_ATTRIBUTE result = namespaces_->Select(txn, tuple_slot, pr);
  NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
  const auto ns_oid = *reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0));

  // Finish
  delete[] buffer;
  return ns_oid;
}

template <typename ClassOid, typename Ptr>
bool PgCoreImpl::SetClassPointer(const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid oid,
                                 const Ptr *const pointer, const col_oid_t class_col) {
  static_assert(std::is_same_v<ClassOid, table_oid_t> || std::is_same_v<ClassOid, index_oid_t>, "Invalid ClassOid.");
  static_assert((std::is_same_v<ClassOid, table_oid_t> && std::is_same_v<Ptr, storage::SqlTable>) ||
                    (std::is_same_v<ClassOid, table_oid_t> && std::is_same_v<Ptr, Schema>) ||
                    (std::is_same_v<ClassOid, index_oid_t> && std::is_same_v<Ptr, storage::index::Index>) ||
                    (std::is_same_v<ClassOid, index_oid_t> && std::is_same_v<Ptr, IndexSchema>),
                "Invalid Ptr.");
  NOISEPAGE_ASSERT(
      (std::is_same<ClassOid, table_oid_t>::value &&
       (std::is_same<Ptr, storage::SqlTable>::value || std::is_same<Ptr, catalog::Schema>::value)) ||
          (std::is_same<ClassOid, index_oid_t>::value &&
           (std::is_same<Ptr, storage::index::Index>::value || std::is_same<Ptr, catalog::IndexSchema>::value)),
      "OID type must correspond to the same object type (Table or index)");
  NOISEPAGE_ASSERT(pointer != nullptr, "Why are you inserting nullptr here? That seems wrong.");
  const auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Do not need to store the projection map because it is only a single column
  auto pr_init = classes_->InitializerForProjectedRow({class_col});
  NOISEPAGE_ASSERT(pr_init.ProjectedRowSize() >= oid_pri.ProjectedRowSize(), "Buffer must allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(0))) = oid;
  std::vector<storage::TupleSlot> index_results;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no reasonable "
      "code path for this to be called on an oid that isn't present.");

  auto &initializer = (class_col == PgClass::REL_PTR_COL_OID) ? set_class_pointer_pri_ : set_class_schema_pri_;
  auto *update_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, initializer);
  update_redo->SetTupleSlot(index_results[0]);
  auto *update_pr = update_redo->Delta();
  auto *const class_ptr_ptr = update_pr->AccessForceNotNull(0);
  *(reinterpret_cast<const Ptr **>(class_ptr_ptr)) = pointer;

  // Finish
  delete[] buffer;
  return classes_->Update(txn, update_redo);
}

bool PgCoreImpl::CreateTableEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const table_oid_t table_oid, const namespace_oid_t ns_oid, const std::string &name,
                                  const Schema &schema) {
  auto *const insert_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, pg_class_all_cols_pri_);
  auto *const insert_pr = insert_redo->Delta();

  // Write the ns_oid into the PR
  const auto ns_offset = pg_class_all_cols_prm_[PgClass::RELNAMESPACE_COL_OID];
  auto *const ns_ptr = insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<namespace_oid_t *>(ns_ptr)) = ns_oid;

  // Write the table_oid into the PR
  const auto table_oid_offset = pg_class_all_cols_prm_[PgClass::RELOID_COL_OID];
  auto *const table_oid_ptr = insert_pr->AccessForceNotNull(table_oid_offset);
  *(reinterpret_cast<table_oid_t *>(table_oid_ptr)) = table_oid;

  auto next_col_oid = col_oid_t(static_cast<uint32_t>(schema.GetColumns().size() + 1));

  // Write the next_col_oid into the PR
  const auto next_col_oid_offset = pg_class_all_cols_prm_[PgClass::REL_NEXTCOLOID_COL_OID];
  auto *const next_col_oid_ptr = insert_pr->AccessForceNotNull(next_col_oid_offset);
  *(reinterpret_cast<col_oid_t *>(next_col_oid_ptr)) = next_col_oid;

  // Write the schema_ptr as nullptr into the PR (need to update once we've recreated the columns)
  const auto schema_ptr_offset = pg_class_all_cols_prm_[PgClass::REL_SCHEMA_COL_OID];
  auto *const schema_ptr_ptr = insert_pr->AccessForceNotNull(schema_ptr_offset);
  *(reinterpret_cast<Schema **>(schema_ptr_ptr)) = nullptr;

  // Set table_ptr to NULL because it gets set by execution layer after instantiation
  const auto table_ptr_offset = pg_class_all_cols_prm_[PgClass::REL_PTR_COL_OID];
  insert_pr->SetNull(table_ptr_offset);

  // Write the kind into the PR
  const auto kind_offset = pg_class_all_cols_prm_[PgClass::RELKIND_COL_OID];
  auto *const kind_ptr = insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<char *>(kind_ptr)) = static_cast<char>(PgClass::RelKind::REGULAR_TABLE);

  // Create the necessary varlen for storage operations
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Write the name into the PR
  const auto name_offset = pg_class_all_cols_prm_[PgClass::RELNAME_COL_OID];
  auto *const name_ptr = insert_pr->AccessForceNotNull(name_offset);
  *(reinterpret_cast<storage::VarlenEntry *>(name_ptr)) = name_varlen;

  // Insert into pg_class table
  const auto tuple_slot = classes_->Insert(txn, insert_redo);

  // Get PR initializers and allocate a buffer from the largest one
  const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
  auto *const index_buffer = common::AllocationUtil::AllocateAligned(name_index_init.ProjectedRowSize());

  // Insert into oid_index
  auto *index_pr = oid_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<table_oid_t *>(index_pr->AccessForceNotNull(0))) = table_oid;
  if (!classes_oid_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was an oid conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into name_index
  index_pr = name_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  if (!classes_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into namespace_index
  index_pr = ns_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(0))) = ns_oid;
  const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insertion into non-unique namespace index failed.");

  delete[] index_buffer;

  // Write the col oids into a new Schema object
  col_oid_t curr_col_oid(1);
  for (auto &col : schema.GetColumns()) {
    auto success = CreateColumn(txn, table_oid, curr_col_oid++, col);
    if (!success) return false;
  }

  std::vector<Schema::Column> cols = GetColumns<Schema::Column, table_oid_t, col_oid_t>(txn, table_oid);
  auto *new_schema = new Schema(cols);
  txn->RegisterAbortAction([=]() { delete new_schema; });

  auto *const update_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, set_class_schema_pri_);
  auto *const update_pr = update_redo->Delta();

  update_redo->SetTupleSlot(tuple_slot);
  *reinterpret_cast<Schema **>(update_pr->AccessForceNotNull(0)) = new_schema;
  auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
  NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail");

  return true;
}

bool PgCoreImpl::DeleteTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                             const common::ManagedPointer<DatabaseCatalog> dbc, const table_oid_t table) {
  // We should respect foreign key relations and attempt to delete the table's columns first
  auto result = DeleteColumns<Schema::Column, table_oid_t>(txn, table);
  if (!result) return false;

  const auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  NOISEPAGE_ASSERT(pg_class_all_cols_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated for largest ProjectedRow size");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_class_all_cols_pri_.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<table_oid_t *>(key_pr->AccessForceNotNull(0))) = table;
  std::vector<storage::TupleSlot> index_results;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense. "
      "Was a DROP plan node reused twice? IF EXISTS should be handled in the Binder, rather than pushing logic here.");

  // Select the tuple out of the table before deletion. We need the attributes to do index deletions later
  auto *const table_pr = pg_class_all_cols_pri_.InitializeRow(buffer);
  result = classes_->Select(txn, index_results[0], table_pr);
  NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");

  // Delete from pg_classes table
  txn->StageDelete(db_oid_, PgClass::CLASS_TABLE_OID, index_results[0]);
  result = classes_->Delete(txn, index_results[0]);
  if (!result) {
    // write-write conflict. Someone beat us to this operation.
    delete[] buffer;
    return false;
  }

  dbc->DeleteIndexes(txn, table);

  // Get the attributes we need for indexes
  const table_oid_t table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::RELOID_COL_OID])));
  NOISEPAGE_ASSERT(table == table_oid,
                   "table oid from pg_classes did not match what was found by the index scan from the argument.");
  const namespace_oid_t ns_oid = *(reinterpret_cast<const namespace_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::RELNAMESPACE_COL_OID])));
  const storage::VarlenEntry name_varlen = *(reinterpret_cast<const storage::VarlenEntry *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::RELNAME_COL_OID])));

  // Get the attributes we need for delete
  auto *const schema_ptr = *(reinterpret_cast<const Schema *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::REL_SCHEMA_COL_OID])));
  auto *const table_ptr = *(reinterpret_cast<storage::SqlTable *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::REL_PTR_COL_OID])));

  const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();

  // Delete from oid_index
  auto *index_pr = oid_index_init.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *const>(index_pr->AccessForceNotNull(0))) = table_oid;
  classes_oid_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from name_index
  index_pr = name_index_init.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *const>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  classes_name_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from namespace_index
  index_pr = ns_index_init.InitializeRow(buffer);
  *(reinterpret_cast<namespace_oid_t *const>(index_pr->AccessForceNotNull(0))) = ns_oid;
  classes_namespace_index_->Delete(txn, *index_pr, index_results[0]);

  // Everything succeeded from an MVCC standpoint, register deferred action for the GC with txn manager. See base
  // function comment.
  txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() {
      deferred_action_manager->RegisterDeferredAction([=]() {
        // Defer an action upon commit to delete the table. Delete table will need a double deferral because there could
        // be transactions not yet unlinked by the GC that depend on the table
        delete schema_ptr;
        delete table_ptr;
      });
    });
  });

  delete[] buffer;
  return true;
}

bool PgCoreImpl::CreateIndexEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const namespace_oid_t ns_oid, const table_oid_t table_oid,
                                  const index_oid_t index_oid, const std::string &name, const IndexSchema &schema) {
  // First, insert into pg_class
  auto *const class_insert_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, pg_class_all_cols_pri_);
  auto *const class_insert_pr = class_insert_redo->Delta();

  // Write the index_oid into the PR
  auto index_oid_offset = pg_class_all_cols_prm_[PgClass::RELOID_COL_OID];
  auto *index_oid_ptr = class_insert_pr->AccessForceNotNull(index_oid_offset);
  *(reinterpret_cast<index_oid_t *>(index_oid_ptr)) = index_oid;

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Write the name into the PR
  const auto name_offset = pg_class_all_cols_prm_[PgClass::RELNAME_COL_OID];
  auto *const name_ptr = class_insert_pr->AccessForceNotNull(name_offset);
  *(reinterpret_cast<storage::VarlenEntry *>(name_ptr)) = name_varlen;

  // Write the ns_oid into the PR
  const auto ns_offset = pg_class_all_cols_prm_[PgClass::RELNAMESPACE_COL_OID];
  auto *const ns_ptr = class_insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<namespace_oid_t *>(ns_ptr)) = ns_oid;

  // Write the kind into the PR
  const auto kind_offset = pg_class_all_cols_prm_[PgClass::RELKIND_COL_OID];
  auto *const kind_ptr = class_insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<PgClass::RelKind *>(kind_ptr)) = PgClass::RelKind::INDEX;

  // Write the index_schema_ptr into the PR
  const auto index_schema_ptr_offset = pg_class_all_cols_prm_[PgClass::REL_SCHEMA_COL_OID];
  auto *const index_schema_ptr_ptr = class_insert_pr->AccessForceNotNull(index_schema_ptr_offset);
  *(reinterpret_cast<IndexSchema **>(index_schema_ptr_ptr)) = nullptr;

  // Set next_col_oid to NULL because indexes don't need col_oid
  const auto next_col_oid_offset = pg_class_all_cols_prm_[PgClass::REL_NEXTCOLOID_COL_OID];
  class_insert_pr->SetNull(next_col_oid_offset);

  // Set index_ptr to NULL because it gets set by execution layer after instantiation
  const auto index_ptr_offset = pg_class_all_cols_prm_[PgClass::REL_PTR_COL_OID];
  class_insert_pr->SetNull(index_ptr_offset);

  // Insert into pg_class table
  const auto class_tuple_slot = classes_->Insert(txn, class_insert_redo);

  // Now we insert into indexes on pg_class
  // Get PR initializers allocate a buffer from the largest one
  const auto class_oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto class_name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto class_ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT((class_name_index_init.ProjectedRowSize() >= class_oid_index_init.ProjectedRowSize()) &&
                       (class_name_index_init.ProjectedRowSize() >= class_ns_index_init.ProjectedRowSize()),
                   "Index buffer must be allocated based on the largest PR initializer");
  auto *index_buffer = common::AllocationUtil::AllocateAligned(class_name_index_init.ProjectedRowSize());

  // Insert into oid_index
  auto *index_pr = class_oid_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<index_oid_t *>(index_pr->AccessForceNotNull(0))) = index_oid;
  if (!classes_oid_index_->InsertUnique(txn, *index_pr, class_tuple_slot)) {
    // There was an oid conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into name_index
  index_pr = class_name_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  if (!classes_name_index_->InsertUnique(txn, *index_pr, class_tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into namespace_index
  index_pr = class_ns_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(0))) = ns_oid;
  const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, class_tuple_slot);
  NOISEPAGE_ASSERT(result, "Insertion into non-unique namespace index failed.");

  // Next, insert index metadata into pg_index

  auto *const indexes_insert_redo = txn->StageWrite(db_oid_, PgIndex::INDEX_TABLE_OID, pg_index_all_cols_pri_);
  auto *const indexes_insert_pr = indexes_insert_redo->Delta();

  // Write the index_oid into the PR
  index_oid_offset = pg_index_all_cols_prm_[PgIndex::INDOID_COL_OID];
  index_oid_ptr = indexes_insert_pr->AccessForceNotNull(index_oid_offset);
  *(reinterpret_cast<index_oid_t *>(index_oid_ptr)) = index_oid;

  // Write the table_oid for the table the index is for into the PR
  const auto rel_oid_offset = pg_index_all_cols_prm_[PgIndex::INDRELID_COL_OID];
  auto *const rel_oid_ptr = indexes_insert_pr->AccessForceNotNull(rel_oid_offset);
  *(reinterpret_cast<table_oid_t *>(rel_oid_ptr)) = table_oid;

  // Write boolean values to PR
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[PgIndex::INDISUNIQUE_COL_OID]))) = schema.is_unique_;
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[PgIndex::INDISPRIMARY_COL_OID]))) = schema.is_primary_;
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[PgIndex::INDISEXCLUSION_COL_OID]))) = schema.is_exclusion_;
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[PgIndex::INDIMMEDIATE_COL_OID]))) = schema.is_immediate_;
  // TODO(Matt): these should actually be set later based on runtime information about the index. @yeshengm
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[PgIndex::INDISVALID_COL_OID]))) = true;
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[PgIndex::INDISREADY_COL_OID]))) = true;
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[PgIndex::INDISLIVE_COL_OID]))) = true;
  *(reinterpret_cast<storage::index::IndexType *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[PgIndex::IND_TYPE_COL_OID]))) = schema.type_;

  // Insert into pg_index table
  const auto indexes_tuple_slot = indexes_->Insert(txn, indexes_insert_redo);

  // Now insert into the indexes on pg_index
  // Get PR initializers and allocate a buffer from the largest one
  const auto indexes_oid_index_init = indexes_oid_index_->GetProjectedRowInitializer();
  const auto indexes_table_index_init = indexes_table_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT((class_name_index_init.ProjectedRowSize() >= indexes_oid_index_init.ProjectedRowSize()) &&
                       (class_name_index_init.ProjectedRowSize() > indexes_table_index_init.ProjectedRowSize()),
                   "Index buffer must be allocated based on the largest PR initializer");

  // Insert into indexes_oid_index
  index_pr = indexes_oid_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<index_oid_t *>(index_pr->AccessForceNotNull(0))) = index_oid;
  if (!indexes_oid_index_->InsertUnique(txn, *index_pr, indexes_tuple_slot)) {
    // There was an oid conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into (non-unique) indexes_table_index
  index_pr = indexes_table_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<table_oid_t *>(index_pr->AccessForceNotNull(0))) = table_oid;
  if (!indexes_table_index_->Insert(txn, *index_pr, indexes_tuple_slot)) {
    // There was duplicate value. Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Free the buffer, we are finally done
  delete[] index_buffer;

  // Write the col oids into a new Schema object
  indexkeycol_oid_t curr_col_oid(1);
  for (auto &col : schema.GetColumns()) {
    auto success = CreateColumn(txn, index_oid, curr_col_oid++, col);
    if (!success) return false;
  }

  std::vector<IndexSchema::Column> cols =
      GetColumns<IndexSchema::Column, index_oid_t, indexkeycol_oid_t>(txn, index_oid);
  auto *new_schema =
      new IndexSchema(cols, schema.Type(), schema.Unique(), schema.Primary(), schema.Exclusion(), schema.Immediate());
  txn->RegisterAbortAction([=]() { delete new_schema; });

  auto *const update_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, set_class_schema_pri_);
  auto *const update_pr = update_redo->Delta();

  update_redo->SetTupleSlot(class_tuple_slot);
  *reinterpret_cast<IndexSchema **>(update_pr->AccessForceNotNull(0)) = new_schema;
  auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
  NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail");

  return true;
}

bool PgCoreImpl::DeleteIndex(const common::ManagedPointer<transaction::TransactionContext> txn,
                             common::ManagedPointer<DatabaseCatalog> dbc, index_oid_t index) {
  // We should respect foreign key relations and attempt to delete the index's columns first
  auto result = DeleteColumns<IndexSchema::Column, index_oid_t>(txn, index);
  if (!result) return false;

  // Initialize PRs for pg_class
  const auto class_oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Allocate buffer for largest PR
  NOISEPAGE_ASSERT(pg_class_all_cols_pri_.ProjectedRowSize() >= class_oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated for largest ProjectedRow size");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_class_all_cols_pri_.ProjectedRowSize());
  auto *key_pr = class_oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<index_oid_t *>(key_pr->AccessForceNotNull(0))) = index;
  std::vector<storage::TupleSlot> index_results;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense. "
      "Was a DROP plan node reused twice? IF EXISTS should be handled in the Binder, rather than pushing logic here.");

  // Select the tuple out of the table before deletion. We need the attributes to do index deletions later
  auto *table_pr = pg_class_all_cols_pri_.InitializeRow(buffer);
  result = classes_->Select(txn, index_results[0], table_pr);
  NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");

  // Delete from pg_classes table
  txn->StageDelete(db_oid_, PgClass::CLASS_TABLE_OID, index_results[0]);
  result = classes_->Delete(txn, index_results[0]);
  if (!result) {
    // write-write conflict. Someone beat us to this operation.
    delete[] buffer;
    return false;
  }

  // Get the attributes we need for pg_class indexes
  table_oid_t table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::RELOID_COL_OID])));
  const namespace_oid_t ns_oid = *(reinterpret_cast<const namespace_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::RELNAMESPACE_COL_OID])));
  const storage::VarlenEntry name_varlen = *(reinterpret_cast<const storage::VarlenEntry *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::RELNAME_COL_OID])));

  auto *const schema_ptr = *(reinterpret_cast<const IndexSchema *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::REL_SCHEMA_COL_OID])));
  auto *const index_ptr = *(reinterpret_cast<storage::index::Index *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[PgClass::REL_PTR_COL_OID])));

  const auto class_oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto class_name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto class_ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();

  // Delete from classes_oid_index_
  auto *index_pr = class_oid_index_init.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *const>(index_pr->AccessForceNotNull(0))) = table_oid;
  classes_oid_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from classes_name_index_
  index_pr = class_name_index_init.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *const>(index_pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(index_pr->AccessForceNotNull(1))) = ns_oid;
  classes_name_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from classes_namespace_index_
  index_pr = class_ns_index_init.InitializeRow(buffer);
  *(reinterpret_cast<namespace_oid_t *const>(index_pr->AccessForceNotNull(0))) = ns_oid;
  classes_namespace_index_->Delete(txn, *index_pr, index_results[0]);

  // Now we need to delete from pg_index and its indexes
  // Initialize PRs for pg_index
  const auto index_oid_pr = indexes_oid_index_->GetProjectedRowInitializer();
  const auto index_table_pr = indexes_table_index_->GetProjectedRowInitializer();

  NOISEPAGE_ASSERT((pg_class_all_cols_pri_.ProjectedRowSize() >= delete_index_pri_.ProjectedRowSize()) &&
                       (pg_class_all_cols_pri_.ProjectedRowSize() >= index_oid_pr.ProjectedRowSize()) &&
                       (pg_class_all_cols_pri_.ProjectedRowSize() >= index_table_pr.ProjectedRowSize()),
                   "Buffer must be allocated for largest ProjectedRow size");

  // Find the entry in pg_index using the oid index
  index_results.clear();
  key_pr = index_oid_pr.InitializeRow(buffer);
  *(reinterpret_cast<index_oid_t *>(key_pr->AccessForceNotNull(0))) = index;
  indexes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(index_results.size() == 1,
                   "Incorrect number of results from index scan. Expect 1 because it's a unique index. size() of 0 "
                   "implies an error in Catalog state because scanning pg_class worked, but it doesn't exist in "
                   "pg_index. Something broke.");

  // Select the tuple out of pg_index before deletion. We need the attributes to do index deletions later
  table_pr = delete_index_pri_.InitializeRow(buffer);
  result = indexes_->Select(txn, index_results[0], table_pr);
  NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");

  NOISEPAGE_ASSERT(index == *(reinterpret_cast<const index_oid_t *const>(
                                table_pr->AccessForceNotNull(delete_index_prm_[PgIndex::INDOID_COL_OID]))),
                   "index oid from pg_index did not match what was found by the index scan from the argument.");

  // Delete from pg_index table
  txn->StageDelete(db_oid_, PgIndex::INDEX_TABLE_OID, index_results[0]);
  result = indexes_->Delete(txn, index_results[0]);
  NOISEPAGE_ASSERT(
      result,
      "Delete from pg_index should always succeed as write-write conflicts are detected during delete from pg_class");

  // Get the table oid
  table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(delete_index_prm_[PgIndex::INDRELID_COL_OID])));

  // Delete from indexes_oid_index
  index_pr = index_oid_pr.InitializeRow(buffer);
  *(reinterpret_cast<index_oid_t *const>(index_pr->AccessForceNotNull(0))) = index;
  indexes_oid_index_->Delete(txn, *index_pr, index_results[0]);

  // Delete from indexes_table_index
  index_pr = index_table_pr.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *const>(index_pr->AccessForceNotNull(0))) = table_oid;
  indexes_table_index_->Delete(txn, *index_pr, index_results[0]);

  // Everything succeeded from an MVCC standpoint, so register a deferred action for the GC to delete the index with txn
  // manager. See base function comment.
  txn->RegisterCommitAction(
      [=, garbage_collector{dbc->garbage_collector_}](transaction::DeferredActionManager *deferred_action_manager) {
        if (index_ptr->Type() == storage::index::IndexType::BWTREE) {
          garbage_collector->UnregisterIndexForGC(common::ManagedPointer(index_ptr));
        }
        // Unregistering from GC can happen immediately, but we have to double-defer freeing the actual objects
        deferred_action_manager->RegisterDeferredAction([=]() {
          deferred_action_manager->RegisterDeferredAction([=]() {
            delete schema_ptr;
            delete index_ptr;
          });
        });
      });

  delete[] buffer;
  return true;
}

std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> PgCoreImpl::GetIndexes(
    const common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) {
  // Step 1: Get all index oids on table
  // Initialize PR for index scan
  auto indexes_oid_pri = indexes_table_index_->GetProjectedRowInitializer();

  // Do not need projection map when there is only one column
  NOISEPAGE_ASSERT(get_class_object_and_schema_pri_.ProjectedRowSize() >= indexes_oid_pri.ProjectedRowSize() &&
                       get_class_object_and_schema_pri_.ProjectedRowSize() >= get_indexes_pri_.ProjectedRowSize() &&
                       get_class_object_and_schema_pri_.ProjectedRowSize() >=
                           classes_oid_index_->GetProjectedRowInitializer().ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_object_and_schema_pri_.ProjectedRowSize());

  // Find all entries for the given table using the index
  auto *indexes_key_pr = indexes_oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *>(indexes_key_pr->AccessForceNotNull(0))) = table;
  std::vector<storage::TupleSlot> index_scan_results;
  indexes_table_index_->ScanKey(*txn, *indexes_key_pr, &index_scan_results);

  // If we found no indexes, return an empty list
  if (index_scan_results.empty()) {
    delete[] buffer;
    return {};
  }

  std::vector<index_oid_t> index_oids;
  index_oids.reserve(index_scan_results.size());
  auto *index_select_pr = get_indexes_pri_.InitializeRow(buffer);
  for (auto &slot : index_scan_results) {
    const auto result UNUSED_ATTRIBUTE = indexes_->Select(txn, slot, index_select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
    index_oids.emplace_back(*(reinterpret_cast<index_oid_t *>(index_select_pr->AccessForceNotNull(0))));
  }

  // Step 2: Scan the pg_class oid index for all entries in pg_class
  // We do the index scans and table selects in separate loops to avoid having to initialize the pr each time
  index_scan_results.clear();
  auto *class_key_pr = classes_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  std::vector<storage::TupleSlot> class_tuple_slots;
  class_tuple_slots.reserve(index_oids.size());
  for (const auto &index_oid : index_oids) {
    // Find the entry using the index
    *(reinterpret_cast<uint32_t *>(class_key_pr->AccessForceNotNull(0))) = index_oid.UnderlyingValue();
    classes_oid_index_->ScanKey(*txn, *class_key_pr, &index_scan_results);
    NOISEPAGE_ASSERT(
        index_scan_results.size() == 1,
        "Incorrect number of results from index scan. Expect 1 because it's a unique index. size() of 0 "
        "implies an error in Catalog state because scanning pg_index returned the index oid, but it doesn't "
        "exist in pg_class. Something broke.");
    class_tuple_slots.push_back(index_scan_results[0]);
    index_scan_results.clear();
  }
  NOISEPAGE_ASSERT(class_tuple_slots.size() == index_oids.size(),
                   "We should have found an entry in pg_class for every index oid");

  // Step 3: Select all the objects from the tuple slots retrieved by step 2
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> index_objects;
  index_objects.reserve(class_tuple_slots.size());
  auto *class_select_pr = get_class_object_and_schema_pri_.InitializeRow(buffer);
  for (const auto &slot : class_tuple_slots) {
    bool result UNUSED_ATTRIBUTE = classes_->Select(txn, slot, class_select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

    auto *index = *(reinterpret_cast<storage::index::Index *const *const>(
        class_select_pr->AccessForceNotNull(get_class_object_and_schema_prm_[PgClass::REL_PTR_COL_OID])));
    NOISEPAGE_ASSERT(
        index != nullptr,
        "Catalog conventions say you should not find a nullptr for an object ptr in pg_class. Did you call "
        "SetIndexPointer?");
    auto *schema = *(reinterpret_cast<catalog::IndexSchema *const *const>(
        class_select_pr->AccessForceNotNull(get_class_object_and_schema_prm_[PgClass::REL_SCHEMA_COL_OID])));
    NOISEPAGE_ASSERT(schema != nullptr,
                     "Catalog conventions say you should not find a nullptr for an schema ptr in pg_class");

    index_objects.emplace_back(common::ManagedPointer(index), *schema);
  }
  delete[] buffer;
  return index_objects;
}

std::vector<index_oid_t> PgCoreImpl::GetIndexOids(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                  table_oid_t table) {
  // Initialize PR for index scan
  auto oid_pri = indexes_table_index_->GetProjectedRowInitializer();

  // Do not need projection map when there is only one column
  NOISEPAGE_ASSERT(get_indexes_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_indexes_pri_.ProjectedRowSize());

  // Find all entries for the given table using the index
  auto *key_pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<table_oid_t *>(key_pr->AccessForceNotNull(0))) = table;
  std::vector<storage::TupleSlot> index_scan_results;
  indexes_table_index_->ScanKey(*txn, *key_pr, &index_scan_results);

  // If we found no indexes, return an empty list
  if (index_scan_results.empty()) {
    delete[] buffer;
    return {};
  }

  std::vector<index_oid_t> index_oids;
  index_oids.reserve(index_scan_results.size());
  auto *select_pr = get_indexes_pri_.InitializeRow(buffer);
  for (auto &slot : index_scan_results) {
    const auto result UNUSED_ATTRIBUTE = indexes_->Select(txn, slot, select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
    index_oids.emplace_back(*(reinterpret_cast<index_oid_t *>(select_pr->AccessForceNotNull(0))));
  }

  // Finish
  delete[] buffer;
  return index_oids;
}

std::vector<std::pair<uint32_t, PgClass::RelKind>> PgCoreImpl::GetNamespaceClassOids(
    const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid) {
  std::vector<storage::TupleSlot> index_scan_results;

  // Initialize both PR initializers, allocate buffer using size of largest one so we can reuse buffer
  auto oid_pri = classes_namespace_index_->GetProjectedRowInitializer();
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_oid_kind_pri_.ProjectedRowSize());

  // Find the entry using the index
  auto *key_pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<namespace_oid_t *>(key_pr->AccessForceNotNull(0))) = ns_oid;
  classes_namespace_index_->ScanKey(*txn, *key_pr, &index_scan_results);

  // If we found no objects, return an empty list
  if (index_scan_results.empty()) {
    delete[] buffer;
    return {};
  }

  auto *select_pr = get_class_oid_kind_pri_.InitializeRow(buffer);
  std::vector<std::pair<uint32_t, PgClass::RelKind>> ns_objects;
  ns_objects.reserve(index_scan_results.size());
  for (const auto scan_result : index_scan_results) {
    const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, scan_result, select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
    // oid_t is guaranteed to be larger in size than ClassKind, so we know the column offsets without the PR map
    ns_objects.emplace_back(*(reinterpret_cast<const uint32_t *const>(select_pr->AccessWithNullCheck(0))),
                            *(reinterpret_cast<const PgClass::RelKind *const>(select_pr->AccessForceNotNull(1))));
  }

  // Finish
  delete[] buffer;
  return ns_objects;
}

std::pair<void *, PgClass::RelKind> PgCoreImpl::GetClassPtrKind(
    const common::ManagedPointer<transaction::TransactionContext> txn, uint32_t oid) {
  std::vector<storage::TupleSlot> index_results;

  // Initialize both PR initializers, allocate buffer using size of largest one so we can reuse buffer
  auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Since these two attributes are fixed size and one is larger than the other we know PTR will be 0 and KIND will be 1
  NOISEPAGE_ASSERT(get_class_pointer_kind_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_pointer_kind_pri_.ProjectedRowSize());

  // Find the entry using the index
  auto *key_pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(0))) = oid;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  auto *select_pr = get_class_pointer_kind_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], select_pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  auto *const ptr_ptr = (reinterpret_cast<void *const *const>(select_pr->AccessWithNullCheck(0)));
  auto kind = *(reinterpret_cast<const PgClass::RelKind *const>(select_pr->AccessForceNotNull(1)));

  void *ptr;
  if (ptr_ptr == nullptr) {
    ptr = nullptr;
  } else {
    ptr = *ptr_ptr;
  }

  delete[] buffer;
  return {ptr, kind};
}

std::pair<void *, PgClass::RelKind> PgCoreImpl::GetClassSchemaPtrKind(
    const common::ManagedPointer<transaction::TransactionContext> txn, uint32_t oid) {
  std::vector<storage::TupleSlot> index_results;

  // Initialize both PR initializers, allocate buffer using size of largest one so we can reuse buffer
  auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Since these two attributes are fixed size and one is larger than the other we know PTR will be 0 and KIND will be 1
  NOISEPAGE_ASSERT(get_class_schema_pointer_kind_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_schema_pointer_kind_pri_.ProjectedRowSize());

  // Find the entry using the index
  auto *key_pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(0))) = oid;
  classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  auto *select_pr = get_class_schema_pointer_kind_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], select_pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  auto *const ptr = *(reinterpret_cast<void *const *const>(select_pr->AccessForceNotNull(0)));
  auto kind = *(reinterpret_cast<const PgClass::RelKind *const>(select_pr->AccessForceNotNull(1)));

  NOISEPAGE_ASSERT(ptr != nullptr, "Schema pointer shouldn't ever be NULL under current catalog semantics.");

  delete[] buffer;
  return {ptr, kind};
}

std::pair<uint32_t, PgClass::RelKind> PgCoreImpl::GetClassOidKind(
    const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid,
    const std::string &name) {
  const auto name_pri = classes_name_index_->GetProjectedRowInitializer();

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Buffer is large enough to hold all prs
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of attribute
  // sizes
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(1))) = ns_oid;

  std::vector<storage::TupleSlot> index_results;
  classes_name_index_->ScanKey(*txn, *pr, &index_results);
  // Clean up the varlen's buffer in the case it wasn't inlined.
  if (!name_varlen.IsInlined()) {
    delete[] name_varlen.Content();
  }

  if (index_results.empty()) {
    delete[] buffer;
    // If the OID is invalid, we don't care the class kind and return a random one.
    return std::make_pair(catalog::NULL_OID, PgClass::RelKind::REGULAR_TABLE);
  }
  NOISEPAGE_ASSERT(index_results.size() == 1, "name not unique in classes_name_index_");

  NOISEPAGE_ASSERT(get_class_oid_kind_pri_.ProjectedRowSize() <= name_pri.ProjectedRowSize(),
                   "I want to reuse this buffer because I'm lazy and malloc is slow but it needs to be big enough.");
  pr = get_class_oid_kind_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of attribute
  // sizes
  const auto oid = *(reinterpret_cast<const uint32_t *const>(pr->AccessForceNotNull(0)));
  const auto kind = *(reinterpret_cast<const PgClass::RelKind *const>(pr->AccessForceNotNull(1)));

  // Finish
  delete[] buffer;
  return std::make_pair(oid, kind);
}

template <typename Column, typename ClassOid, typename ColOid>
bool PgCoreImpl::CreateColumn(const common::ManagedPointer<transaction::TransactionContext> txn,
                              const ClassOid class_oid, const ColOid col_oid, const Column &col) {
  // Step 1: Insert into the table
  auto *const redo = txn->StageWrite(db_oid_, PgAttribute::COLUMN_TABLE_OID, pg_attribute_all_cols_pri_);
  // Write the attributes in the Redo Record
  auto oid_entry = reinterpret_cast<ColOid *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[PgAttribute::ATTNUM_COL_OID]));
  auto relid_entry = reinterpret_cast<ClassOid *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[PgAttribute::ATTRELID_COL_OID]));
  auto name_entry = reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[PgAttribute::ATTNAME_COL_OID]));
  auto type_entry = reinterpret_cast<uint32_t *>(redo->Delta()->AccessForceNotNull(
      pg_attribute_all_cols_prm_[PgAttribute::ATTTYPID_COL_OID]));  // TypeId is a uint8_t enum class, but in
  // the schema it's INTEGER (32-bit)
  auto len_entry = reinterpret_cast<uint16_t *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[PgAttribute::ATTLEN_COL_OID]));
  auto notnull_entry = reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[PgAttribute::ATTNOTNULL_COL_OID]));
  auto dsrc_entry = reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[PgAttribute::ADSRC_COL_OID]));
  *oid_entry = col_oid;
  *relid_entry = class_oid;
  const auto name_varlen = storage::StorageUtil::CreateVarlen(col.Name());

  *name_entry = name_varlen;
  *type_entry = static_cast<uint32_t>(col.Type());
  // TODO(Amadou): Figure out what really goes here for varlen. Unclear if it's attribute size (16) or varlen length
  *len_entry = (col.Type() == type::TypeId::VARCHAR || col.Type() == type::TypeId::VARBINARY) ? col.MaxVarlenSize()
                                                                                              : col.AttrSize();
  *notnull_entry = !col.Nullable();
  storage::VarlenEntry dsrc_varlen = storage::StorageUtil::CreateVarlen(col.StoredExpression()->ToJson().dump());
  *dsrc_entry = dsrc_varlen;
  // Finally, insert into the table to get the tuple slot
  const auto tupleslot = columns_->Insert(txn, redo);

  // Step 2: Insert into name index
  const auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  // Create a buffer large enough for all columns
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto *pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of attribute
  // sizes
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(1))) = class_oid;

  if (!columns_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    // There was a name conflict and we need to abort.  Free the buffer and return false to indicate failure
    delete[] buffer;

    // Clean up the varlen's buffer in the case it wasn't inlined.
    if (!name_varlen.IsInlined()) {
      delete[] name_varlen.Content();
    }

    return false;
  }

  // Step 3: Insert into oid index
  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();
  pr = oid_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow. These hardcoded indexkeycol_oids come from
  // Builder::GetColumnOidIndexSchema()
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<ColOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = col_oid;

  bool UNUSED_ATTRIBUTE result = columns_oid_index_->InsertUnique(txn, *pr, tupleslot);
  NOISEPAGE_ASSERT(result, "Assigned OIDs failed to be unique.");

  // Finish
  delete[] buffer;
  return true;
}

template <typename Column, typename ClassOid, typename ColOid>
std::vector<Column> PgCoreImpl::GetColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
                                           ClassOid class_oid) {
  // Step 1: Read Index

  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();

  // Buffer is large enough to hold all prs
  byte *const buffer = common::AllocationUtil::AllocateAligned(get_columns_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  // Scan the class index
  NOISEPAGE_ASSERT(get_columns_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be large enough to fit largest PR");
  auto *pr = oid_pri.InitializeRow(buffer);
  auto *pr_high = oid_pri.InitializeRow(key_buffer);

  // Write the attributes in the ProjectedRow
  // Low key (class, INVALID_COLUMN_OID)
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<ColOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = ColOid(0);

  // High key (class + 1, INVALID_COLUMN_OID)
  *(reinterpret_cast<ClassOid *>(pr_high->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = ++class_oid;
  *(reinterpret_cast<ColOid *>(pr_high->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = ColOid(0);
  std::vector<storage::TupleSlot> index_results;
  columns_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, pr_high, 0, &index_results);

  NOISEPAGE_ASSERT(!index_results.empty(),
                   "Incorrect number of results from index scan. empty() implies that function was called with an oid "
                   "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  // Step 2: Scan the table to get the columns
  std::vector<Column> cols;
  pr = get_columns_pri_.InitializeRow(buffer);
  for (const auto &slot : index_results) {
    const auto UNUSED_ATTRIBUTE result = columns_->Select(txn, slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    cols.emplace_back(MakeColumn<Column, ColOid>(pr, get_columns_prm_));
  }

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  // Finish
  delete[] buffer;
  delete[] key_buffer;
  return cols;
}

// TODO(Matt): we need a DeleteColumn()

template <typename Column, typename ClassOid>
bool PgCoreImpl::DeleteColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
                               const ClassOid class_oid) {
  // Step 1: Read Index
  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();
  const auto name_pri = columns_name_index_->GetProjectedRowInitializer();

  // Buffer is large enough to hold all prs
  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_columns_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  // Scan the class index
  auto *pr = oid_pri.InitializeRow(buffer);
  auto *key_pr = oid_pri.InitializeRow(key_buffer);

  // Write the attributes in the ProjectedRow
  // Low key (class, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
  *(reinterpret_cast<ClassOid *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
  *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = 0;

  auto next_oid = ClassOid(class_oid.UnderlyingValue() + 1);
  // High key (class + 1, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
  *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = next_oid;
  *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = 0;
  std::vector<storage::TupleSlot> index_results;
  columns_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, key_pr, 0, &index_results);

  NOISEPAGE_ASSERT(!index_results.empty(),
                   "Incorrect number of results from index scan. empty() implies that function was called with an oid "
                   "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  // Step 2: Scan the table to get the columns
  pr = delete_columns_pri_.InitializeRow(buffer);
  for (const auto &slot : index_results) {
    // 1. Extract attributes from the tuple for the index deletions
    auto UNUSED_ATTRIBUTE result = columns_->Select(txn, slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    const auto *const col_name = reinterpret_cast<const storage::VarlenEntry *const>(
        pr->AccessWithNullCheck(delete_columns_prm_[PgAttribute::ATTNAME_COL_OID]));
    NOISEPAGE_ASSERT(col_name != nullptr, "Name shouldn't be NULL.");
    const auto *const col_oid = reinterpret_cast<const uint32_t *const>(
        pr->AccessWithNullCheck(delete_columns_prm_[PgAttribute::ATTNUM_COL_OID]));
    NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

    // 2. Delete from the table
    txn->StageDelete(db_oid_, PgAttribute::COLUMN_TABLE_OID, slot);
    result = columns_->Delete(txn, slot);
    if (!result) {
      // Failed to delete one of the columns, clean up and return false to indicate failure
      delete[] buffer;
      delete[] key_buffer;
      return false;
    }

    // 4. Delete from oid index
    key_pr = oid_pri.InitializeRow(key_buffer);
    // Write the attributes in the ProjectedRow. These hardcoded indexkeycol_oids come from
    // Builder::GetColumnOidIndexSchema()
    *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(1)]))) = class_oid;
    *(reinterpret_cast<uint32_t *>(key_pr->AccessForceNotNull(oid_prm[indexkeycol_oid_t(2)]))) = *col_oid;
    columns_oid_index_->Delete(txn, *key_pr, slot);

    // 5. Delete from name index
    key_pr = name_pri.InitializeRow(key_buffer);
    // Write the attributes in the ProjectedRow. We know the offsets without the map because of the ordering of
    // attribute sizes
    *(reinterpret_cast<storage::VarlenEntry *>(key_pr->AccessForceNotNull(0))) = *col_name;
    *(reinterpret_cast<ClassOid *>(key_pr->AccessForceNotNull(1))) = class_oid;
    columns_name_index_->Delete(txn, *key_pr, slot);
  }
  delete[] buffer;
  delete[] key_buffer;
  return true;
}

template <typename Column, typename ColOid>
Column PgCoreImpl::MakeColumn(storage::ProjectedRow *const pr, const storage::ProjectionMap &pr_map) {
  const auto col_oid = *reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(pr_map.at(PgAttribute::ATTNUM_COL_OID)));
  const auto col_name =
      reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_map.at(PgAttribute::ATTNAME_COL_OID)));
  const auto col_type = static_cast<type::TypeId>(*reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(
      pr_map.at(PgAttribute::ATTTYPID_COL_OID))));  // TypeId is a uint8_t enum class, but in the schema it's
  // INTEGER (32-bit)
  const auto col_len = *reinterpret_cast<uint16_t *>(pr->AccessForceNotNull(pr_map.at(PgAttribute::ATTLEN_COL_OID)));
  const auto col_null =
      !(*reinterpret_cast<bool *>(pr->AccessForceNotNull(pr_map.at(PgAttribute::ATTNOTNULL_COL_OID))));
  const auto *const col_expr =
      reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_map.at(PgAttribute::ADSRC_COL_OID)));

  // TODO(WAN): Why are we deserializing expressions to make a catalog column? This is potentially busted.
  // Our JSON library is also not the most performant.
  // I believe it is OK that the unique ptr goes out of scope because right now both Column constructors copy the expr.
  auto deserialized = parser::DeserializeExpression(nlohmann::json::parse(col_expr->StringView()));

  auto expr = std::move(deserialized.result_);
  NOISEPAGE_ASSERT(deserialized.non_owned_exprs_.empty(), "Congrats, you get to refactor the catalog API.");

  const std::string name(reinterpret_cast<const char *>(col_name->Content()), col_name->Size());
  Column col = (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY)
                   ? Column(name, col_type, col_len, col_null, *expr)
                   : Column(name, col_type, col_null, *expr);

  col.SetOid(ColOid(col_oid));
  return col;
}

#define DEFINE_SET_CLASS_POINTER(ClassOid, Ptr)                                                                        \
  template bool PgCoreImpl::SetClassPointer<ClassOid, Ptr>(                                                            \
      const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid oid, const Ptr *const pointer, \
      const col_oid_t class_col);
#define DEFINE_CREATE_COLUMN(Column, ClassOid, ColOid)                                             \
  template bool PgCoreImpl::CreateColumn<Column, ClassOid, ColOid>(                                \
      const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid class_oid, \
      const ColOid col_oid, const Column &col);
#define DEFINE_GET_COLUMNS(Column, ClassOid, ColOid)                             \
  template std::vector<Column> PgCoreImpl::GetColumns<Column, ClassOid, ColOid>( \
      const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid class_oid);
#define DEFINE_DELETE_COLUMNS(Column, ClassOid)              \
  template bool PgCoreImpl::DeleteColumns<Column, ClassOid>( \
      const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid class_oid);
#define DEFINE_MAKE_COLUMN(Column, ColOid)                                                \
  template Column PgCoreImpl::MakeColumn<Column, ColOid>(storage::ProjectedRow *const pr, \
                                                         const storage::ProjectionMap &pr_map);

DEFINE_SET_CLASS_POINTER(table_oid_t, storage::SqlTable);
DEFINE_SET_CLASS_POINTER(table_oid_t, Schema);
DEFINE_SET_CLASS_POINTER(index_oid_t, storage::index::Index);
DEFINE_SET_CLASS_POINTER(index_oid_t, IndexSchema);
DEFINE_CREATE_COLUMN(Schema::Column, table_oid_t, col_oid_t);
DEFINE_CREATE_COLUMN(IndexSchema::Column, index_oid_t, indexkeycol_oid_t);
DEFINE_GET_COLUMNS(Schema::Column, table_oid_t, col_oid_t);
DEFINE_GET_COLUMNS(IndexSchema::Column, index_oid_t, indexkeycol_oid_t);
DEFINE_DELETE_COLUMNS(Schema::Column, table_oid_t);
DEFINE_DELETE_COLUMNS(IndexSchema::Column, index_oid_t);
DEFINE_MAKE_COLUMN(Schema::Column, col_oid_t);
DEFINE_MAKE_COLUMN(IndexSchema::Column, indexkeycol_oid_t);

#undef DEFINE_SET_CLASS_POINTER
#undef DEFINE_CREATE_COLUMN
#undef DEFINE_GET_COLUMNS
#undef DEFINE_DELETE_COLUMNS
#undef DEFINE_MAKE_COLUMN

}  // namespace noisepage::catalog::postgres