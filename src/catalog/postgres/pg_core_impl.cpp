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

  const std::vector<col_oid_t> delete_namespace_oids{PgNamespace::NSPNAME.oid_};
  delete_namespace_pri_ = namespaces_->InitializerForProjectedRow(delete_namespace_oids);

  const std::vector<col_oid_t> get_namespace_oids{PgNamespace::NSPOID.oid_};
  get_namespace_pri_ = namespaces_->InitializerForProjectedRow(get_namespace_oids);
}

void PgCoreImpl::BootstrapPRIsPgClass() {
  const std::vector<col_oid_t> pg_class_all_oids{PgClass::PG_CLASS_ALL_COL_OIDS.cbegin(),
                                                 PgClass::PG_CLASS_ALL_COL_OIDS.cend()};
  pg_class_all_cols_pri_ = classes_->InitializerForProjectedRow(pg_class_all_oids);
  pg_class_all_cols_prm_ = classes_->ProjectionMapForOids(pg_class_all_oids);

  const std::vector<col_oid_t> get_class_oid_kind_oids{PgClass::RELOID.oid_, PgClass::RELKIND.oid_};
  get_class_oid_kind_pri_ = classes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> set_class_pointer_oids{PgClass::REL_PTR.oid_};
  set_class_pointer_pri_ = classes_->InitializerForProjectedRow(set_class_pointer_oids);

  const std::vector<col_oid_t> set_class_schema_oids{PgClass::REL_SCHEMA.oid_};
  set_class_schema_pri_ = classes_->InitializerForProjectedRow(set_class_schema_oids);

  const std::vector<col_oid_t> get_class_pointer_kind_oids{PgClass::REL_PTR.oid_, PgClass::RELKIND.oid_};
  get_class_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_schema_pointer_kind_oids{PgClass::REL_SCHEMA.oid_, PgClass::RELKIND.oid_};
  get_class_schema_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_schema_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_object_and_schema_oids{PgClass::REL_PTR.oid_, PgClass::REL_SCHEMA.oid_};
  get_class_object_and_schema_pri_ = classes_->InitializerForProjectedRow(get_class_object_and_schema_oids);
  get_class_object_and_schema_prm_ = classes_->ProjectionMapForOids(get_class_object_and_schema_oids);
}

void PgCoreImpl::BootstrapPRIsPgIndex() {
  const std::vector<col_oid_t> pg_index_all_oids{PgIndex::PG_INDEX_ALL_COL_OIDS.cbegin(),
                                                 PgIndex::PG_INDEX_ALL_COL_OIDS.cend()};
  pg_index_all_cols_pri_ = indexes_->InitializerForProjectedRow(pg_index_all_oids);
  pg_index_all_cols_prm_ = indexes_->ProjectionMapForOids(pg_index_all_oids);

  const std::vector<col_oid_t> get_indexes_oids{PgIndex::INDOID.oid_};
  get_indexes_pri_ = indexes_->InitializerForProjectedRow(get_indexes_oids);

  const std::vector<col_oid_t> delete_index_oids{PgIndex::INDOID.oid_, PgIndex::INDRELID.oid_};
  delete_index_pri_ = indexes_->InitializerForProjectedRow(delete_index_oids);
  delete_index_prm_ = indexes_->ProjectionMapForOids(delete_index_oids);
}

void PgCoreImpl::BootstrapPRIsPgAttribute() {
  const std::vector<col_oid_t> pg_attribute_all_oids{PgAttribute::PG_ATTRIBUTE_ALL_COL_OIDS.cbegin(),
                                                     PgAttribute::PG_ATTRIBUTE_ALL_COL_OIDS.end()};
  pg_attribute_all_cols_pri_ = columns_->InitializerForProjectedRow(pg_attribute_all_oids);
  pg_attribute_all_cols_prm_ = columns_->ProjectionMapForOids(pg_attribute_all_oids);

  const std::vector<col_oid_t> get_columns_oids{PgAttribute::ATTNUM.oid_,     PgAttribute::ATTNAME.oid_,
                                                PgAttribute::ATTTYPID.oid_,   PgAttribute::ATTTYPMOD.oid_,
                                                PgAttribute::ATTNOTNULL.oid_, PgAttribute::ADSRC.oid_};
  // TODO(Matt): Maybe length should still be read back out from the table rather than relying ton Column's constructor?
  get_columns_pri_ = columns_->InitializerForProjectedRow(get_columns_oids);
  get_columns_prm_ = columns_->ProjectionMapForOids(get_columns_oids);

  const std::vector<col_oid_t> delete_columns_oids{PgAttribute::ATTNUM.oid_, PgAttribute::ATTNAME.oid_};
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
  dbc->BootstrapTable(txn, PgNamespace::NAMESPACE_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                      "pg_namespace", Builder::GetNamespaceTableSchema(), namespaces_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgNamespace::NAMESPACE_TABLE_OID,
                      PgNamespace::NAMESPACE_OID_INDEX_OID, "pg_namespace_oid_index",
                      Builder::GetNamespaceOidIndexSchema(db_oid_), namespaces_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgNamespace::NAMESPACE_TABLE_OID,
                      PgNamespace::NAMESPACE_NAME_INDEX_OID, "pg_namespace_name_index",
                      Builder::GetNamespaceNameIndexSchema(db_oid_), namespaces_name_index_);
}

void PgCoreImpl::BootstrapPgClass(common::ManagedPointer<transaction::TransactionContext> txn,
                                  common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgClass::CLASS_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_class",
                      Builder::GetClassTableSchema(), classes_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgClass::CLASS_TABLE_OID,
                      PgClass::CLASS_OID_INDEX_OID, "pg_class_oid_index", Builder::GetClassOidIndexSchema(db_oid_),
                      classes_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgClass::CLASS_TABLE_OID,
                      PgClass::CLASS_NAME_INDEX_OID, "pg_class_name_index", Builder::GetClassNameIndexSchema(db_oid_),
                      classes_name_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgClass::CLASS_TABLE_OID,
                      PgClass::CLASS_NAMESPACE_INDEX_OID, "pg_class_namespace_index",
                      Builder::GetClassNamespaceIndexSchema(db_oid_), classes_namespace_index_);
}

void PgCoreImpl::BootstrapPgIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                                  common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgIndex::INDEX_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_index",
                      Builder::GetIndexTableSchema(), indexes_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgIndex::INDEX_TABLE_OID,
                      PgIndex::INDEX_OID_INDEX_OID, "pg_index_oid_index", Builder::GetIndexOidIndexSchema(db_oid_),
                      indexes_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgIndex::INDEX_TABLE_OID,
                      PgIndex::INDEX_TABLE_INDEX_OID, "pg_index_table_index",
                      Builder::GetIndexTableIndexSchema(db_oid_), indexes_table_index_);
}

void PgCoreImpl::BootstrapPgAttribute(common::ManagedPointer<transaction::TransactionContext> txn,
                                      common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->BootstrapTable(txn, PgAttribute::COLUMN_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_attribute",
                      Builder::GetColumnTableSchema(), columns_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgAttribute::COLUMN_TABLE_OID,
                      PgAttribute::COLUMN_OID_INDEX_OID, "pg_attribute_oid_index",
                      Builder::GetColumnOidIndexSchema(db_oid_), columns_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgAttribute::COLUMN_TABLE_OID,
                      PgAttribute::COLUMN_NAME_INDEX_OID, "pg_attribute_name_index",
                      Builder::GetColumnNameIndexSchema(db_oid_), columns_name_index_);
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

std::function<void(void)> PgCoreImpl::GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn,
                                                    const common::ManagedPointer<DatabaseCatalog> dbc) {
  std::vector<Schema *> table_schemas;
  std::vector<storage::SqlTable *> tables;
  std::vector<IndexSchema *> index_schemas;
  std::vector<storage::index::Index *> indexes;

  // pg_class (schemas & objects)
  const std::vector<col_oid_t> pg_class_oids{PgClass::RELKIND.oid_, PgClass::REL_SCHEMA.oid_, PgClass::REL_PTR.oid_};

  auto pci = classes_->InitializerForProjectedColumns(pg_class_oids, DatabaseCatalog::TEARDOWN_MAX_TUPLES);
  auto pm = classes_->ProjectionMapForOids(pg_class_oids);

  uint64_t buffer_len = pci.ProjectedColumnsSize();
  byte *buffer = common::AllocationUtil::AllocateAligned(buffer_len);
  auto pc = pci.Initialize(buffer);

  // Fetch pointers to the start of each attribute in the projected columns.
  auto classes = reinterpret_cast<PgClass::RelKind *>(pc->ColumnStart(pm[PgClass::RELKIND.oid_]));
  auto schemas = reinterpret_cast<void **>(pc->ColumnStart(pm[PgClass::REL_SCHEMA.oid_]));
  auto objects = reinterpret_cast<void **>(pc->ColumnStart(pm[PgClass::REL_PTR.oid_]));

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
  return [garbage_collector{dbc->garbage_collector_}, tables{std::move(tables)}, indexes{std::move(indexes)},
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
  auto *const redo = txn->StageWrite(db_oid_, PgNamespace::NAMESPACE_TABLE_OID, pg_namespace_all_cols_pri_);
  auto delta = common::ManagedPointer(redo->Delta());
  auto &pm = pg_namespace_all_cols_prm_;

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Insert into pg_namespace.
  {
    PgNamespace::NSPOID.Set(delta, pm, ns_oid);
    PgNamespace::NSPNAME.Set(delta, pm, name_varlen);
  }
  const auto tuple_slot = namespaces_->Insert(txn, redo);

  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT(name_pri.ProjectedRowSize() >= oid_pri.ProjectedRowSize(), "Name should be largest PRI.");
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Insert into pg_namespace_name_index.
  {
    auto *index_pr = name_pri.InitializeRow(buffer);
    index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);

    if (!namespaces_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {  // Conflict. Request abort.
      delete[] buffer;
      return false;
    }
  }

  // Insert into pg_namespace_oid_index.
  {
    auto *index_pr = oid_pri.InitializeRow(buffer);
    index_pr->Set<namespace_oid_t, false>(0, ns_oid, false);
    const bool UNUSED_ATTRIBUTE result = namespaces_oid_index_->InsertUnique(txn, *index_pr, tuple_slot);
    NOISEPAGE_ASSERT(result, "Assigned namespace OID failed to be unique.");
  }

  delete[] buffer;
  return true;
}

bool PgCoreImpl::DeleteNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const common::ManagedPointer<DatabaseCatalog> dbc, const namespace_oid_t ns_oid) {
  // This buffer is large enough for all prs because it's meant to hold 1 VarlenEntry.
  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_namespace_pri_.ProjectedRowSize());

  const auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();

  // Scan pg_namespace_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *pr = oid_pri.InitializeRow(buffer);
    pr->Set<namespace_oid_t, false>(0, ns_oid, false);
    namespaces_oid_index_->ScanKey(*txn, *pr, &index_results);
    NOISEPAGE_ASSERT(
        index_results.size() == 1,
        "Incorrect number of results from index scan. Expect 1 because it's a unique index."
        "0 implies that function was called with an oid that doesn't exist in the Catalog, but binding somehow "
        "succeeded. That doesn't make sense. Was a DROP plan node reused twice?"
        "IF EXISTS should be handled in the Binder, rather than pushing logic here.");
  }
  const auto tuple_slot = index_results[0];

  // Get the name from pg_namespace.
  storage::VarlenEntry name_varlen;
  {
    auto *pr = delete_namespace_pri_.InitializeRow(buffer);
    auto UNUSED_ATTRIBUTE result = namespaces_->Select(txn, tuple_slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    name_varlen = *pr->Get<storage::VarlenEntry, false>(0, nullptr);
  }

  // Delete from pg_namespace.
  {
    txn->StageDelete(db_oid_, PgNamespace::NAMESPACE_TABLE_OID, tuple_slot);
    if (!namespaces_->Delete(txn, tuple_slot)) {
      // Someone else has a write-lock. Free the buffer and return false to indicate failure
      delete[] buffer;
      return false;
    }
  }

  // Cascading deletes.
  // Get the objects in this namespace
  {
    auto ns_objects = GetNamespaceClassOids(txn, ns_oid);
    for (const auto &object : ns_objects) {
      // Delete all of the tables. This should get most of the indexes.
      if (object.second == PgClass::RelKind::REGULAR_TABLE) {
        if (!dbc->DeleteTable(txn, static_cast<table_oid_t>(object.first))) {  // Write-lock failed. Ask to abort.
          delete[] buffer;
          return false;
        }
      }
    }
  }

  // Get the objects in the namespace again, just in case there were any indexes that don't belong to a table in this
  // namespace. We could do all of this cascading cleanup with a more complex single index scan, but we're taking
  // advantage of existing PRIs and indexes and expecting that deleting a namespace isn't that common of an operation,
  // so we can be slightly less efficient than optimal.
  {
    auto ns_objects = GetNamespaceClassOids(txn, ns_oid);
    for (const auto &object : ns_objects) {
      // Delete all of the straggler indexes that may have been built on tables in other namespaces. We shouldn't get
      // any double-deletions because indexes on tables will already be invisible to us (logically deleted already).
      if (object.second == PgClass::RelKind::INDEX) {
        if (!dbc->DeleteIndex(txn, static_cast<index_oid_t>(object.first))) {  // Write-lock failed. Ask to abort.
          delete[] buffer;
          return false;
        }
      }
    }
    NOISEPAGE_ASSERT(GetNamespaceClassOids(txn, ns_oid).empty(), "Failed to drop all of the namespace objects.");
  }

  // Delete from pg_namespace_oid_index.
  {
    auto *pr = oid_pri.InitializeRow(buffer);
    pr->Set<namespace_oid_t, false>(0, ns_oid, false);
    namespaces_oid_index_->Delete(txn, *pr, tuple_slot);
  }

  // Delete from pg_namespace_name_index.
  {
    const auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
    auto *pr = name_pri.InitializeRow(buffer);
    pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    namespaces_name_index_->Delete(txn, *pr, tuple_slot);
  }

  delete[] buffer;
  return true;
}

namespace_oid_t PgCoreImpl::GetNamespaceOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const std::string &name) {
  // Buffer is large enough for all prs because it's meant to hold 1 VarlenEntry
  const auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Scan through pg_namespace_name_index.
  std::vector<storage::TupleSlot> index_results;
  {
    const auto name_varlen = storage::StorageUtil::CreateVarlen(name);
    auto *pr = name_pri.InitializeRow(buffer);
    pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    namespaces_name_index_->ScanKey(*txn, *pr, &index_results);
    if (name_varlen.NeedReclaim()) {
      delete[] name_varlen.Content();
    }
    if (index_results.empty()) {  // Namespace doesn't exist. Ask to abort.
      delete[] buffer;
      return INVALID_NAMESPACE_OID;
    }
    NOISEPAGE_ASSERT(index_results.size() == 1, "Namespace name not unique in index.");
  }

  // Scan through pg_namespace for the OID.
  namespace_oid_t ns_oid;
  {
    auto *pr = get_namespace_pri_.InitializeRow(buffer);
    const auto tuple_slot = index_results[0];
    const auto UNUSED_ATTRIBUTE result = namespaces_->Select(txn, tuple_slot, pr);
    NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
    ns_oid = *reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0));
  }

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
  NOISEPAGE_ASSERT(pointer != nullptr, "Why are you inserting nullptr here? That seems wrong.");

  const auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();
  auto pr_init = classes_->InitializerForProjectedRow({class_col});

  NOISEPAGE_ASSERT(pr_init.ProjectedRowSize() >= oid_pri.ProjectedRowSize(), "Buffer must allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());

  // Scan through pg_class_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *const key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<ClassOid, false>(0, oid, false);
    classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
    NOISEPAGE_ASSERT(
        index_results.size() == 1,
        "Incorrect number of results from index scan. Expect 1 because it's a unique index. "
        "0 implies that function was called with an oid that doesn't exist in the Catalog, which implies a programmer"
        "error. There's no reasonable code path for this to be called on an oid that isn't present.");
  }

  // Update pg_class.
  auto &initializer = (class_col == PgClass::REL_PTR.oid_) ? set_class_pointer_pri_ : set_class_schema_pri_;
  auto *update_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, initializer);
  update_redo->SetTupleSlot(index_results[0]);
  update_redo->Delta()->Set<const Ptr *, false>(0, pointer, false);

  delete[] buffer;
  return classes_->Update(txn, update_redo);
}

bool PgCoreImpl::CreateTableEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const table_oid_t table_oid, const namespace_oid_t ns_oid, const std::string &name,
                                  const Schema &schema) {
  auto *const insert_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, pg_class_all_cols_pri_);
  auto delta = common::ManagedPointer(insert_redo->Delta());
  auto &pm = pg_class_all_cols_prm_;

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Prepare for insertion.
  {
    auto next_col_oid = col_oid_t(static_cast<uint32_t>(schema.GetColumns().size() + 1));
    PgClass::RELOID.Set(delta, pm, table_oid);
    PgClass::RELNAME.Set(delta, pm, name_varlen);
    PgClass::RELNAMESPACE.Set(delta, pm, ns_oid);
    PgClass::RELKIND.Set(delta, pm, static_cast<char>(PgClass::RelKind::REGULAR_TABLE));
    PgClass::REL_SCHEMA.Set(delta, pm, nullptr);  // Need to update once we've recreated the columns.
    PgClass::REL_PTR.SetNull(delta, pm);
    PgClass::REL_NEXTCOLOID.Set(delta, pm, next_col_oid);
  }

  // Insert into pg_class.
  const auto tuple_slot = classes_->Insert(txn, insert_redo);

  // Insert into indexes.
  {
    // Get PR initializers and allocate a buffer from the largest one.
    const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
    const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
    const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
    NOISEPAGE_ASSERT(name_index_init.ProjectedRowSize() >= ns_index_init.ProjectedRowSize() &&
                         name_index_init.ProjectedRowSize() >= oid_index_init.ProjectedRowSize(),
                     "Name is expected to be the largest.");
    auto *const index_buffer = common::AllocationUtil::AllocateAligned(name_index_init.ProjectedRowSize());

    // Insert into pg_class_oid_index.
    {
      auto *index_pr = oid_index_init.InitializeRow(index_buffer);
      index_pr->Set<table_oid_t, false>(0, table_oid, false);
      if (!classes_oid_index_->InsertUnique(txn, *index_pr, tuple_slot)) {  // Oid conflict. Ask to abort.
        delete[] index_buffer;
        return false;
      }
    }

    // Insert into pg_class_name_index.
    {
      auto *index_pr = name_index_init.InitializeRow(index_buffer);
      index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
      index_pr->Set<namespace_oid_t, false>(1, ns_oid, false);
      if (!classes_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {  // Name conflict. Ask to abort.
        delete[] index_buffer;
        return false;
      }
    }

    // Insert into pg_class_namespace_index.
    {
      auto *index_pr = ns_index_init.InitializeRow(index_buffer);
      index_pr->Set<namespace_oid_t, false>(0, ns_oid, false);
      const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, tuple_slot);
      NOISEPAGE_ASSERT(result, "Insertion into non-unique namespace index failed.");
    }

    delete[] index_buffer;
  }

  // Create the columns.
  {
    col_oid_t curr_col_oid(1);
    for (auto &col : schema.GetColumns()) {
      auto success = CreateColumn(txn, table_oid, curr_col_oid++, col);
      if (!success) return false;
    }
  }

  // Write the schema for the columns.
  {
    std::vector<Schema::Column> cols = GetColumns<Schema::Column, table_oid_t, col_oid_t>(txn, table_oid);
    auto *new_schema = new Schema(cols);
    txn->RegisterAbortAction([=]() { delete new_schema; });

    auto *const update_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, set_class_schema_pri_);
    auto *const update_pr = update_redo->Delta();

    update_redo->SetTupleSlot(tuple_slot);
    update_pr->Set<Schema *, false>(0, new_schema, false);
    auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
    NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail");
  }

  return true;
}

bool PgCoreImpl::DeleteTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                             const common::ManagedPointer<DatabaseCatalog> dbc, const table_oid_t table) {
  // We should respect foreign key relations and attempt to delete the table's columns first.
  {
    auto result = DeleteColumns<Schema::Column, table_oid_t>(txn, table);
    if (!result) return false;
  }

  const auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT(pg_class_all_cols_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated for largest ProjectedRow size");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_class_all_cols_pri_.ProjectedRowSize());

  // Find the table entry using pg_class_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *const key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<table_oid_t, false>(0, table, false);
    classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
    NOISEPAGE_ASSERT(
        index_results.size() == 1,
        "Incorrect number of results from index scan. Expect 1 because it's a unique index."
        "0 implies that function was called with an oid that doesn't exist in the Catalog, but binding somehow "
        "succeeded. That doesn't make sense. Was a DROP plan node reused twice? IF EXISTS should be handled in the"
        "Binder, rather than pushing logic here.");
  }

  // Select the tuple out of pg_class before deletion. We need the attributes to do index deletions later.
  auto *const table_pr = pg_class_all_cols_pri_.InitializeRow(buffer);
  {
    bool UNUSED_ATTRIBUTE result = classes_->Select(txn, index_results[0], table_pr);
    NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");
  }

  // Delete from the pg_class table.
  {
    txn->StageDelete(db_oid_, PgClass::CLASS_TABLE_OID, index_results[0]);
    if (!classes_->Delete(txn, index_results[0])) {  // Write-write conflict. Ask to abort.
      // write-write conflict. Someone beat us to this operation.
      delete[] buffer;
      return false;
    }
  }

  // Delete all the relevant pg_index entries.
  {
    bool UNUSED_ATTRIBUTE result = dbc->DeleteIndexes(txn, table);
    NOISEPAGE_ASSERT(result, "We should have the DDL lock.");
  }

  // Delete from the pg_class indexes.
  Schema *schema_ptr;
  storage::SqlTable *table_ptr;
  {
    auto &pm = pg_class_all_cols_prm_;

    // Get the attributes we need for indexes.
    const table_oid_t table_oid = *PgClass::RELOID.Get(common::ManagedPointer(table_pr), pm);
    NOISEPAGE_ASSERT(table == table_oid,
                     "table oid from pg_class did not match what was found by the index scan from the argument.");
    const namespace_oid_t ns_oid = *PgClass::RELNAMESPACE.Get(common::ManagedPointer(table_pr), pm);
    const storage::VarlenEntry name_varlen = *PgClass::RELNAME.Get(common::ManagedPointer(table_pr), pm);

    // Get the attributes we need for delete.
    schema_ptr = *PgClass::REL_SCHEMA.Get(common::ManagedPointer(table_pr), pm);
    table_ptr = *PgClass::REL_PTR.Get(common::ManagedPointer(table_pr), pm);

    const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
    const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
    const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();

    // Delete from pg_class_oid_index.
    {
      auto *index_pr = oid_index_init.InitializeRow(buffer);
      index_pr->Set<table_oid_t, false>(0, table_oid, false);
      classes_oid_index_->Delete(txn, *index_pr, index_results[0]);
    }

    // Delete from pg_class_name_index.
    {
      auto *index_pr = name_index_init.InitializeRow(buffer);
      index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
      index_pr->Set<namespace_oid_t, false>(1, ns_oid, false);
      classes_name_index_->Delete(txn, *index_pr, index_results[0]);
    }

    // Delete from pg_class_namespace_index.
    {
      auto *index_pr = ns_index_init.InitializeRow(buffer);
      index_pr->Set<namespace_oid_t, false>(0, ns_oid, false);
      classes_namespace_index_->Delete(txn, *index_pr, index_results[0]);
    }
  }

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

bool PgCoreImpl::RenameTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                             const common::ManagedPointer<DatabaseCatalog> dbc, const table_oid_t table,
                             const std::string &name) {
  // TODO(John): Implement
  NOISEPAGE_ASSERT(false, "Not implemented");
  return false;
}

bool PgCoreImpl::CreateIndexEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const namespace_oid_t ns_oid, const table_oid_t table_oid,
                                  const index_oid_t index_oid, const std::string &name, const IndexSchema &schema) {
  auto *const class_insert_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, pg_class_all_cols_pri_);

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Prepare PR for insertion.
  {
    auto delta = common::ManagedPointer(class_insert_redo->Delta());
    auto &pm = pg_class_all_cols_prm_;

    PgClass::RELOID.Set(delta, pm, table_oid_t(index_oid.UnderlyingValue()));
    PgClass::RELNAME.Set(delta, pm, name_varlen);
    PgClass::RELNAMESPACE.Set(delta, pm, ns_oid);
    PgClass::RELKIND.Set(delta, pm, static_cast<char>(PgClass::RelKind::INDEX));
    PgClass::REL_SCHEMA.Set(delta, pm, nullptr);
    PgClass::REL_PTR.SetNull(delta, pm);         // Set by execution layer after instantiation.
    PgClass::REL_NEXTCOLOID.SetNull(delta, pm);  // Indexes don't need col_oid.
  }

  // Insert into pg_class.
  const auto class_tuple_slot = classes_->Insert(txn, class_insert_redo);

  // Insert into indexes on pg_class. Insert into pg_index.
  {
    // Get PR initializers, allocate a buffer from the largest one.
    const auto class_oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
    const auto class_name_index_init = classes_name_index_->GetProjectedRowInitializer();
    const auto class_ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
    NOISEPAGE_ASSERT((class_name_index_init.ProjectedRowSize() >= class_oid_index_init.ProjectedRowSize()) &&
                         (class_name_index_init.ProjectedRowSize() >= class_ns_index_init.ProjectedRowSize()),
                     "Index buffer must be allocated based on the largest PR initializer");
    auto *index_buffer = common::AllocationUtil::AllocateAligned(class_name_index_init.ProjectedRowSize());

    // Insert into pg_class_oid_index.
    {
      auto *index_pr = class_oid_index_init.InitializeRow(index_buffer);
      index_pr->Set<index_oid_t, false>(0, index_oid, false);
      if (!classes_oid_index_->InsertUnique(txn, *index_pr, class_tuple_slot)) {  // OID conflict. Ask to abort.
        delete[] index_buffer;
        return false;
      }
    }

    // Insert into pg_class_name_index.
    {
      auto *index_pr = class_name_index_init.InitializeRow(index_buffer);
      index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
      index_pr->Set<namespace_oid_t, false>(1, ns_oid, false);
      if (!classes_name_index_->InsertUnique(txn, *index_pr, class_tuple_slot)) {  // Name conflict. Ask to abort.
        delete[] index_buffer;
        return false;
      }
    }

    // Insert into pg_class_namespace_index.
    {
      auto *index_pr = class_ns_index_init.InitializeRow(index_buffer);
      index_pr->Set<namespace_oid_t, false>(0, ns_oid, false);
      const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, class_tuple_slot);
      NOISEPAGE_ASSERT(result, "Insertion into non-unique namespace index failed.");
    }

    // Next, insert index metadata into pg_index.
    {
      auto *const indexes_insert_redo = txn->StageWrite(db_oid_, PgIndex::INDEX_TABLE_OID, pg_index_all_cols_pri_);
      auto delta = common::ManagedPointer(indexes_insert_redo->Delta());
      auto &pm = pg_index_all_cols_prm_;

      PgIndex::INDOID.Set(delta, pm, index_oid);
      PgIndex::INDRELID.Set(delta, pm, table_oid);
      PgIndex::INDISUNIQUE.Set(delta, pm, schema.is_unique_);
      PgIndex::INDISPRIMARY.Set(delta, pm, schema.is_primary_);
      PgIndex::INDISEXCLUSION.Set(delta, pm, schema.is_exclusion_);
      PgIndex::INDIMMEDIATE.Set(delta, pm, schema.is_immediate_);

      // TODO(Matt): these should actually be set later based on runtime information about the index. @yeshengm
      PgIndex::INDISVALID.Set(delta, pm, true);
      PgIndex::INDISREADY.Set(delta, pm, true);
      PgIndex::INDISLIVE.Set(delta, pm, true);
      PgIndex::IND_TYPE.Set(delta, pm, static_cast<char>(schema.type_));

      // Insert into pg_index.
      const auto indexes_tuple_slot = indexes_->Insert(txn, indexes_insert_redo);

      // Now insert into the indexes on pg_index.

      // Get PR initializers and allocate a buffer from the largest one
      const auto indexes_oid_index_init = indexes_oid_index_->GetProjectedRowInitializer();
      const auto indexes_table_index_init = indexes_table_index_->GetProjectedRowInitializer();
      NOISEPAGE_ASSERT((class_name_index_init.ProjectedRowSize() >= indexes_oid_index_init.ProjectedRowSize()) &&
                           (class_name_index_init.ProjectedRowSize() > indexes_table_index_init.ProjectedRowSize()),
                       "Index buffer must be allocated based on the largest PR initializer");

      // Insert into pg_index_oid_index.
      {
        auto *index_pr = indexes_oid_index_init.InitializeRow(index_buffer);
        index_pr->Set<index_oid_t, false>(0, index_oid, false);
        if (!indexes_oid_index_->InsertUnique(txn, *index_pr, indexes_tuple_slot)) {  // OID conflict. Ask to abort.
          delete[] index_buffer;
          return false;
        }
      }

      // Insert into pg_index_table_index.
      {
        auto *index_pr = indexes_table_index_init.InitializeRow(index_buffer);
        index_pr->Set<table_oid_t, false>(0, table_oid, false);
        if (!indexes_table_index_->Insert(txn, *index_pr, indexes_tuple_slot)) {  // Duplicate value. Ask to abort.
          delete[] index_buffer;
          return false;
        }
      }
    }

    // Free the buffer, we are finally done.
    delete[] index_buffer;
  }

  // Write the col oids into a new Schema object
  // Create the columns.
  {
    indexkeycol_oid_t curr_col_oid(1);
    for (auto &col : schema.GetColumns()) {
      auto success = CreateColumn(txn, index_oid, curr_col_oid++, col);
      if (!success) return false;
    }
  }

  // Update pg_class with the new index schema.
  {
    std::vector<IndexSchema::Column> cols =
        GetColumns<IndexSchema::Column, index_oid_t, indexkeycol_oid_t>(txn, index_oid);
    auto *new_schema =
        new IndexSchema(cols, schema.Type(), schema.Unique(), schema.Primary(), schema.Exclusion(), schema.Immediate());
    txn->RegisterAbortAction([=]() { delete new_schema; });

    auto *const update_redo = txn->StageWrite(db_oid_, PgClass::CLASS_TABLE_OID, set_class_schema_pri_);
    auto *const update_pr = update_redo->Delta();

    update_redo->SetTupleSlot(class_tuple_slot);
    update_pr->Set<IndexSchema *, false>(0, new_schema, false);
    auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
    NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail.");
  }

  return true;
}

bool PgCoreImpl::DeleteIndex(const common::ManagedPointer<transaction::TransactionContext> txn,
                             common::ManagedPointer<DatabaseCatalog> dbc, index_oid_t index) {
  {
    // We should respect foreign key relations and attempt to delete the index's columns first.
    auto result = DeleteColumns<IndexSchema::Column, index_oid_t>(txn, index);
    if (!result) return false;
  }

  // Allocate buffer for largest PR.
  const auto class_oid_pri = classes_oid_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT(pg_class_all_cols_pri_.ProjectedRowSize() >= class_oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated for largest ProjectedRow size");
  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_class_all_cols_pri_.ProjectedRowSize());

  // Delete from pg_class and associated indexes.
  IndexSchema *schema_ptr;
  storage::index::Index *index_ptr;
  {
    // Find the entry using pg_class_oid_index.
    std::vector<storage::TupleSlot> index_results;
    {
      auto *key_pr = class_oid_pri.InitializeRow(buffer);
      key_pr->Set<index_oid_t, false>(0, index, false);
      classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
      NOISEPAGE_ASSERT(
          index_results.size() == 1,
          "Incorrect number of results from index scan. Expect 1 because it's a unique index."
          "0 implies that function was called with an oid that doesn't exist in the Catalog, but binding somehow "
          "succeeded. That doesn't make sense. Was a DROP plan node reused twice? IF EXISTS should be handled in the "
          "Binder, rather than pushing logic here.");
    }

    // Select the tuple out of the table before deletion. We need the attributes to do index deletions later.
    auto *table_pr = pg_class_all_cols_pri_.InitializeRow(buffer);
    {
      bool UNUSED_ATTRIBUTE result = classes_->Select(txn, index_results[0], table_pr);
      NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");
    }

    // Delete from pg_class.
    {
      txn->StageDelete(db_oid_, PgClass::CLASS_TABLE_OID, index_results[0]);
      bool UNUSED_ATTRIBUTE result = classes_->Delete(txn, index_results[0]);
      if (!result) {  // Write-write conflict. Ask to abort.
        delete[] buffer;
        return false;
      }
    }

    // Delete from the pg_class indexes.
    {
      // Get the attributes we need for pg_class indexes.
      table_oid_t table_oid = *PgClass::RELOID.Get(common::ManagedPointer(table_pr), pg_class_all_cols_prm_);
      const namespace_oid_t ns_oid =
          *PgClass::RELNAMESPACE.Get(common::ManagedPointer(table_pr), pg_class_all_cols_prm_);
      const storage::VarlenEntry name_varlen =
          *PgClass::RELNAME.Get(common::ManagedPointer(table_pr), pg_class_all_cols_prm_);
      schema_ptr = reinterpret_cast<IndexSchema *>(
          *PgClass::REL_SCHEMA.Get(common::ManagedPointer(table_pr), pg_class_all_cols_prm_));
      index_ptr = reinterpret_cast<storage::index::Index *>(
          *PgClass::REL_PTR.Get(common::ManagedPointer(table_pr), pg_class_all_cols_prm_));

      // Delete from pg_class_oid_index.
      {
        const auto class_oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
        auto *index_pr = class_oid_index_init.InitializeRow(buffer);
        index_pr->Set<table_oid_t, false>(0, table_oid, false);
        classes_oid_index_->Delete(txn, *index_pr, index_results[0]);
      }

      // Delete from pg_class_name_index.
      {
        const auto class_name_index_init = classes_name_index_->GetProjectedRowInitializer();
        auto *index_pr = class_name_index_init.InitializeRow(buffer);
        index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
        index_pr->Set<namespace_oid_t, false>(1, ns_oid, false);
        classes_name_index_->Delete(txn, *index_pr, index_results[0]);
      }

      // Delete from pg_class_namespace_index.
      {
        const auto class_ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
        auto *index_pr = class_ns_index_init.InitializeRow(buffer);
        index_pr->Set<namespace_oid_t, false>(0, ns_oid, false);
        classes_namespace_index_->Delete(txn, *index_pr, index_results[0]);
      }
    }
  }

  // Delete from pg_index and associated indexes.
  {
    // Initialize PRs for pg_index.
    const auto index_oid_pr = indexes_oid_index_->GetProjectedRowInitializer();
    const auto index_table_pr = indexes_table_index_->GetProjectedRowInitializer();

    NOISEPAGE_ASSERT((pg_class_all_cols_pri_.ProjectedRowSize() >= delete_index_pri_.ProjectedRowSize()) &&
                         (pg_class_all_cols_pri_.ProjectedRowSize() >= index_oid_pr.ProjectedRowSize()) &&
                         (pg_class_all_cols_pri_.ProjectedRowSize() >= index_table_pr.ProjectedRowSize()),
                     "Buffer must be allocated for largest ProjectedRow size");

    // Find the entry in pg_index using pg_index_oid_index.
    std::vector<storage::TupleSlot> index_results;
    {
      auto *key_pr = index_oid_pr.InitializeRow(buffer);
      key_pr->Set<index_oid_t, false>(0, index, false);
      indexes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
      NOISEPAGE_ASSERT(index_results.size() == 1,
                       "Incorrect number of results from index scan. Expect 1 because it's a unique index. size() of 0 "
                       "implies an error in Catalog state because scanning pg_class worked, but it doesn't exist in "
                       "pg_index. Something broke.");
    }

    // Select the tuple out of pg_index before deletion. We need the attributes to do index deletions later.
    auto table_pr = common::ManagedPointer(delete_index_pri_.InitializeRow(buffer));
    {
      bool UNUSED_ATTRIBUTE result = indexes_->Select(txn, index_results[0], table_pr.Get());
      NOISEPAGE_ASSERT(result, "Select must succeed if the index scan gave a visible result.");
      NOISEPAGE_ASSERT(index == *(reinterpret_cast<const index_oid_t *const>(
                                    table_pr->AccessForceNotNull(delete_index_prm_[PgIndex::INDOID.oid_]))),
                       "index oid from pg_index did not match what was found by the index scan from the argument.");
    }

    // Delete from pg_index.
    {
      txn->StageDelete(db_oid_, PgIndex::INDEX_TABLE_OID, index_results[0]);
      bool UNUSED_ATTRIBUTE result = indexes_->Delete(txn, index_results[0]);
      NOISEPAGE_ASSERT(result,
                       "Delete from pg_index should always succeed as write-write conflicts are detected during delete "
                       "from pg_class");
    }

    // Get the table oid.
    const auto table_oid = *PgIndex::INDRELID.Get(table_pr, delete_index_prm_);

    // Delete from pg_index_oid_index.
    {
      auto *index_pr = index_oid_pr.InitializeRow(buffer);
      index_pr->Set<index_oid_t, false>(0, index, false);
      indexes_oid_index_->Delete(txn, *index_pr, index_results[0]);
    }

    // Delete from pg_index_table_index.
    {
      auto *index_pr = index_table_pr.InitializeRow(buffer);
      index_pr->Set<table_oid_t, false>(0, table_oid, false);
      indexes_table_index_->Delete(txn, *index_pr, index_results[0]);
    }

    // Everything succeeded from an MVCC standpoint, so register a deferred action for the GC to delete the index with
    // txn manager. See base function comment.
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
  }

  delete[] buffer;
  return true;
}

std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> PgCoreImpl::GetIndexes(
    const common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) {
  auto indexes_oid_pri = indexes_table_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT(get_class_object_and_schema_pri_.ProjectedRowSize() >= indexes_oid_pri.ProjectedRowSize() &&
                       get_class_object_and_schema_pri_.ProjectedRowSize() >= get_indexes_pri_.ProjectedRowSize() &&
                       get_class_object_and_schema_pri_.ProjectedRowSize() >=
                           classes_oid_index_->GetProjectedRowInitializer().ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");

  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_object_and_schema_pri_.ProjectedRowSize());

  // Collect the OIDs of the indexes.
  std::vector<index_oid_t> index_oids;
  {
    // Find all entries for the given table using pg_index_table_index.
    std::vector<storage::TupleSlot> index_scan_results;
    {
      auto *indexes_key_pr = indexes_oid_pri.InitializeRow(buffer);
      indexes_key_pr->Set<table_oid_t, false>(0, table, false);
      indexes_table_index_->ScanKey(*txn, *indexes_key_pr, &index_scan_results);
    }

    // If we found no indexes, return an empty list.
    if (index_scan_results.empty()) {
      delete[] buffer;
      return {};
    }

    // Collect index OIDs.
    {
      index_oids.reserve(index_scan_results.size());
      auto *index_select_pr = get_indexes_pri_.InitializeRow(buffer);
      for (auto &slot : index_scan_results) {
        const auto result UNUSED_ATTRIBUTE = indexes_->Select(txn, slot, index_select_pr);
        NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
        index_oids.emplace_back(*index_select_pr->Get<index_oid_t, false>(0, nullptr));
      }
    }
  }

  // Scan the pg_class_oid_index for all entries in pg_class.
  // We do the index scans and table selects in separate loops to avoid having to initialize the PR each time.
  std::vector<storage::TupleSlot> class_tuple_slots;
  class_tuple_slots.reserve(index_oids.size());
  {
    std::vector<storage::TupleSlot> index_scan_results;
    auto *class_key_pr = classes_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
    for (const auto &index_oid : index_oids) {
      // Find the index entry using pg_class_oid_index.
      class_key_pr->Set<index_oid_t, false>(0, index_oid, false);
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
  }

  // Select all the objects from the tuple slots retrieved above.
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> index_objects;
  {
    index_objects.reserve(class_tuple_slots.size());
    auto class_select_pr = common::ManagedPointer(get_class_object_and_schema_pri_.InitializeRow(buffer));
    for (const auto &slot : class_tuple_slots) {
      bool result UNUSED_ATTRIBUTE = classes_->Select(txn, slot, class_select_pr.Get());
      NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

      auto *index = reinterpret_cast<storage::index::Index *>(
          *PgClass::REL_PTR.Get(class_select_pr, get_class_object_and_schema_prm_));

      NOISEPAGE_ASSERT(
          index != nullptr,
          "Catalog conventions say you should not find a nullptr for an object ptr in pg_class. Did you call "
          "SetIndexPointer?");
      auto *schema =
          reinterpret_cast<IndexSchema *>(*PgClass::REL_SCHEMA.Get(class_select_pr, get_class_object_and_schema_prm_));
      NOISEPAGE_ASSERT(schema != nullptr,
                       "Catalog conventions say you should not find a nullptr for an schema ptr in pg_class");

      index_objects.emplace_back(common::ManagedPointer(index), *schema);
    }
  }

  delete[] buffer;
  return index_objects;
}

std::vector<index_oid_t> PgCoreImpl::GetIndexOids(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                  table_oid_t table) {
  auto oid_pri = indexes_table_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT(get_indexes_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_indexes_pri_.ProjectedRowSize());

  // Find all entries for the given table using pg_index_table_index.
  std::vector<storage::TupleSlot> index_scan_results;
  {
    auto *key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<table_oid_t, false>(0, table, false);
    indexes_table_index_->ScanKey(*txn, *key_pr, &index_scan_results);
  }

  // If we found no indexes, return an empty list.
  {
    if (index_scan_results.empty()) {
      delete[] buffer;
      return {};
    }
  }

  // Collect index OIDs from pg_index.
  std::vector<index_oid_t> index_oids;
  {
    index_oids.reserve(index_scan_results.size());
    auto *select_pr = get_indexes_pri_.InitializeRow(buffer);
    for (auto &slot : index_scan_results) {
      const auto result UNUSED_ATTRIBUTE = indexes_->Select(txn, slot, select_pr);
      NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
      index_oids.emplace_back(*select_pr->Get<index_oid_t, false>(0, nullptr));
    }
  }

  delete[] buffer;
  return index_oids;
}

std::vector<std::pair<uint32_t, PgClass::RelKind>> PgCoreImpl::GetNamespaceClassOids(
    const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid) {
  // Initialize both PR initializers, allocate buffer using size of largest one so we can reuse buffer.
  auto oid_pri = classes_namespace_index_->GetProjectedRowInitializer();
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_oid_kind_pri_.ProjectedRowSize());

  // Find the entry using pg_class_namespace_index.
  std::vector<storage::TupleSlot> index_scan_results;
  {
    auto *key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<namespace_oid_t, false>(0, ns_oid, false);
    classes_namespace_index_->ScanKey(*txn, *key_pr, &index_scan_results);
  }

  // If we found no objects, return an empty list.
  {
    if (index_scan_results.empty()) {
      delete[] buffer;
      return {};
    }
  }

  // Collect namespace OIDs from pg_class.
  std::vector<std::pair<uint32_t, PgClass::RelKind>> ns_objects;
  {
    auto *select_pr = get_class_oid_kind_pri_.InitializeRow(buffer);
    ns_objects.reserve(index_scan_results.size());
    for (const auto scan_result : index_scan_results) {
      const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, scan_result, select_pr);
      NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
      // oid_t is guaranteed to be larger in size than ClassKind, so we know the column offsets without the PR map.
      ns_objects.emplace_back(select_pr->Get<namespace_oid_t, false>(0, nullptr)->UnderlyingValue(),
                              static_cast<PgClass::RelKind>(*select_pr->Get<char, false>(1, nullptr)));
    }
  }

  delete[] buffer;
  return ns_objects;
}

std::pair<void *, PgClass::RelKind> PgCoreImpl::GetClassPtrKind(
    const common::ManagedPointer<transaction::TransactionContext> txn, uint32_t oid) {
  // Initialize both PR initializers, allocate buffer using size of largest one so we can reuse buffer.
  auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Since these two attributes are fixed size and one is larger than the other we know PTR is 0 and KIND is 1.
  NOISEPAGE_ASSERT(get_class_pointer_kind_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_pointer_kind_pri_.ProjectedRowSize());

  // Find the entry using pg_class_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<table_oid_t, false>(0, table_oid_t(oid), false);
    classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
    NOISEPAGE_ASSERT(index_results.size() == 1,
                     "Incorrect number of results from index scan. Expect 1 because it's a unique index. "
                     "0 implies that function was called with an oid that doesn't exist in the Catalog, but binding "
                     "somehow succeeded. That doesn't make sense.");
  }

  // Select the tuple out.
  auto *select_pr = get_class_pointer_kind_pri_.InitializeRow(buffer);
  {
    const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
  }

  auto *const ptr_ptr = select_pr->Get<void *, false>(0, nullptr);
  auto kind = static_cast<PgClass::RelKind>(*select_pr->Get<char, false>(1, nullptr));

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
  // Initialize both PR initializers, allocate buffer using size of largest one so we can reuse buffer.
  auto oid_pri = classes_oid_index_->GetProjectedRowInitializer();

  // Since these two attributes are fixed size and one is larger than the other we know PTR is 0 and KIND is 1.
  NOISEPAGE_ASSERT(get_class_schema_pointer_kind_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                   "Buffer must be allocated to fit largest PR");
  auto *const buffer = common::AllocationUtil::AllocateAligned(get_class_schema_pointer_kind_pri_.ProjectedRowSize());

  // Find the entry using pg_class_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *key_pr = oid_pri.InitializeRow(buffer);
    key_pr->Set<table_oid_t, false>(0, table_oid_t(oid), false);
    classes_oid_index_->ScanKey(*txn, *key_pr, &index_results);
    NOISEPAGE_ASSERT(index_results.size() == 1,
                     "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies "
                     "that function was "
                     "called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That "
                     "doesn't make sense.");
  }

  // Select the tuple out.
  auto *select_pr = get_class_schema_pointer_kind_pri_.InitializeRow(buffer);
  {
    const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
  }

  auto *const ptr = *select_pr->Get<void *, false>(0, nullptr);
  auto kind = static_cast<PgClass::RelKind>(*select_pr->Get<char, false>(1, nullptr));

  NOISEPAGE_ASSERT(ptr != nullptr, "Schema pointer shouldn't ever be NULL under current catalog semantics.");

  delete[] buffer;
  return {ptr, kind};
}

std::pair<uint32_t, PgClass::RelKind> PgCoreImpl::GetClassOidKind(
    const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid,
    const std::string &name) {
  const auto name_pri = classes_name_index_->GetProjectedRowInitializer();

  // Buffer is large enough to hold all PRs.
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Find the entry in pg_class_name_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto pr = name_pri.InitializeRow(buffer);
    const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

    // We know the offsets without a projection map because of the ordering of attribute sizes.
    pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    pr->Set<namespace_oid_t, false>(1, ns_oid, false);

    classes_name_index_->ScanKey(*txn, *pr, &index_results);

    if (name_varlen.NeedReclaim()) {
      delete[] name_varlen.Content();
    }
  }

  // If the OID is invalid, we don't care about the class kind and return a random one.
  {
    if (index_results.empty()) {
      delete[] buffer;
      return std::make_pair(catalog::NULL_OID, PgClass::RelKind::REGULAR_TABLE);
    }
  }

  NOISEPAGE_ASSERT(index_results.size() == 1, "Name not unique in pg_class_name_index.");
  NOISEPAGE_ASSERT(get_class_oid_kind_pri_.ProjectedRowSize() <= name_pri.ProjectedRowSize(),
                   "I want to reuse this buffer because I'm lazy and malloc is slow but it needs to be big enough.");

  // Select out the tuple from pg_class.
  auto *pr = get_class_oid_kind_pri_.InitializeRow(buffer);
  {
    const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
  }

  // Get the OID and kind.
  uint32_t oid;
  PgClass::RelKind kind;
  {
    oid = pr->Get<namespace_oid_t, false>(0, nullptr)->UnderlyingValue();
    kind = static_cast<PgClass::RelKind>(*pr->Get<char, false>(1, nullptr));
  }

  delete[] buffer;
  return std::make_pair(oid, kind);
}

template <typename Column, typename ClassOid, typename ColOid>
bool PgCoreImpl::CreateColumn(const common::ManagedPointer<transaction::TransactionContext> txn,
                              const ClassOid class_oid, const ColOid col_oid, const Column &col) {
  static_assert(std::is_same_v<ClassOid, table_oid_t> || std::is_same_v<ClassOid, index_oid_t>, "Invalid ClassOid.");
  static_assert(std::is_same_v<ColOid, col_oid_t> || std::is_same_v<ColOid, indexkeycol_oid_t>, "Invalid ColOid.");

  auto *const redo = txn->StageWrite(db_oid_, PgAttribute::COLUMN_TABLE_OID, pg_attribute_all_cols_pri_);

  const auto name_varlen = storage::StorageUtil::CreateVarlen(col.Name());
  {
    auto delta = common::ManagedPointer(redo->Delta());
    auto &pm = pg_attribute_all_cols_prm_;
    const auto dsrc_varlen = storage::StorageUtil::CreateVarlen(col.StoredExpression()->ToJson().dump());
    const auto attlen = col.AttributeLength();
    const auto atttypmod = col.TypeModifier();

    PgAttribute::ATTNUM.Set(delta, pm, col_oid_t(col_oid.UnderlyingValue()));
    PgAttribute::ATTRELID.Set(delta, pm, table_oid_t(class_oid.UnderlyingValue()));
    PgAttribute::ATTNAME.Set(delta, pm, name_varlen);
    PgAttribute::ATTTYPID.Set(delta, pm, type_oid_t(static_cast<uint32_t>(col.Type())));
    PgAttribute::ATTLEN.Set(delta, pm, attlen);
    PgAttribute::ATTTYPMOD.Set(delta, pm, atttypmod);
    PgAttribute::ATTNOTNULL.Set(delta, pm, !col.Nullable());
    PgAttribute::ADSRC.Set(delta, pm, dsrc_varlen);
  }

  // Insert into pg_attribute.
  const auto tupleslot = columns_->Insert(txn, redo);

  // Create a buffer large enough for all columns.
  const auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Insert into pg_attribute_name_index.
  {
    auto *pr = name_pri.InitializeRow(buffer);
    // We know the offsets without the map because of the ordering of attribute sizes.
    pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    pr->Set<ClassOid, false>(1, class_oid, false);

    if (!columns_name_index_->InsertUnique(txn, *pr, tupleslot)) {  // Name conflict. Ask to abort.
      delete[] buffer;
      if (name_varlen.NeedReclaim()) {
        delete[] name_varlen.Content();
      }
      return false;
    }
  }

  // Insert into pg_attribute_oid_index.
  {
    const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
    auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();
    auto *pr = oid_pri.InitializeRow(buffer);
    pr->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], class_oid, false);
    pr->Set<ColOid, false>(oid_prm[indexkeycol_oid_t(2)], col_oid, false);
    bool UNUSED_ATTRIBUTE result = columns_oid_index_->InsertUnique(txn, *pr, tupleslot);
    NOISEPAGE_ASSERT(result, "Assigned OIDs failed to be unique.");
  }

  delete[] buffer;
  return true;
}

template <typename Column, typename ClassOid, typename ColOid>
std::vector<Column> PgCoreImpl::GetColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
                                           ClassOid class_oid) {
  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();

  // Buffer is large enough to hold all PRs.
  byte *const buffer = common::AllocationUtil::AllocateAligned(get_columns_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());

  // Find the entry in pg_attribute_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    NOISEPAGE_ASSERT(get_columns_pri_.ProjectedRowSize() >= oid_pri.ProjectedRowSize(),
                     "Buffer must be large enough to fit largest PR");
    auto *pr = oid_pri.InitializeRow(buffer);
    auto *pr_high = oid_pri.InitializeRow(key_buffer);

    // Low key (class, INVALID_COLUMN_OID)
    pr->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], class_oid, false);
    pr->Set<ColOid, false>(oid_prm[indexkeycol_oid_t(2)], ColOid(0), false);

    // High key (class + 1, INVALID_COLUMN_OID)
    pr_high->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], ++class_oid, false);
    pr_high->Set<ColOid, false>(oid_prm[indexkeycol_oid_t(2)], ColOid(0), false);
    columns_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, pr_high, 0, &index_results);

    NOISEPAGE_ASSERT(
        !index_results.empty(),
        "Incorrect number of results from index scan. empty() implies that function was called with an oid "
        "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");
  }

  // Scan pg_attribute to get the columns.
  std::vector<Column> cols;
  {
    auto *pr = get_columns_pri_.InitializeRow(buffer);
    for (const auto &slot : index_results) {
      const auto UNUSED_ATTRIBUTE result = columns_->Select(txn, slot, pr);
      NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");
      cols.emplace_back(MakeColumn<Column, ColOid>(pr, get_columns_prm_));
    }
  }

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  delete[] buffer;
  delete[] key_buffer;
  return cols;
}

// TODO(Matt): we need a DeleteColumn()

template <typename Column, typename ClassOid>
bool PgCoreImpl::DeleteColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
                               const ClassOid class_oid) {
  const auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  const auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  auto oid_prm = columns_oid_index_->GetKeyOidToOffsetMap();

  // Buffer is large enough to hold all PRs.
  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_columns_pri_.ProjectedRowSize());
  byte *const key_buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Find the entry in pg_attribute_oid_index.
  std::vector<storage::TupleSlot> index_results;
  {
    auto *pr = oid_pri.InitializeRow(buffer);
    auto *pr_high = oid_pri.InitializeRow(key_buffer);

    // Write the attributes in the ProjectedRow
    // Low key (class, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
    pr->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], class_oid, false);
    pr->Set<uint32_t, false>(oid_prm[indexkeycol_oid_t(2)], 0, false);

    // High key (class + 1, INVALID_COLUMN_OID) [using uint32_t to avoid adding ColOid to template]
    auto next_oid = ClassOid(class_oid.UnderlyingValue() + 1);
    pr_high->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], next_oid, false);
    pr_high->Set<uint32_t, false>(oid_prm[indexkeycol_oid_t(2)], 0, false);

    columns_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr, pr_high, 0, &index_results);

    NOISEPAGE_ASSERT(
        !index_results.empty(),
        "Incorrect number of results from index scan. empty() implies that function was called with an oid "
        "that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't make sense.");
  }

  // TODO(Matt): do we have any way to assert that we got the number of attributes we expect? From another attribute in
  // another catalog table maybe?

  // Scan the pg_attribute to get the columns.
  {
    auto pr = common::ManagedPointer(delete_columns_pri_.InitializeRow(buffer));
    for (const auto &slot : index_results) {
      auto UNUSED_ATTRIBUTE result = columns_->Select(txn, slot, pr.Get());
      NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");

      auto &pm = delete_columns_prm_;
      auto *col_name = PgAttribute::ATTNAME.Get(pr, pm);
      NOISEPAGE_ASSERT(col_name != nullptr, "Name shouldn't be NULL.");
      auto *col_oid = PgAttribute::ATTNUM.Get(pr, pm);
      NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

      // Delete from pg_attribute.
      {
        txn->StageDelete(db_oid_, PgAttribute::COLUMN_TABLE_OID, slot);
        result = columns_->Delete(txn, slot);
        if (!result) {  // Failed to delete some column. Ask to abort.
          delete[] buffer;
          delete[] key_buffer;
          return false;
        }
      }

      // Delete from pg_attribute_oid_index.
      {
        auto *key_pr = oid_pri.InitializeRow(key_buffer);
        key_pr->Set<ClassOid, false>(oid_prm[indexkeycol_oid_t(1)], class_oid, false);
        key_pr->Set<uint32_t, false>(oid_prm[indexkeycol_oid_t(2)], col_oid->UnderlyingValue(), false);
        columns_oid_index_->Delete(txn, *key_pr, slot);
      }

      // Delete from pg_attribute_name_index.
      {
        auto *key_pr = name_pri.InitializeRow(key_buffer);
        key_pr->Set<storage::VarlenEntry, false>(0, *col_name, false);
        key_pr->Set<ClassOid, false>(1, class_oid, false);
        columns_name_index_->Delete(txn, *key_pr, slot);
      }
    }
  }

  delete[] buffer;
  delete[] key_buffer;
  return true;
}

template <typename Column, typename ColOid>
Column PgCoreImpl::MakeColumn(storage::ProjectedRow *const pr, const storage::ProjectionMap &pr_map) {
  auto delta = common::ManagedPointer(pr);
  const auto col_oid = *PgAttribute::ATTNUM.Get(delta, pr_map);
  const auto col_name = PgAttribute::ATTNAME.Get(delta, pr_map);
  const auto col_type = *PgAttribute::ATTTYPID.Get(delta, pr_map);
  const auto col_mod = *PgAttribute::ATTTYPMOD.Get(delta, pr_map);
  const auto col_null = !(*PgAttribute::ATTNOTNULL.Get(delta, pr_map));
  const auto *const col_expr = PgAttribute::ADSRC.Get(delta, pr_map);

  // See the warning in DatabaseCatalog::GetTypeOidForType. The below line needs to change if you modify that.
  const auto type = static_cast<type::TypeId>(col_type.UnderlyingValue());

  // TODO(WAN): Why are we deserializing expressions to make a catalog column? This is potentially busted.
  // Our JSON library is also not the most performant.
  // I believe it is OK that the unique ptr goes out of scope because right now both Column constructors copy the expr.
  auto deserialized = parser::DeserializeExpression(nlohmann::json::parse(col_expr->StringView()));

  auto expr = std::move(deserialized.result_);
  NOISEPAGE_ASSERT(deserialized.non_owned_exprs_.empty(), "Congrats, you get to refactor the catalog API.");

  const std::string name(reinterpret_cast<const char *>(col_name->Content()), col_name->Size());
  Column col = (type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY || type == type::TypeId::DECIMAL)
                   ? Column(name, type, col_mod, col_null, *expr)
                   : Column(name, type, col_null, *expr);

  col.SetOid(ColOid(col_oid.UnderlyingValue()));
  return col;
}

#define DEFINE_SET_CLASS_POINTER(ClassOid, Ptr)                                                                  \
  template bool PgCoreImpl::SetClassPointer<ClassOid, Ptr>(                                                      \
      const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid oid, const Ptr *pointer, \
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
