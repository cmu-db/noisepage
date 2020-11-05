#include "catalog/database_catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_language.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_proc.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "common/error/error_code.h"
#include "execution/functions/function_context.h"
#include "nlohmann/json.hpp"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"

namespace noisepage::catalog {

void DatabaseCatalog::Bootstrap(const common::ManagedPointer<transaction::TransactionContext> txn) {
  BootstrapPRIs();

  // Declare variable for return values (UNUSED when compiled for release)
  bool UNUSED_ATTRIBUTE retval;

  retval = TryLock(txn);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateNamespace(txn, "pg_catalog", postgres::NAMESPACE_CATALOG_NAMESPACE_OID);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateNamespace(txn, "public", postgres::NAMESPACE_DEFAULT_NAMESPACE_OID);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapTypes(txn);

  // pg_namespace and associated indexes
  retval = CreateTableEntry(txn, postgres::NAMESPACE_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID,
                            "pg_namespace", postgres::Builder::GetNamespaceTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::NAMESPACE_TABLE_OID, namespaces_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::NAMESPACE_TABLE_OID,
                            postgres::NAMESPACE_OID_INDEX_OID, "pg_namespace_oid_index",
                            postgres::Builder::GetNamespaceOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::NAMESPACE_OID_INDEX_OID, namespaces_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::NAMESPACE_TABLE_OID,
                            postgres::NAMESPACE_NAME_INDEX_OID, "pg_namespace_name_index",
                            postgres::Builder::GetNamespaceNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::NAMESPACE_NAME_INDEX_OID, namespaces_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  // pg_class and associated indexes
  retval = CreateTableEntry(txn, postgres::CLASS_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_class",
                            postgres::Builder::GetClassTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::CLASS_TABLE_OID, classes_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CLASS_TABLE_OID,
                            postgres::CLASS_OID_INDEX_OID, "pg_class_oid_index",
                            postgres::Builder::GetClassOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CLASS_OID_INDEX_OID, classes_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CLASS_TABLE_OID,
                            postgres::CLASS_NAME_INDEX_OID, "pg_class_name_index",
                            postgres::Builder::GetClassNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CLASS_NAME_INDEX_OID, classes_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CLASS_TABLE_OID,
                            postgres::CLASS_NAMESPACE_INDEX_OID, "pg_class_namespace_index",
                            postgres::Builder::GetClassNamespaceIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CLASS_NAMESPACE_INDEX_OID, classes_namespace_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  // pg_index and associated indexes
  retval = CreateTableEntry(txn, postgres::INDEX_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_index",
                            postgres::Builder::GetIndexTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::INDEX_TABLE_OID, indexes_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::INDEX_TABLE_OID,
                            postgres::INDEX_OID_INDEX_OID, "pg_index_oid_index",
                            postgres::Builder::GetIndexOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::INDEX_OID_INDEX_OID, indexes_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::INDEX_TABLE_OID,
                            postgres::INDEX_TABLE_INDEX_OID, "pg_index_table_index",
                            postgres::Builder::GetIndexTableIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::INDEX_TABLE_INDEX_OID, indexes_table_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  // pg_attribute and associated indexes
  retval = CreateTableEntry(txn, postgres::COLUMN_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_attribute",
                            postgres::Builder::GetColumnTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::COLUMN_TABLE_OID, columns_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::COLUMN_TABLE_OID,
                            postgres::COLUMN_OID_INDEX_OID, "pg_attribute_oid_index",
                            postgres::Builder::GetColumnOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::COLUMN_OID_INDEX_OID, columns_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::COLUMN_TABLE_OID,
                            postgres::COLUMN_NAME_INDEX_OID, "pg_attribute_name_index",
                            postgres::Builder::GetColumnNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::COLUMN_NAME_INDEX_OID, columns_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  // pg_type and associated indexes
  retval = CreateTableEntry(txn, postgres::TYPE_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_type",
                            postgres::Builder::GetTypeTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::TYPE_TABLE_OID, types_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::TYPE_TABLE_OID,
                            postgres::TYPE_OID_INDEX_OID, "pg_type_oid_index",
                            postgres::Builder::GetTypeOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::TYPE_OID_INDEX_OID, types_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::TYPE_TABLE_OID,
                            postgres::TYPE_NAME_INDEX_OID, "pg_type_name_index",
                            postgres::Builder::GetTypeNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::TYPE_NAME_INDEX_OID, types_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::TYPE_TABLE_OID,
                            postgres::TYPE_NAMESPACE_INDEX_OID, "pg_type_namespace_index",
                            postgres::Builder::GetTypeNamespaceIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::TYPE_NAMESPACE_INDEX_OID, types_namespace_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  // pg_constraint and associated indexes
  retval = CreateTableEntry(txn, postgres::CONSTRAINT_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID,
                            "pg_constraint", postgres::Builder::GetConstraintTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::CONSTRAINT_TABLE_OID, constraints_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CONSTRAINT_TABLE_OID,
                            postgres::CONSTRAINT_OID_INDEX_OID, "pg_constraint_oid_index",
                            postgres::Builder::GetConstraintOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CONSTRAINT_OID_INDEX_OID, constraints_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CONSTRAINT_TABLE_OID,
                            postgres::CONSTRAINT_NAME_INDEX_OID, "pg_constraint_name_index",
                            postgres::Builder::GetConstraintNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CONSTRAINT_NAME_INDEX_OID, constraints_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CONSTRAINT_TABLE_OID,
                            postgres::CONSTRAINT_NAMESPACE_INDEX_OID, "pg_constraint_namespace_index",
                            postgres::Builder::GetConstraintNamespaceIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CONSTRAINT_NAMESPACE_INDEX_OID, constraints_namespace_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CONSTRAINT_TABLE_OID,
                            postgres::CONSTRAINT_TABLE_INDEX_OID, "pg_constraint_table_index",
                            postgres::Builder::GetConstraintTableIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CONSTRAINT_TABLE_INDEX_OID, constraints_table_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CONSTRAINT_TABLE_OID,
                            postgres::CONSTRAINT_INDEX_INDEX_OID, "pg_constraint_index_index",
                            postgres::Builder::GetConstraintIndexIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CONSTRAINT_INDEX_INDEX_OID, constraints_index_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::CONSTRAINT_TABLE_OID,
                            postgres::CONSTRAINT_FOREIGNTABLE_INDEX_OID, "pg_constraint_foreigntable_index",
                            postgres::Builder::GetConstraintForeignTableIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::CONSTRAINT_FOREIGNTABLE_INDEX_OID, constraints_foreigntable_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  // pg_language and associated indexes
  retval = CreateTableEntry(txn, postgres::LANGUAGE_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_language",
                            postgres::Builder::GetLanguageTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::LANGUAGE_TABLE_OID, languages_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::LANGUAGE_TABLE_OID,
                            postgres::LANGUAGE_OID_INDEX_OID, "pg_languages_oid_index",
                            postgres::Builder::GetLanguageOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::LANGUAGE_OID_INDEX_OID, languages_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::LANGUAGE_TABLE_OID,
                            postgres::LANGUAGE_NAME_INDEX_OID, "pg_languages_name_index",
                            postgres::Builder::GetLanguageNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::LANGUAGE_NAME_INDEX_OID, languages_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapLanguages(txn);

  // pg_proc and associated indexes
  retval = CreateTableEntry(txn, postgres::PRO_TABLE_OID, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_proc",
                            postgres::Builder::GetProcTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetTablePointer(txn, postgres::PRO_TABLE_OID, procs_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::PRO_TABLE_OID,
                            postgres::PRO_OID_INDEX_OID, "pg_proc_oid_index",
                            postgres::Builder::GetProcOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::PRO_OID_INDEX_OID, procs_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = CreateIndexEntry(txn, postgres::NAMESPACE_CATALOG_NAMESPACE_OID, postgres::PRO_TABLE_OID,
                            postgres::PRO_NAME_INDEX_OID, "pg_proc_name_index",
                            postgres::Builder::GetProcNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = SetIndexPointer(txn, postgres::PRO_NAME_INDEX_OID, procs_name_index_);

  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapProcs(txn);
}

void DatabaseCatalog::BootstrapPRIs() {
  // TODO(Matt): another potential optimization in the future would be to cache the offsets, rather than the maps
  // themselves (see TPC-C microbenchmark transactions for example). That seems premature right now though.

  // pg_namespace
  const std::vector<col_oid_t> pg_namespace_all_oids{postgres::PG_NAMESPACE_ALL_COL_OIDS.cbegin(),
                                                     postgres::PG_NAMESPACE_ALL_COL_OIDS.cend()};
  pg_namespace_all_cols_pri_ = namespaces_->InitializerForProjectedRow(pg_namespace_all_oids);
  pg_namespace_all_cols_prm_ = namespaces_->ProjectionMapForOids(pg_namespace_all_oids);

  const std::vector<col_oid_t> delete_namespace_oids{postgres::NSPNAME_COL_OID};
  delete_namespace_pri_ = namespaces_->InitializerForProjectedRow(delete_namespace_oids);

  const std::vector<col_oid_t> get_namespace_oids{postgres::NSPOID_COL_OID};
  get_namespace_pri_ = namespaces_->InitializerForProjectedRow(get_namespace_oids);

  // pg_attribute
  const std::vector<col_oid_t> pg_attribute_all_oids{postgres::PG_ATTRIBUTE_ALL_COL_OIDS.cbegin(),
                                                     postgres::PG_ATTRIBUTE_ALL_COL_OIDS.end()};
  pg_attribute_all_cols_pri_ = columns_->InitializerForProjectedRow(pg_attribute_all_oids);
  pg_attribute_all_cols_prm_ = columns_->ProjectionMapForOids(pg_attribute_all_oids);

  const std::vector<col_oid_t> get_columns_oids{postgres::ATTNUM_COL_OID,     postgres::ATTNAME_COL_OID,
                                                postgres::ATTTYPID_COL_OID,   postgres::ATTLEN_COL_OID,
                                                postgres::ATTNOTNULL_COL_OID, postgres::ADSRC_COL_OID};
  get_columns_pri_ = columns_->InitializerForProjectedRow(get_columns_oids);
  get_columns_prm_ = columns_->ProjectionMapForOids(get_columns_oids);

  const std::vector<col_oid_t> delete_columns_oids{postgres::ATTNUM_COL_OID, postgres::ATTNAME_COL_OID};
  delete_columns_pri_ = columns_->InitializerForProjectedRow(delete_columns_oids);
  delete_columns_prm_ = columns_->ProjectionMapForOids(delete_columns_oids);

  // pg_class
  const std::vector<col_oid_t> pg_class_all_oids{postgres::PG_CLASS_ALL_COL_OIDS.cbegin(),
                                                 postgres::PG_CLASS_ALL_COL_OIDS.cend()};
  pg_class_all_cols_pri_ = classes_->InitializerForProjectedRow(pg_class_all_oids);
  pg_class_all_cols_prm_ = classes_->ProjectionMapForOids(pg_class_all_oids);

  const std::vector<col_oid_t> get_class_oid_kind_oids{postgres::RELOID_COL_OID, postgres::RELKIND_COL_OID};
  get_class_oid_kind_pri_ = classes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> set_class_pointer_oids{postgres::REL_PTR_COL_OID};
  set_class_pointer_pri_ = classes_->InitializerForProjectedRow(set_class_pointer_oids);

  const std::vector<col_oid_t> set_class_schema_oids{postgres::REL_SCHEMA_COL_OID};
  set_class_schema_pri_ = classes_->InitializerForProjectedRow(set_class_schema_oids);

  const std::vector<col_oid_t> get_class_pointer_kind_oids{postgres::REL_PTR_COL_OID, postgres::RELKIND_COL_OID};
  get_class_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_schema_pointer_kind_oids{postgres::REL_SCHEMA_COL_OID,
                                                                  postgres::RELKIND_COL_OID};
  get_class_schema_pointer_kind_pri_ = classes_->InitializerForProjectedRow(get_class_schema_pointer_kind_oids);

  const std::vector<col_oid_t> get_class_object_and_schema_oids{postgres::REL_PTR_COL_OID,
                                                                postgres::REL_SCHEMA_COL_OID};
  get_class_object_and_schema_pri_ = classes_->InitializerForProjectedRow(get_class_object_and_schema_oids);
  get_class_object_and_schema_prm_ = classes_->ProjectionMapForOids(get_class_object_and_schema_oids);

  // pg_index
  const std::vector<col_oid_t> pg_index_all_oids{postgres::PG_INDEX_ALL_COL_OIDS.cbegin(),
                                                 postgres::PG_INDEX_ALL_COL_OIDS.cend()};
  pg_index_all_cols_pri_ = indexes_->InitializerForProjectedRow(pg_index_all_oids);
  pg_index_all_cols_prm_ = indexes_->ProjectionMapForOids(pg_index_all_oids);

  const std::vector<col_oid_t> get_indexes_oids{postgres::INDOID_COL_OID};
  get_indexes_pri_ = indexes_->InitializerForProjectedRow(get_class_oid_kind_oids);

  const std::vector<col_oid_t> delete_index_oids{postgres::INDOID_COL_OID, postgres::INDRELID_COL_OID};
  delete_index_pri_ = indexes_->InitializerForProjectedRow(delete_index_oids);
  delete_index_prm_ = indexes_->ProjectionMapForOids(delete_index_oids);

  // pg_type
  const std::vector<col_oid_t> pg_type_all_oids{postgres::PG_TYPE_ALL_COL_OIDS.cbegin(),
                                                postgres::PG_TYPE_ALL_COL_OIDS.cend()};
  pg_type_all_cols_pri_ = types_->InitializerForProjectedRow(pg_type_all_oids);
  pg_type_all_cols_prm_ = types_->ProjectionMapForOids(pg_type_all_oids);

  // pg_language
  const std::vector<col_oid_t> pg_language_all_oids{postgres::PG_LANGUAGE_ALL_COL_OIDS.cbegin(),
                                                    postgres::PG_LANGUAGE_ALL_COL_OIDS.cend()};
  pg_language_all_cols_pri_ = languages_->InitializerForProjectedRow(pg_language_all_oids);
  pg_language_all_cols_prm_ = languages_->ProjectionMapForOids(pg_language_all_oids);

  // pg_proc
  const std::vector<col_oid_t> pg_proc_all_oids{postgres::PG_PRO_ALL_COL_OIDS.cbegin(),
                                                postgres::PG_PRO_ALL_COL_OIDS.cend()};
  pg_proc_all_cols_pri_ = procs_->InitializerForProjectedRow(pg_proc_all_oids);
  pg_proc_all_cols_prm_ = procs_->ProjectionMapForOids(pg_proc_all_oids);

  const std::vector<col_oid_t> set_pg_proc_ptr_oids{postgres::PRO_CTX_PTR_COL_OID};
  pg_proc_ptr_pri_ = procs_->InitializerForProjectedRow(set_pg_proc_ptr_oids);
}

namespace_oid_t DatabaseCatalog::CreateNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                 const std::string &name) {
  if (!TryLock(txn)) return INVALID_NAMESPACE_OID;
  const namespace_oid_t ns_oid{next_oid_++};
  if (!CreateNamespace(txn, name, ns_oid)) {
    return INVALID_NAMESPACE_OID;
  }
  return ns_oid;
}

bool DatabaseCatalog::CreateNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const std::string &name, const namespace_oid_t ns_oid) {
  // Step 1: Insert into table
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);
  // Get & Fill Redo Record
  auto *const redo = txn->StageWrite(db_oid_, postgres::NAMESPACE_TABLE_OID, pg_namespace_all_cols_pri_);
  // Write the attributes in the Redo Record
  *(reinterpret_cast<namespace_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_namespace_all_cols_prm_[postgres::NSPOID_COL_OID]))) = ns_oid;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_namespace_all_cols_prm_[postgres::NSPNAME_COL_OID]))) = name_varlen;
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

bool DatabaseCatalog::DeleteNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const namespace_oid_t ns_oid) {
  if (!TryLock(txn)) return false;
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
  txn->StageDelete(db_oid_, postgres::NAMESPACE_TABLE_OID, tuple_slot);
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
    if (object.second == postgres::ClassKind::REGULAR_TABLE) {
      result = DeleteTable(txn, static_cast<table_oid_t>(object.first));
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
    if (object.second == postgres::ClassKind::INDEX) {
      result = DeleteIndex(txn, static_cast<index_oid_t>(object.first));
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

namespace_oid_t DatabaseCatalog::GetNamespaceOid(const common::ManagedPointer<transaction::TransactionContext> txn,
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

template <typename Column, typename ClassOid, typename ColOid>
bool DatabaseCatalog::CreateColumn(const common::ManagedPointer<transaction::TransactionContext> txn,
                                   const ClassOid class_oid, const ColOid col_oid, const Column &col) {
  // Step 1: Insert into the table
  auto *const redo = txn->StageWrite(db_oid_, postgres::COLUMN_TABLE_OID, pg_attribute_all_cols_pri_);
  // Write the attributes in the Redo Record
  auto oid_entry = reinterpret_cast<ColOid *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ATTNUM_COL_OID]));
  auto relid_entry = reinterpret_cast<ClassOid *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ATTRELID_COL_OID]));
  auto name_entry = reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ATTNAME_COL_OID]));
  auto type_entry = reinterpret_cast<type::TypeId *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ATTTYPID_COL_OID]));
  auto len_entry = reinterpret_cast<uint16_t *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ATTLEN_COL_OID]));
  auto notnull_entry = reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ATTNOTNULL_COL_OID]));
  auto dsrc_entry = reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_attribute_all_cols_prm_[postgres::ADSRC_COL_OID]));
  *oid_entry = col_oid;
  *relid_entry = class_oid;
  const auto name_varlen = storage::StorageUtil::CreateVarlen(col.Name());

  *name_entry = name_varlen;
  *type_entry = col.Type();
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
std::vector<Column> DatabaseCatalog::GetColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
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
bool DatabaseCatalog::DeleteColumns(const common::ManagedPointer<transaction::TransactionContext> txn,
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
        pr->AccessWithNullCheck(delete_columns_prm_[postgres::ATTNAME_COL_OID]));
    NOISEPAGE_ASSERT(col_name != nullptr, "Name shouldn't be NULL.");
    const auto *const col_oid =
        reinterpret_cast<const uint32_t *const>(pr->AccessWithNullCheck(delete_columns_prm_[postgres::ATTNUM_COL_OID]));
    NOISEPAGE_ASSERT(col_oid != nullptr, "OID shouldn't be NULL.");

    // 2. Delete from the table
    txn->StageDelete(db_oid_, postgres::COLUMN_TABLE_OID, slot);
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

table_oid_t DatabaseCatalog::CreateTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                                         const namespace_oid_t ns, const std::string &name, const Schema &schema) {
  if (!TryLock(txn)) return INVALID_TABLE_OID;
  const table_oid_t table_oid = static_cast<table_oid_t>(next_oid_++);

  return CreateTableEntry(txn, table_oid, ns, name, schema) ? table_oid : INVALID_TABLE_OID;
}

bool DatabaseCatalog::DeleteIndexes(const common::ManagedPointer<transaction::TransactionContext> txn,
                                    const table_oid_t table) {
  if (!TryLock(txn)) return false;
  // Get the indexes
  const auto index_oids = GetIndexOids(txn, table);
  // Delete all indexes
  for (const auto index_oid : index_oids) {
    auto result = DeleteIndex(txn, index_oid);
    if (!result) {
      // write-write conflict. Someone beat us to this operation.
      return false;
    }
  }
  return true;
}

bool DatabaseCatalog::DeleteTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const table_oid_t table) {
  if (!TryLock(txn)) return false;
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
  txn->StageDelete(db_oid_, postgres::CLASS_TABLE_OID, index_results[0]);
  result = classes_->Delete(txn, index_results[0]);
  if (!result) {
    // write-write conflict. Someone beat us to this operation.
    delete[] buffer;
    return false;
  }

  DeleteIndexes(txn, table);

  // Get the attributes we need for indexes
  const table_oid_t table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::RELOID_COL_OID])));
  NOISEPAGE_ASSERT(table == table_oid,
                   "table oid from pg_classes did not match what was found by the index scan from the argument.");
  const namespace_oid_t ns_oid = *(reinterpret_cast<const namespace_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::RELNAMESPACE_COL_OID])));
  const storage::VarlenEntry name_varlen = *(reinterpret_cast<const storage::VarlenEntry *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::RELNAME_COL_OID])));

  // Get the attributes we need for delete
  auto *const schema_ptr = *(reinterpret_cast<const Schema *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::REL_SCHEMA_COL_OID])));
  auto *const table_ptr = *(reinterpret_cast<storage::SqlTable *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::REL_PTR_COL_OID])));

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

std::pair<uint32_t, postgres::ClassKind> DatabaseCatalog::GetClassOidKind(
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
    return std::make_pair(catalog::NULL_OID, postgres::ClassKind::REGULAR_TABLE);
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
  const auto kind = *(reinterpret_cast<const postgres::ClassKind *const>(pr->AccessForceNotNull(1)));

  // Finish
  delete[] buffer;
  return std::make_pair(oid, kind);
}

table_oid_t DatabaseCatalog::GetTableOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                         const namespace_oid_t ns, const std::string &name) {
  const auto oid_pair = GetClassOidKind(txn, ns, name);
  if (oid_pair.first == catalog::NULL_OID || oid_pair.second != postgres::ClassKind::REGULAR_TABLE) {
    // User called GetTableOid on an object that doesn't have type REGULAR_TABLE
    return INVALID_TABLE_OID;
  }
  return table_oid_t(oid_pair.first);
}

bool DatabaseCatalog::SetTablePointer(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const table_oid_t table, const storage::SqlTable *const table_ptr) {
  NOISEPAGE_ASSERT(
      write_lock_.load() == txn->FinishTime(),
      "Setting the object's pointer should only be done after successful DDL change request. i.e. this txn "
      "should already have the lock.");
  // We need to double-defer the deletion because there may be subsequent undo records into this table that need to be
  // GCed before we can safely delete this.  Specifically, the following ordering results in a use-after-free when the
  // unlink step dereferences a deleted SqlTable if the delete is only a single deferral:
  //
  //            Txn           |          Log Manager           |    GC
  // ---------------------------------------------------------------------------
  // CreateDatabase           |                                |
  // ABORT                    |                                |
  // Execute abort actions    |                                |
  //                          |                                | ENTER
  // Checkout ABORT timestamp |                                |
  //                          | Remove ABORT from running txns |
  //                          |                                | Read oldest running timestamp
  //                          |                                | Unlink (not unlinked because abort is "visible")
  //                          |                                | Process defers (deletes table)
  //                          |                                | EXIT
  //                          |                                | ENTER
  //                          |                                | Unlink (ASAN crashes process for use-after-free)
  //
  // TODO(John,Ling): This needs to become a triple deferral when DAF gets merged in order to maintain
  // assurances about object lifetimes in a multi-threaded GC situation.
  txn->RegisterAbortAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction(
        [=]() { deferred_action_manager->RegisterDeferredAction([=]() { delete table_ptr; }); });
  });
  return SetClassPointer(txn, table, table_ptr, postgres::REL_PTR_COL_OID);
}

/**
 * Obtain the storage pointer for a SQL table
 * @param table to which we want the storage object
 * @return the storage object corresponding to the passed OID
 */
common::ManagedPointer<storage::SqlTable> DatabaseCatalog::GetTable(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t table) {
  const auto ptr_pair = GetClassPtrKind(txn, table.UnderlyingValue());
  if (ptr_pair.second != postgres::ClassKind::REGULAR_TABLE) {
    // User called GetTable with an OID for an object that doesn't have type REGULAR_TABLE
    return common::ManagedPointer<storage::SqlTable>(nullptr);
  }
  return common::ManagedPointer(reinterpret_cast<storage::SqlTable *>(ptr_pair.first));
}

bool DatabaseCatalog::RenameTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const table_oid_t table, const std::string &name) {
  if (!TryLock(txn)) return false;
  // TODO(John): Implement
  NOISEPAGE_ASSERT(false, "Not implemented");
  return false;
}

bool DatabaseCatalog::UpdateSchema(const common::ManagedPointer<transaction::TransactionContext> txn,
                                   const table_oid_t table, Schema *const new_schema) {
  if (!TryLock(txn)) return false;
  // TODO(John): Implement
  NOISEPAGE_ASSERT(false, "Not implemented");
  return false;
}

const Schema &DatabaseCatalog::GetSchema(const common::ManagedPointer<transaction::TransactionContext> txn,
                                         const table_oid_t table) {
  const auto ptr_pair = GetClassSchemaPtrKind(txn, table.UnderlyingValue());
  NOISEPAGE_ASSERT(ptr_pair.first != nullptr, "Schema pointer shouldn't ever be NULL under current catalog semantics.");
  NOISEPAGE_ASSERT(ptr_pair.second == postgres::ClassKind::REGULAR_TABLE, "Requested a table schema for a non-table");
  return *reinterpret_cast<Schema *>(ptr_pair.first);
}

std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(
    const common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) {
  // TODO(John): Implement
  NOISEPAGE_ASSERT(false, "Not implemented");
  return {};
}

std::vector<index_oid_t> DatabaseCatalog::GetIndexOids(
    const common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table) {
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

index_oid_t DatabaseCatalog::CreateIndex(const common::ManagedPointer<transaction::TransactionContext> txn,
                                         namespace_oid_t ns, const std::string &name, table_oid_t table,
                                         const IndexSchema &schema) {
  if (!TryLock(txn)) return INVALID_INDEX_OID;
  const index_oid_t index_oid = static_cast<index_oid_t>(next_oid_++);
  return CreateIndexEntry(txn, ns, table, index_oid, name, schema) ? index_oid : INVALID_INDEX_OID;
}

bool DatabaseCatalog::DeleteIndex(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  index_oid_t index) {
  if (!TryLock(txn)) return false;
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
  txn->StageDelete(db_oid_, postgres::CLASS_TABLE_OID, index_results[0]);
  result = classes_->Delete(txn, index_results[0]);
  if (!result) {
    // write-write conflict. Someone beat us to this operation.
    delete[] buffer;
    return false;
  }

  // Get the attributes we need for pg_class indexes
  table_oid_t table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::RELOID_COL_OID])));
  const namespace_oid_t ns_oid = *(reinterpret_cast<const namespace_oid_t *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::RELNAMESPACE_COL_OID])));
  const storage::VarlenEntry name_varlen = *(reinterpret_cast<const storage::VarlenEntry *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::RELNAME_COL_OID])));

  auto *const schema_ptr = *(reinterpret_cast<const IndexSchema *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::REL_SCHEMA_COL_OID])));
  auto *const index_ptr = *(reinterpret_cast<storage::index::Index *const *const>(
      table_pr->AccessForceNotNull(pg_class_all_cols_prm_[postgres::REL_PTR_COL_OID])));

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
                                table_pr->AccessForceNotNull(delete_index_prm_[postgres::INDOID_COL_OID]))),
                   "index oid from pg_index did not match what was found by the index scan from the argument.");

  // Delete from pg_index table
  txn->StageDelete(db_oid_, postgres::INDEX_TABLE_OID, index_results[0]);
  result = indexes_->Delete(txn, index_results[0]);
  NOISEPAGE_ASSERT(
      result,
      "Delete from pg_index should always succeed as write-write conflicts are detected during delete from pg_class");

  // Get the table oid
  table_oid = *(reinterpret_cast<const table_oid_t *const>(
      table_pr->AccessForceNotNull(delete_index_prm_[postgres::INDRELID_COL_OID])));

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
      [=, garbage_collector{garbage_collector_}](transaction::DeferredActionManager *deferred_action_manager) {
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

bool DatabaseCatalog::SetTableSchemaPointer(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const table_oid_t oid, const Schema *const schema) {
  return SetClassPointer(txn, oid, schema, postgres::REL_SCHEMA_COL_OID);
}

bool DatabaseCatalog::SetIndexSchemaPointer(const common::ManagedPointer<transaction::TransactionContext> txn,
                                            const index_oid_t oid, const IndexSchema *const schema) {
  return SetClassPointer(txn, oid, schema, postgres::REL_SCHEMA_COL_OID);
}

template <typename ClassOid, typename Ptr>
bool DatabaseCatalog::SetClassPointer(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const ClassOid oid, const Ptr *const pointer, const col_oid_t class_col) {
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

  auto &initializer =
      (class_col == catalog::postgres::REL_PTR_COL_OID) ? set_class_pointer_pri_ : set_class_schema_pri_;
  auto *update_redo = txn->StageWrite(db_oid_, postgres::CLASS_TABLE_OID, initializer);
  update_redo->SetTupleSlot(index_results[0]);
  auto *update_pr = update_redo->Delta();
  auto *const class_ptr_ptr = update_pr->AccessForceNotNull(0);
  *(reinterpret_cast<const Ptr **>(class_ptr_ptr)) = pointer;

  // Finish
  delete[] buffer;
  return classes_->Update(txn, update_redo);
}

bool DatabaseCatalog::SetIndexPointer(const common::ManagedPointer<transaction::TransactionContext> txn,
                                      const index_oid_t index, storage::index::Index *const index_ptr) {
  NOISEPAGE_ASSERT(
      write_lock_.load() == txn->FinishTime(),
      "Setting the object's pointer should only be done after successful DDL change request. i.e. this txn "
      "should already have the lock.");
  if (index_ptr->Type() == storage::index::IndexType::BWTREE) {
    garbage_collector_->RegisterIndexForGC(common::ManagedPointer(index_ptr));
  }
  // This needs to be deferred because if any items were subsequently inserted into this index, they will have deferred
  // abort actions that will be above this action on the abort stack.  The defer ensures we execute after them.
  txn->RegisterAbortAction(
      [=, garbage_collector{garbage_collector_}](transaction::DeferredActionManager *deferred_action_manager) {
        if (index_ptr->Type() == storage::index::IndexType::BWTREE) {
          garbage_collector->UnregisterIndexForGC(common::ManagedPointer(index_ptr));
        }
        deferred_action_manager->RegisterDeferredAction([=]() { delete index_ptr; });
      });
  return SetClassPointer(txn, index, index_ptr, postgres::REL_PTR_COL_OID);
}

common::ManagedPointer<storage::index::Index> DatabaseCatalog::GetIndex(
    const common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index) {
  const auto ptr_pair = GetClassPtrKind(txn, index.UnderlyingValue());
  if (ptr_pair.second != postgres::ClassKind::INDEX) {
    // User called GetTable with an OID for an object that doesn't have type REGULAR_TABLE
    return common::ManagedPointer<storage::index::Index>(nullptr);
  }
  return common::ManagedPointer(reinterpret_cast<storage::index::Index *>(ptr_pair.first));
}

index_oid_t DatabaseCatalog::GetIndexOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                         namespace_oid_t ns, const std::string &name) {
  const auto oid_pair = GetClassOidKind(txn, ns, name);
  if (oid_pair.first == NULL_OID || oid_pair.second != postgres::ClassKind::INDEX) {
    // User called GetIndexOid on an object that doesn't have type INDEX
    return INVALID_INDEX_OID;
  }
  return index_oid_t(oid_pair.first);
}

const IndexSchema &DatabaseCatalog::GetIndexSchema(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                   index_oid_t index) {
  auto ptr_pair = GetClassSchemaPtrKind(txn, index.UnderlyingValue());
  NOISEPAGE_ASSERT(ptr_pair.first != nullptr, "Schema pointer shouldn't ever be NULL under current catalog semantics.");
  NOISEPAGE_ASSERT(ptr_pair.second == postgres::ClassKind::INDEX, "Requested an index schema for a non-index");
  return *reinterpret_cast<IndexSchema *>(ptr_pair.first);
}

std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> DatabaseCatalog::GetIndexes(
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
        class_select_pr->AccessForceNotNull(get_class_object_and_schema_prm_[catalog::postgres::REL_PTR_COL_OID])));
    NOISEPAGE_ASSERT(
        index != nullptr,
        "Catalog conventions say you should not find a nullptr for an object ptr in pg_class. Did you call "
        "SetIndexPointer?");
    auto *schema = *(reinterpret_cast<catalog::IndexSchema *const *const>(
        class_select_pr->AccessForceNotNull(get_class_object_and_schema_prm_[catalog::postgres::REL_SCHEMA_COL_OID])));
    NOISEPAGE_ASSERT(schema != nullptr,
                     "Catalog conventions say you should not find a nullptr for an schema ptr in pg_class");

    index_objects.emplace_back(common::ManagedPointer(index), *schema);
  }
  delete[] buffer;
  return index_objects;
}

void DatabaseCatalog::TearDown(const common::ManagedPointer<transaction::TransactionContext> txn) {
  std::vector<parser::AbstractExpression *> expressions;
  std::vector<Schema *> table_schemas;
  std::vector<storage::SqlTable *> tables;
  std::vector<IndexSchema *> index_schemas;
  std::vector<storage::index::Index *> indexes;
  std::vector<execution::functions::FunctionContext *> func_contexts;

  // pg_class (schemas & objects) [this is the largest projection]
  const std::vector<col_oid_t> pg_class_oids{postgres::RELKIND_COL_OID, postgres::REL_SCHEMA_COL_OID,
                                             postgres::REL_PTR_COL_OID};

  auto pci = classes_->InitializerForProjectedColumns(pg_class_oids, 100);
  auto pm = classes_->ProjectionMapForOids(pg_class_oids);

  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  // Fetch pointers to the start each in the projected columns
  auto classes = reinterpret_cast<postgres::ClassKind *>(pc->ColumnStart(pm[postgres::RELKIND_COL_OID]));
  auto schemas = reinterpret_cast<void **>(pc->ColumnStart(pm[postgres::REL_SCHEMA_COL_OID]));
  auto objects = reinterpret_cast<void **>(pc->ColumnStart(pm[postgres::REL_PTR_COL_OID]));

  // Scan the table
  auto table_iter = classes_->begin();
  while (table_iter != classes_->end()) {
    classes_->Scan(txn, &table_iter, pc);
    for (uint i = 0; i < pc->NumTuples(); i++) {
      NOISEPAGE_ASSERT(objects[i] != nullptr, "Pointer to objects in pg_class should not be nullptr");
      NOISEPAGE_ASSERT(schemas[i] != nullptr, "Pointer to schemas in pg_class should not be nullptr");
      switch (classes[i]) {
        case postgres::ClassKind::REGULAR_TABLE:
          table_schemas.emplace_back(reinterpret_cast<Schema *>(schemas[i]));
          tables.emplace_back(reinterpret_cast<storage::SqlTable *>(objects[i]));
          break;
        case postgres::ClassKind::INDEX:
          index_schemas.emplace_back(reinterpret_cast<IndexSchema *>(schemas[i]));
          indexes.emplace_back(reinterpret_cast<storage::index::Index *>(objects[i]));
          break;
        default:
          throw std::runtime_error("Unimplemented destructor needed");
      }
    }
  }

  // pg_constraint (expressions)
  const std::vector<col_oid_t> pg_constraint_oids{postgres::CONBIN_COL_OID};
  pci = constraints_->InitializerForProjectedColumns(pg_constraint_oids, 100);
  pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>(pc->ColumnStart(0));

  table_iter = constraints_->begin();
  while (table_iter != constraints_->end()) {
    constraints_->Scan(txn, &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // pg_proc (func_contexts)
  const std::vector<col_oid_t> pg_proc_contexts{postgres::PRO_CTX_PTR_COL_OID};
  pci = procs_->InitializerForProjectedColumns(pg_proc_contexts, 100);
  pc = pci.Initialize(buffer);

  auto ctxts = reinterpret_cast<execution::functions::FunctionContext **>(pc->ColumnStart(0));

  table_iter = procs_->begin();
  while (table_iter != procs_->end()) {
    procs_->Scan(txn, &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) {
      if (ctxts[i] == nullptr) {
        continue;
      }
      func_contexts.emplace_back(ctxts[i]);
    }
  }

  auto dbc_nuke = [=, garbage_collector{garbage_collector_}, tables{std::move(tables)}, indexes{std::move(indexes)},
                   table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schemas)},
                   expressions{std::move(expressions)}, func_contexts{std::move(func_contexts)}]() {
    for (auto table : tables) delete table;

    for (auto index : indexes) {
      if (index->Type() == storage::index::IndexType::BWTREE) {
        garbage_collector->UnregisterIndexForGC(common::ManagedPointer(index));
      }
      delete index;
    }

    for (auto schema : table_schemas) delete schema;

    for (auto schema : index_schemas) delete schema;

    for (auto expr : expressions) delete expr;

    for (auto func_ctxt : func_contexts) delete func_ctxt;
  };

  // No new transactions can see these object but there may be deferred index
  // and other operation.  Therefore, we need to defer the deallocation on delete
  txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction(dbc_nuke);
  });

  delete[] buffer;
}

bool DatabaseCatalog::CreateIndexEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                       const namespace_oid_t ns_oid, const table_oid_t table_oid,
                                       const index_oid_t index_oid, const std::string &name,
                                       const IndexSchema &schema) {
  // First, insert into pg_class
  auto *const class_insert_redo = txn->StageWrite(db_oid_, postgres::CLASS_TABLE_OID, pg_class_all_cols_pri_);
  auto *const class_insert_pr = class_insert_redo->Delta();

  // Write the index_oid into the PR
  auto index_oid_offset = pg_class_all_cols_prm_[postgres::RELOID_COL_OID];
  auto *index_oid_ptr = class_insert_pr->AccessForceNotNull(index_oid_offset);
  *(reinterpret_cast<index_oid_t *>(index_oid_ptr)) = index_oid;

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Write the name into the PR
  const auto name_offset = pg_class_all_cols_prm_[postgres::RELNAME_COL_OID];
  auto *const name_ptr = class_insert_pr->AccessForceNotNull(name_offset);
  *(reinterpret_cast<storage::VarlenEntry *>(name_ptr)) = name_varlen;

  // Write the ns_oid into the PR
  const auto ns_offset = pg_class_all_cols_prm_[postgres::RELNAMESPACE_COL_OID];
  auto *const ns_ptr = class_insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<namespace_oid_t *>(ns_ptr)) = ns_oid;

  // Write the kind into the PR
  const auto kind_offset = pg_class_all_cols_prm_[postgres::RELKIND_COL_OID];
  auto *const kind_ptr = class_insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<postgres::ClassKind *>(kind_ptr)) = postgres::ClassKind::INDEX;

  // Write the index_schema_ptr into the PR
  const auto index_schema_ptr_offset = pg_class_all_cols_prm_[postgres::REL_SCHEMA_COL_OID];
  auto *const index_schema_ptr_ptr = class_insert_pr->AccessForceNotNull(index_schema_ptr_offset);
  *(reinterpret_cast<IndexSchema **>(index_schema_ptr_ptr)) = nullptr;

  // Set next_col_oid to NULL because indexes don't need col_oid
  const auto next_col_oid_offset = pg_class_all_cols_prm_[postgres::REL_NEXTCOLOID_COL_OID];
  class_insert_pr->SetNull(next_col_oid_offset);

  // Set index_ptr to NULL because it gets set by execution layer after instantiation
  const auto index_ptr_offset = pg_class_all_cols_prm_[postgres::REL_PTR_COL_OID];
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

  auto *const indexes_insert_redo = txn->StageWrite(db_oid_, postgres::INDEX_TABLE_OID, pg_index_all_cols_pri_);
  auto *const indexes_insert_pr = indexes_insert_redo->Delta();

  // Write the index_oid into the PR
  index_oid_offset = pg_index_all_cols_prm_[postgres::INDOID_COL_OID];
  index_oid_ptr = indexes_insert_pr->AccessForceNotNull(index_oid_offset);
  *(reinterpret_cast<index_oid_t *>(index_oid_ptr)) = index_oid;

  // Write the table_oid for the table the index is for into the PR
  const auto rel_oid_offset = pg_index_all_cols_prm_[postgres::INDRELID_COL_OID];
  auto *const rel_oid_ptr = indexes_insert_pr->AccessForceNotNull(rel_oid_offset);
  *(reinterpret_cast<table_oid_t *>(rel_oid_ptr)) = table_oid;

  // Write boolean values to PR
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[postgres::INDISUNIQUE_COL_OID]))) = schema.is_unique_;
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[postgres::INDISPRIMARY_COL_OID]))) = schema.is_primary_;
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[postgres::INDISEXCLUSION_COL_OID]))) = schema.is_exclusion_;
  *(reinterpret_cast<bool *>(indexes_insert_pr->AccessForceNotNull(
      pg_index_all_cols_prm_[postgres::INDIMMEDIATE_COL_OID]))) = schema.is_immediate_;
  // TODO(Matt): these should actually be set later based on runtime information about the index. @yeshengm
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[postgres::INDISVALID_COL_OID]))) = true;
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[postgres::INDISREADY_COL_OID]))) = true;
  *(reinterpret_cast<bool *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[postgres::INDISLIVE_COL_OID]))) = true;
  *(reinterpret_cast<storage::index::IndexType *>(
      indexes_insert_pr->AccessForceNotNull(pg_index_all_cols_prm_[postgres::IND_TYPE_COL_OID]))) = schema.type_;

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

  auto *const update_redo = txn->StageWrite(db_oid_, postgres::CLASS_TABLE_OID, set_class_schema_pri_);
  auto *const update_pr = update_redo->Delta();

  update_redo->SetTupleSlot(class_tuple_slot);
  *reinterpret_cast<IndexSchema **>(update_pr->AccessForceNotNull(0)) = new_schema;
  auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
  NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail");

  return true;
}

type_oid_t DatabaseCatalog::GetTypeOidForType(const type::TypeId type) {
  return type_oid_t(static_cast<uint8_t>(type));
}

void DatabaseCatalog::InsertType(const common::ManagedPointer<transaction::TransactionContext> txn, type_oid_t type_oid,
                                 const std::string &name, const namespace_oid_t namespace_oid, const int16_t len,
                                 bool by_val, const postgres::Type type_category) {
  // Stage the write into the table
  auto redo_record = txn->StageWrite(db_oid_, postgres::TYPE_TABLE_OID, pg_type_all_cols_pri_);
  auto *delta = redo_record->Delta();

  // Populate oid
  auto offset = pg_type_all_cols_prm_[postgres::TYPOID_COL_OID];
  *(reinterpret_cast<type_oid_t *>(delta->AccessForceNotNull(offset))) = type_oid;

  // Populate type name
  offset = pg_type_all_cols_prm_[postgres::TYPNAME_COL_OID];
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  *(reinterpret_cast<storage::VarlenEntry *>(delta->AccessForceNotNull(offset))) = name_varlen;

  // Populate namespace
  offset = pg_type_all_cols_prm_[postgres::TYPNAMESPACE_COL_OID];
  *(reinterpret_cast<namespace_oid_t *>(delta->AccessForceNotNull(offset))) = namespace_oid;

  // Populate len
  offset = pg_type_all_cols_prm_[postgres::TYPLEN_COL_OID];
  *(reinterpret_cast<int16_t *>(delta->AccessForceNotNull(offset))) = len;

  // Populate byval
  offset = pg_type_all_cols_prm_[postgres::TYPBYVAL_COL_OID];
  *(reinterpret_cast<bool *>(delta->AccessForceNotNull(offset))) = by_val;

  // Populate type
  offset = pg_type_all_cols_prm_[postgres::TYPTYPE_COL_OID];
  auto type = static_cast<uint8_t>(type_category);
  *(reinterpret_cast<uint8_t *>(delta->AccessForceNotNull(offset))) = type;

  // Insert into table
  auto tuple_slot = types_->Insert(txn, redo_record);

  // Allocate buffer of largest size needed
  NOISEPAGE_ASSERT((types_name_index_->GetProjectedRowInitializer().ProjectedRowSize() >=
                    types_oid_index_->GetProjectedRowInitializer().ProjectedRowSize()) &&
                       (types_name_index_->GetProjectedRowInitializer().ProjectedRowSize() >=
                        types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize()),
                   "Buffer must be allocated for largest ProjectedRow size");
  byte *buffer =
      common::AllocationUtil::AllocateAligned(types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());

  // Insert into oid index
  auto oid_index_delta = types_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto oid_index_offset = types_oid_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  *(reinterpret_cast<uint32_t *>(oid_index_delta->AccessForceNotNull(oid_index_offset))) = type_oid.UnderlyingValue();
  auto result UNUSED_ATTRIBUTE = types_oid_index_->InsertUnique(txn, *oid_index_delta, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insert into type oid index should always succeed");

  // Insert into (namespace_oid, name) index
  auto name_index_delta = types_name_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  // Populate namespace
  auto name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  *(reinterpret_cast<uint32_t *>(name_index_delta->AccessForceNotNull(name_index_offset))) =
      namespace_oid.UnderlyingValue();
  // Populate type name
  name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(2));
  *(reinterpret_cast<storage::VarlenEntry *>(name_index_delta->AccessForceNotNull(name_index_offset))) = name_varlen;
  result = types_name_index_->InsertUnique(txn, *name_index_delta, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insert into type name index should always succeed");

  // Insert into (non-unique) namespace oid index
  auto namespace_index_delta = types_namespace_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto namespace_index_offset = types_namespace_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  *(reinterpret_cast<uint32_t *>(namespace_index_delta->AccessForceNotNull(namespace_index_offset))) =
      namespace_oid.UnderlyingValue();
  result = types_namespace_index_->Insert(txn, *name_index_delta, tuple_slot);
  NOISEPAGE_ASSERT(result, "Insert into type namespace index should always succeed");

  delete[] buffer;
}

void DatabaseCatalog::InsertType(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 type::TypeId internal_type, const std::string &name,
                                 const namespace_oid_t namespace_oid, const int16_t len, bool by_val,
                                 const postgres::Type type_category) {
  auto type_oid = GetTypeOidForType(internal_type);
  InsertType(txn, type_oid, name, namespace_oid, len, by_val, type_category);
}

void DatabaseCatalog::BootstrapTypes(const common::ManagedPointer<transaction::TransactionContext> txn) {
  InsertType(txn, type::TypeId::INVALID, "invalid", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, 1, true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::BOOLEAN, "boolean", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(bool), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TINYINT, "tinyint", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int8_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::SMALLINT, "smallint", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int16_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::INTEGER, "integer", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int32_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::BIGINT, "bigint", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int64_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::DECIMAL, "decimal", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(double), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TIMESTAMP, "timestamp", postgres::NAMESPACE_CATALOG_NAMESPACE_OID,
             sizeof(type::timestamp_t), true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::DATE, "date", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::date_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARCHAR, "varchar", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARBINARY, "varbinary", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             postgres::Type::BASE);

  InsertType(txn, postgres::VAR_ARRAY_OID, "var_array", postgres::NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             postgres::Type::COMPOSITE);
}

void DatabaseCatalog::BootstrapLanguages(const common::ManagedPointer<transaction::TransactionContext> txn) {
  CreateLanguage(txn, "plpgsql", postgres::PLPGSQL_LANGUAGE_OID);
  CreateLanguage(txn, "internal", postgres::INTERNAL_LANGUAGE_OID);
}

void DatabaseCatalog::BootstrapProcs(const common::ManagedPointer<transaction::TransactionContext> txn) {
  auto dec_type = GetTypeOidForType(type::TypeId::DECIMAL);
  auto int_type = GetTypeOidForType(type::TypeId::INTEGER);

  CreateProcedure(txn, postgres::EXP_PRO_OID, "exp", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num"}, {dec_type}, {dec_type}, {}, dec_type, "", true);

  CreateProcedure(txn, postgres::ATAN2_PRO_OID, "atan2", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type}, {dec_type, dec_type}, {},
                  dec_type, "", true);

  CreateProcedure(txn, postgres::ABS_REAL_PRO_OID, "abs", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y"}, {dec_type}, {dec_type}, {}, dec_type, "", true);

  CreateProcedure(txn, postgres::ABS_INT_PRO_OID, "abs", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y"}, {int_type}, {int_type}, {}, int_type, "", true);

  CreateProcedure(txn, postgres::MOD_PRO_OID, "mod", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type}, {dec_type, dec_type}, {},
                  dec_type, "", true);

  CreateProcedure(txn, postgres::INTMOD_PRO_OID, "mod", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {int_type, int_type}, {int_type, int_type}, {},
                  int_type, "", true);

  CreateProcedure(txn, postgres::ROUND2_PRO_OID, "round", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, int_type}, {dec_type, int_type}, {},
                  dec_type, "", true);

  CreateProcedure(txn, postgres::POW_PRO_OID, "pow", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"y", "x"}, {dec_type, dec_type}, {dec_type, dec_type}, {},
                  dec_type, "", true);

#define BOOTSTRAP_TRIG_FN(str_name, pro_oid, builtin)                                                                 \
  CreateProcedure(txn, pro_oid, str_name, postgres::INTERNAL_LANGUAGE_OID, postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, \
                  {"theta"}, {dec_type}, {dec_type}, {}, dec_type, "", true);

  BOOTSTRAP_TRIG_FN("acos", postgres::ACOS_PRO_OID, execution::ast::Builtin::ACos)

  BOOTSTRAP_TRIG_FN("asin", postgres::ASIN_PRO_OID, execution::ast::Builtin::ASin)

  BOOTSTRAP_TRIG_FN("atan", postgres::ATAN_PRO_OID, execution::ast::Builtin::ATan)

  BOOTSTRAP_TRIG_FN("cos", postgres::COS_PRO_OID, execution::ast::Builtin::Cos)

  BOOTSTRAP_TRIG_FN("sin", postgres::SIN_PRO_OID, execution::ast::Builtin::Sin)

  BOOTSTRAP_TRIG_FN("tan", postgres::TAN_PRO_OID, execution::ast::Builtin::Tan)

  BOOTSTRAP_TRIG_FN("cosh", postgres::COSH_PRO_OID, execution::ast::Builtin::Cosh)

  BOOTSTRAP_TRIG_FN("sinh", postgres::SINH_PRO_OID, execution::ast::Builtin::Sinh)

  BOOTSTRAP_TRIG_FN("tanh", postgres::TANH_PRO_OID, execution::ast::Builtin::Tanh)

  BOOTSTRAP_TRIG_FN("cot", postgres::COT_PRO_OID, execution::ast::Builtin::Cot)

  BOOTSTRAP_TRIG_FN("ceil", postgres::CEIL_PRO_OID, execution::ast::Builtin::Ceil)

  BOOTSTRAP_TRIG_FN("floor", postgres::FLOOR_PRO_OID, execution::ast::Builtin::Floor)

  BOOTSTRAP_TRIG_FN("truncate", postgres::TRUNCATE_PRO_OID, execution::ast::Builtin::Truncate)

  BOOTSTRAP_TRIG_FN("log10", postgres::LOG10_PRO_OID, execution::ast::Builtin::Log10)

  BOOTSTRAP_TRIG_FN("log2", postgres::LOG2_PRO_OID, execution::ast::Builtin::Log2)

  BOOTSTRAP_TRIG_FN("sqrt", postgres::SQRT_PRO_OID, execution::ast::Builtin::Sqrt)

  BOOTSTRAP_TRIG_FN("cbrt", postgres::CBRT_PRO_OID, execution::ast::Builtin::Cbrt)

  BOOTSTRAP_TRIG_FN("round", postgres::ROUND_PRO_OID, execution::ast::Builtin::Round)

#undef BOOTSTRAP_TRIG_FN

  const auto str_type = GetTypeOidForType(type::TypeId::VARCHAR);
  const auto real_type = GetTypeOidForType(type::TypeId::DECIMAL);
  const auto date_type = GetTypeOidForType(type::TypeId::DATE);
  const auto bool_type = GetTypeOidForType(type::TypeId::BOOLEAN);
  const auto variadic_type = GetTypeOidForType(type::TypeId::VARIADIC);

  CreateProcedure(
      txn, postgres::NP_RUNNERS_EMIT_INT_PRO_OID, "nprunnersemitint", postgres::INTERNAL_LANGUAGE_OID,
      postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"},
      {int_type, int_type, int_type, int_type}, {int_type, int_type, int_type, int_type},
      {postgres::ProArgModes::IN, postgres::ProArgModes::IN, postgres::ProArgModes::IN, postgres::ProArgModes::IN},
      int_type, "", false);

  CreateProcedure(
      txn, postgres::NP_RUNNERS_EMIT_REAL_PRO_OID, "nprunnersemitreal", postgres::INTERNAL_LANGUAGE_OID,
      postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num_tuples", "num_cols", "num_int_cols", "num_real_cols"},
      {int_type, int_type, int_type, int_type}, {int_type, int_type, int_type, int_type},
      {postgres::ProArgModes::IN, postgres::ProArgModes::IN, postgres::ProArgModes::IN, postgres::ProArgModes::IN},
      real_type, "", false);

  CreateProcedure(txn, postgres::NP_RUNNERS_DUMMY_INT_PRO_OID, "nprunnersdummyint", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, int_type, "", false);

  CreateProcedure(txn, postgres::NP_RUNNERS_DUMMY_REAL_PRO_OID, "nprunnersdummyreal", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, real_type, "", false);

  CreateProcedure(txn, postgres::ASCII_PRO_OID, "ascii", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "", true);

  CreateProcedure(txn, postgres::CHR_PRO_OID, "chr", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"num"}, {int_type}, {int_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::CHARLENGTH_PRO_OID, "char_length", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "", true);

  CreateProcedure(txn, postgres::LOWER_PRO_OID, "lower", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::UPPER_PRO_OID, "upper", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::INITCAP_PRO_OID, "initcap", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::VERSION_PRO_OID, "version", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {}, {}, {}, {}, str_type, "", false);

  CreateProcedure(txn, postgres::SPLIT_PART_PRO_OID, "split_part", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "delim", "field"}, {str_type, str_type, int_type},
                  {str_type, str_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::LENGTH_PRO_OID, "length", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, int_type, "", true);

  CreateProcedure(txn, postgres::STARTSWITH_PRO_OID, "starts_with", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "start"}, {str_type, str_type},
                  {str_type, str_type}, {}, bool_type, "", true);

  CreateProcedure(txn, postgres::SUBSTR_PRO_OID, "substr", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "pos", "len"}, {str_type, int_type, int_type},
                  {str_type, int_type, int_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::REVERSE_PRO_OID, "reverse", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::LEFT_PRO_OID, "left", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type}, {str_type, int_type},
                  {}, str_type, "", true);

  CreateProcedure(txn, postgres::RIGHT_PRO_OID, "right", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type}, {str_type, int_type},
                  {}, str_type, "", true);

  CreateProcedure(txn, postgres::REPEAT_PRO_OID, "repeat", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "int"}, {str_type, int_type}, {str_type, int_type},
                  {}, str_type, "", true);

  CreateProcedure(txn, postgres::TRIM_PRO_OID, "btrim", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::TRIM2_PRO_OID, "btrim", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "str"}, {str_type, str_type}, {str_type, str_type},
                  {}, str_type, "", true);

  CreateProcedure(txn, postgres::CONCAT_PRO_OID, "concat", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {variadic_type}, {variadic_type}, {}, str_type,
                  "", true);

  CreateProcedure(txn, postgres::DATE_PART_PRO_OID, "date_part", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"date, date_part_type"}, {date_type, int_type},
                  {date_type, int_type}, {}, int_type, "", false);

  CreateProcedure(txn, postgres::POSITION_PRO_OID, "position", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str1", "str2"}, {str_type, str_type},
                  {str_type, str_type}, {}, int_type, "", true);

  CreateProcedure(txn, postgres::LPAD_PRO_OID, "lpad", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len", "pad"}, {str_type, dec_type, str_type},
                  {str_type, int_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::LPAD2_PRO_OID, "lpad", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len"}, {str_type, dec_type}, {str_type, int_type},
                  {}, str_type, "", true);

  CreateProcedure(txn, postgres::LTRIM2ARG_PRO_OID, "ltrim", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "chars"}, {str_type, str_type},
                  {str_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::LTRIM1ARG_PRO_OID, "ltrim", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::RPAD_PRO_OID, "rpad", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len", "pad"}, {str_type, dec_type, str_type},
                  {str_type, int_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::RPAD2_PRO_OID, "rpad", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "len"}, {str_type, dec_type}, {str_type, int_type},
                  {}, str_type, "", true);

  CreateProcedure(txn, postgres::RTRIM2ARG_PRO_OID, "rtrim", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str", "chars"}, {str_type, str_type},
                  {str_type, str_type}, {}, str_type, "", true);

  CreateProcedure(txn, postgres::RTRIM1ARG_PRO_OID, "rtrim", postgres::INTERNAL_LANGUAGE_OID,
                  postgres::NAMESPACE_DEFAULT_NAMESPACE_OID, {"str"}, {str_type}, {str_type}, {}, str_type, "", true);

  BootstrapProcContexts(txn);
}

void DatabaseCatalog::BootstrapProcContext(const common::ManagedPointer<transaction::TransactionContext> txn,
                                           const proc_oid_t proc_oid, std::string &&func_name,
                                           const type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
                                           const execution::ast::Builtin builtin, const bool is_exec_ctx_required) {
  const auto *const func_context = new execution::functions::FunctionContext(
      std::move(func_name), func_ret_type, std::move(args_type), builtin, is_exec_ctx_required);
  const auto retval UNUSED_ATTRIBUTE = SetProcCtxPtr(txn, proc_oid, func_context);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
}

void DatabaseCatalog::BootstrapProcContexts(const common::ManagedPointer<transaction::TransactionContext> txn) {
  BootstrapProcContext(txn, postgres::ATAN2_PRO_OID, "atan2", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL, type::TypeId::DECIMAL}, execution::ast::Builtin::ATan2, false);

  BootstrapProcContext(txn, postgres::ABS_REAL_PRO_OID, "abs", type::TypeId::DECIMAL, {type::TypeId::DECIMAL},
                       execution::ast::Builtin::Abs, false);

  BootstrapProcContext(txn, postgres::ABS_INT_PRO_OID, "abs", type::TypeId::INTEGER, {type::TypeId::INTEGER},
                       execution::ast::Builtin::Abs, false);

#define BOOTSTRAP_TRIG_FN(str_name, pro_oid, builtin) \
  BootstrapProcContext(txn, pro_oid, str_name, type::TypeId::DECIMAL, {type::TypeId::DECIMAL}, builtin, false);

  BOOTSTRAP_TRIG_FN("acos", postgres::ACOS_PRO_OID, execution::ast::Builtin::ACos)

  BOOTSTRAP_TRIG_FN("asin", postgres::ASIN_PRO_OID, execution::ast::Builtin::ASin)

  BOOTSTRAP_TRIG_FN("atan", postgres::ATAN_PRO_OID, execution::ast::Builtin::ATan)

  BOOTSTRAP_TRIG_FN("cos", postgres::COS_PRO_OID, execution::ast::Builtin::Cos)

  BOOTSTRAP_TRIG_FN("sin", postgres::SIN_PRO_OID, execution::ast::Builtin::Sin)

  BOOTSTRAP_TRIG_FN("tan", postgres::TAN_PRO_OID, execution::ast::Builtin::Tan)

  BOOTSTRAP_TRIG_FN("cosh", postgres::COSH_PRO_OID, execution::ast::Builtin::Cosh)

  BOOTSTRAP_TRIG_FN("sinh", postgres::SINH_PRO_OID, execution::ast::Builtin::Sinh)

  BOOTSTRAP_TRIG_FN("tanh", postgres::TANH_PRO_OID, execution::ast::Builtin::Tanh)

  BOOTSTRAP_TRIG_FN("cot", postgres::COT_PRO_OID, execution::ast::Builtin::Cot)

  BOOTSTRAP_TRIG_FN("ceil", postgres::CEIL_PRO_OID, execution::ast::Builtin::Ceil)

  BOOTSTRAP_TRIG_FN("floor", postgres::FLOOR_PRO_OID, execution::ast::Builtin::Floor)

  BOOTSTRAP_TRIG_FN("truncate", postgres::TRUNCATE_PRO_OID, execution::ast::Builtin::Truncate)

  BOOTSTRAP_TRIG_FN("log10", postgres::LOG10_PRO_OID, execution::ast::Builtin::Log10)

  BOOTSTRAP_TRIG_FN("log2", postgres::LOG2_PRO_OID, execution::ast::Builtin::Log2)

  BOOTSTRAP_TRIG_FN("sqrt", postgres::SQRT_PRO_OID, execution::ast::Builtin::Sqrt)

  BOOTSTRAP_TRIG_FN("cbrt", postgres::CBRT_PRO_OID, execution::ast::Builtin::Cbrt)

  BOOTSTRAP_TRIG_FN("round", postgres::ROUND_PRO_OID, execution::ast::Builtin::Round)

#undef BOOTSTRAP_TRIG_FN

  BootstrapProcContext(txn, postgres::ROUND2_PRO_OID, "round", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL, type::TypeId::INTEGER}, execution::ast::Builtin::Round2, false);

  BootstrapProcContext(txn, postgres::EXP_PRO_OID, "exp", type::TypeId::DECIMAL, {type::TypeId::DECIMAL},
                       execution::ast::Builtin::Exp, true);

  BootstrapProcContext(txn, postgres::ASCII_PRO_OID, "ascii", type::TypeId::INTEGER, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::ASCII, true);

  BootstrapProcContext(txn, postgres::LOWER_PRO_OID, "lower", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Lower, true);

  BootstrapProcContext(txn, postgres::INITCAP_PRO_OID, "initcap", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::InitCap, true);

  BootstrapProcContext(txn, postgres::POW_PRO_OID, "pow", type::TypeId::DECIMAL, {type::TypeId::DECIMAL},
                       execution::ast::Builtin::Pow, false);

  BootstrapProcContext(txn, postgres::SPLIT_PART_PRO_OID, "split_part", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR, type::TypeId::INTEGER},
                       execution::ast::Builtin::SplitPart, true);

  BootstrapProcContext(txn, postgres::CHR_PRO_OID, "chr", type::TypeId::VARCHAR, {type::TypeId::INTEGER},
                       execution::ast::Builtin::Chr, true);

  BootstrapProcContext(txn, postgres::CHARLENGTH_PRO_OID, "char_length", type::TypeId::INTEGER, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::CharLength, true);

  BootstrapProcContext(txn, postgres::POSITION_PRO_OID, "position", type::TypeId::INTEGER,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Position, true);

  BootstrapProcContext(txn, postgres::LENGTH_PRO_OID, "length", type::TypeId::INTEGER, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Length, true);

  BootstrapProcContext(txn, postgres::UPPER_PRO_OID, "upper", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Upper, true);

  BootstrapProcContext(txn, postgres::VERSION_PRO_OID, "version", type::TypeId::VARCHAR, {},
                       execution::ast::Builtin::Version, true);

  BootstrapProcContext(txn, postgres::STARTSWITH_PRO_OID, "starts_with", type::TypeId::BOOLEAN,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::StartsWith, true);

  BootstrapProcContext(txn, postgres::SUBSTR_PRO_OID, "substr", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::Substring, true);

  BootstrapProcContext(txn, postgres::REVERSE_PRO_OID, "reverse", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Reverse, true);

  BootstrapProcContext(txn, postgres::LEFT_PRO_OID, "left", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Left, true);

  BootstrapProcContext(txn, postgres::RIGHT_PRO_OID, "right", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Right, true);

  BootstrapProcContext(txn, postgres::REPEAT_PRO_OID, "repeat", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Repeat, true);

  BootstrapProcContext(txn, postgres::TRIM_PRO_OID, "btrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Trim, true);

  BootstrapProcContext(txn, postgres::TRIM2_PRO_OID, "btrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Trim2, true);

  BootstrapProcContext(txn, postgres::CONCAT_PRO_OID, "concat", type::TypeId::VARCHAR, {type::TypeId::VARIADIC},
                       execution::ast::Builtin::Concat, true);

  BootstrapProcContext(txn, postgres::LPAD_PRO_OID, "lpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Lpad, true);

  BootstrapProcContext(txn, postgres::LPAD2_PRO_OID, "lpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Lpad, true);

  BootstrapProcContext(txn, postgres::LTRIM2ARG_PRO_OID, "ltrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Ltrim, true);

  BootstrapProcContext(txn, postgres::LTRIM1ARG_PRO_OID, "ltrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Ltrim, true);

  BootstrapProcContext(txn, postgres::RPAD_PRO_OID, "rpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::VARCHAR},
                       execution::ast::Builtin::Rpad, true);

  BootstrapProcContext(txn, postgres::RPAD2_PRO_OID, "rpad", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::INTEGER}, execution::ast::Builtin::Rpad, true);

  BootstrapProcContext(txn, postgres::RTRIM2ARG_PRO_OID, "rtrim", type::TypeId::VARCHAR,
                       {type::TypeId::VARCHAR, type::TypeId::VARCHAR}, execution::ast::Builtin::Rtrim, true);

  BootstrapProcContext(txn, postgres::RTRIM1ARG_PRO_OID, "rtrim", type::TypeId::VARCHAR, {type::TypeId::VARCHAR},
                       execution::ast::Builtin::Rtrim, true);

  BootstrapProcContext(txn, postgres::MOD_PRO_OID, "mod", type::TypeId::DECIMAL,
                       {type::TypeId::DECIMAL, type::TypeId::DECIMAL}, execution::ast::Builtin::Mod, false);

  BootstrapProcContext(txn, postgres::INTMOD_PRO_OID, "mod", type::TypeId::INTEGER,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER}, execution::ast::Builtin::Mod, false);

  BootstrapProcContext(txn, postgres::NP_RUNNERS_EMIT_INT_PRO_OID, "NpRunnersEmitInt", type::TypeId::INTEGER,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::NpRunnersEmitInt, true);

  BootstrapProcContext(txn, postgres::NP_RUNNERS_EMIT_REAL_PRO_OID, "NpRunnersEmitReal", type::TypeId::DECIMAL,
                       {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER},
                       execution::ast::Builtin::NpRunnersEmitReal, true);

  BootstrapProcContext(txn, postgres::NP_RUNNERS_DUMMY_INT_PRO_OID, "NpRunnersDummyInt", type::TypeId::INTEGER, {},
                       execution::ast::Builtin::NpRunnersDummyInt, true);

  BootstrapProcContext(txn, postgres::NP_RUNNERS_DUMMY_REAL_PRO_OID, "NpRunnersDummyReal", type::TypeId::DECIMAL, {},
                       execution::ast::Builtin::NpRunnersDummyReal, true);

  BootstrapProcContext(txn, postgres::DATE_PART_PRO_OID, "date_part", type::TypeId::INTEGER,
                       {type::TypeId::DATE, type::TypeId::INTEGER}, execution::ast::Builtin::DatePart, false);
}

bool DatabaseCatalog::SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn,
                                    const proc_oid_t proc_oid,
                                    const execution::functions::FunctionContext *func_context) {
  NOISEPAGE_ASSERT(
      write_lock_.load() == txn->FinishTime(),
      "Setting the object's pointer should only be done after successful DDL change request. i.e. this txn "
      "should already have the lock.");

  // The catalog owns this pointer now, so if the txn ends up aborting, we need to make sure it gets freed.
  txn->RegisterAbortAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() { delete func_context; });
  });

  // Do not need to store the projection map because it is only a single column
  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();

  auto *const index_buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(index_buffer);

  // Find the entry using the index
  *(reinterpret_cast<proc_oid_t *>(key_pr->AccessForceNotNull(0))) = proc_oid;
  std::vector<storage::TupleSlot> index_results;
  procs_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no reasonable "
      "code path for this to be called on an oid that isn't present.");

  delete[] index_buffer;
  auto *const update_redo = txn->StageWrite(db_oid_, postgres::PRO_TABLE_OID, pg_proc_ptr_pri_);
  *reinterpret_cast<const execution::functions::FunctionContext **>(update_redo->Delta()->AccessForceNotNull(0)) =
      func_context;
  update_redo->SetTupleSlot(index_results[0]);
  return procs_->Update(txn, update_redo);
}

common::ManagedPointer<execution::functions::FunctionContext> DatabaseCatalog::GetProcCtxPtr(
    common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid) {
  // Do not need to store the projection map because it is only a single column
  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();

  auto *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_ptr_pri_.ProjectedRowSize());
  auto *const key_pr = oid_pri.InitializeRow(buffer);

  // Find the entry using the index
  *(reinterpret_cast<proc_oid_t *>(key_pr->AccessForceNotNull(0))) = proc_oid;
  std::vector<storage::TupleSlot> index_results;
  procs_oid_index_->ScanKey(*txn, *key_pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that function was "
      "called with an oid that doesn't exist in the Catalog, which implies a programmer error. There's no reasonable "
      "code path for this to be called on an oid that isn't present.");

  auto *select_pr = pg_proc_ptr_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = procs_->Select(txn, index_results[0], select_pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  auto *ptr_ptr = (reinterpret_cast<void **>(select_pr->AccessWithNullCheck(0)));

  execution::functions::FunctionContext *ptr;
  if (ptr_ptr == nullptr) {
    ptr = nullptr;
  } else {
    ptr = *reinterpret_cast<execution::functions::FunctionContext **>(ptr_ptr);
  }

  delete[] buffer;
  return common::ManagedPointer<execution::functions::FunctionContext>(ptr);
}

common::ManagedPointer<execution::functions::FunctionContext> DatabaseCatalog::GetFunctionContext(
    const common::ManagedPointer<transaction::TransactionContext> txn, catalog::proc_oid_t proc_oid) {
  auto func_ctx = GetProcCtxPtr(txn, proc_oid);
  if (func_ctx == nullptr) {
    if (IS_BUILTIN_PROC(proc_oid)) {
      BootstrapProcContexts(txn);
    } else {
      UNREACHABLE("We don't support dynamically added udf's yet");
    }
    func_ctx = GetProcCtxPtr(txn, proc_oid);
  }
  return func_ctx;
}

bool DatabaseCatalog::CreateTableEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                       const table_oid_t table_oid, const namespace_oid_t ns_oid,
                                       const std::string &name, const Schema &schema) {
  auto *const insert_redo = txn->StageWrite(db_oid_, postgres::CLASS_TABLE_OID, pg_class_all_cols_pri_);
  auto *const insert_pr = insert_redo->Delta();

  // Write the ns_oid into the PR
  const auto ns_offset = pg_class_all_cols_prm_[postgres::RELNAMESPACE_COL_OID];
  auto *const ns_ptr = insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<namespace_oid_t *>(ns_ptr)) = ns_oid;

  // Write the table_oid into the PR
  const auto table_oid_offset = pg_class_all_cols_prm_[postgres::RELOID_COL_OID];
  auto *const table_oid_ptr = insert_pr->AccessForceNotNull(table_oid_offset);
  *(reinterpret_cast<table_oid_t *>(table_oid_ptr)) = table_oid;

  auto next_col_oid = col_oid_t(static_cast<uint32_t>(schema.GetColumns().size() + 1));

  // Write the next_col_oid into the PR
  const auto next_col_oid_offset = pg_class_all_cols_prm_[postgres::REL_NEXTCOLOID_COL_OID];
  auto *const next_col_oid_ptr = insert_pr->AccessForceNotNull(next_col_oid_offset);
  *(reinterpret_cast<col_oid_t *>(next_col_oid_ptr)) = next_col_oid;

  // Write the schema_ptr as nullptr into the PR (need to update once we've recreated the columns)
  const auto schema_ptr_offset = pg_class_all_cols_prm_[postgres::REL_SCHEMA_COL_OID];
  auto *const schema_ptr_ptr = insert_pr->AccessForceNotNull(schema_ptr_offset);
  *(reinterpret_cast<Schema **>(schema_ptr_ptr)) = nullptr;

  // Set table_ptr to NULL because it gets set by execution layer after instantiation
  const auto table_ptr_offset = pg_class_all_cols_prm_[postgres::REL_PTR_COL_OID];
  insert_pr->SetNull(table_ptr_offset);

  // Write the kind into the PR
  const auto kind_offset = pg_class_all_cols_prm_[postgres::RELKIND_COL_OID];
  auto *const kind_ptr = insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<char *>(kind_ptr)) = static_cast<char>(postgres::ClassKind::REGULAR_TABLE);

  // Create the necessary varlen for storage operations
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Write the name into the PR
  const auto name_offset = pg_class_all_cols_prm_[postgres::RELNAME_COL_OID];
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

  auto *const update_redo = txn->StageWrite(db_oid_, postgres::CLASS_TABLE_OID, set_class_schema_pri_);
  auto *const update_pr = update_redo->Delta();

  update_redo->SetTupleSlot(tuple_slot);
  *reinterpret_cast<Schema **>(update_pr->AccessForceNotNull(0)) = new_schema;
  auto UNUSED_ATTRIBUTE res = classes_->Update(txn, update_redo);
  NOISEPAGE_ASSERT(res, "Updating an uncommitted insert should not fail");

  return true;
}

std::vector<std::pair<uint32_t, postgres::ClassKind>> DatabaseCatalog::GetNamespaceClassOids(
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
  std::vector<std::pair<uint32_t, postgres::ClassKind>> ns_objects;
  ns_objects.reserve(index_scan_results.size());
  for (const auto scan_result : index_scan_results) {
    const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, scan_result, select_pr);
    NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
    // oid_t is guaranteed to be larger in size than ClassKind, so we know the column offsets without the PR map
    ns_objects.emplace_back(*(reinterpret_cast<const uint32_t *const>(select_pr->AccessWithNullCheck(0))),
                            *(reinterpret_cast<const postgres::ClassKind *const>(select_pr->AccessForceNotNull(1))));
  }

  // Finish
  delete[] buffer;
  return ns_objects;
}

std::pair<void *, postgres::ClassKind> DatabaseCatalog::GetClassPtrKind(
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
  auto kind = *(reinterpret_cast<const postgres::ClassKind *const>(select_pr->AccessForceNotNull(1)));

  void *ptr;
  if (ptr_ptr == nullptr) {
    ptr = nullptr;
  } else {
    ptr = *ptr_ptr;
  }

  delete[] buffer;
  return {ptr, kind};
}

std::pair<void *, postgres::ClassKind> DatabaseCatalog::GetClassSchemaPtrKind(
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
  auto kind = *(reinterpret_cast<const postgres::ClassKind *const>(select_pr->AccessForceNotNull(1)));

  NOISEPAGE_ASSERT(ptr != nullptr, "Schema pointer shouldn't ever be NULL under current catalog semantics.");

  delete[] buffer;
  return {ptr, kind};
}

template <typename Column, typename ColOid>
Column DatabaseCatalog::MakeColumn(storage::ProjectedRow *const pr, const storage::ProjectionMap &pr_map) {
  auto col_oid = *reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(pr_map.at(postgres::ATTNUM_COL_OID)));
  auto col_name =
      reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_map.at(postgres::ATTNAME_COL_OID)));
  auto col_type = *reinterpret_cast<type::TypeId *>(pr->AccessForceNotNull(pr_map.at(postgres::ATTTYPID_COL_OID)));
  auto col_len = *reinterpret_cast<uint16_t *>(pr->AccessForceNotNull(pr_map.at(postgres::ATTLEN_COL_OID)));
  auto col_null = !(*reinterpret_cast<bool *>(pr->AccessForceNotNull(pr_map.at(postgres::ATTNOTNULL_COL_OID))));
  auto *col_expr = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(pr_map.at(postgres::ADSRC_COL_OID)));

  // TODO(WAN): Why are we deserializing expressions to make a catalog column? This is potentially busted.
  // Our JSON library is also not the most performant.
  // I believe it is OK that the unique ptr goes out of scope because right now both Column constructors copy the expr.
  auto deserialized = parser::DeserializeExpression(nlohmann::json::parse(col_expr->StringView()));

  auto expr = std::move(deserialized.result_);
  NOISEPAGE_ASSERT(deserialized.non_owned_exprs_.empty(), "Congrats, you get to refactor the catalog API.");

  std::string name(reinterpret_cast<const char *>(col_name->Content()), col_name->Size());
  Column col = (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY)
                   ? Column(name, col_type, col_len, col_null, *expr)
                   : Column(name, col_type, col_null, *expr);

  col.SetOid(ColOid(col_oid));
  return col;
}

bool DatabaseCatalog::TryLock(const common::ManagedPointer<transaction::TransactionContext> txn) {
  auto current_val = write_lock_.load();

  const transaction::timestamp_t txn_id = txn->FinishTime();     // this is the uncommitted txn id
  const transaction::timestamp_t start_time = txn->StartTime();  // this is the unchanging start time of the txn

  const bool already_hold_lock = current_val == txn_id;
  if (already_hold_lock) return true;

  const bool owned_by_other_txn = !transaction::TransactionUtil::Committed(current_val);
  const bool newer_committed_version = transaction::TransactionUtil::Committed(current_val) &&
                                       transaction::TransactionUtil::NewerThan(current_val, start_time);

  if (owned_by_other_txn || newer_committed_version) {
    txn->SetMustAbort();  // though no changes were written to the storage layer, we'll treat this as a DDL change
                          // failure
    // and force the txn to rollback
    return false;
  }

  if (write_lock_.compare_exchange_strong(current_val, txn_id)) {
    // acquired the lock
    auto *const write_lock = &write_lock_;
    txn->RegisterCommitAction([=]() -> void { write_lock->store(txn->FinishTime()); });
    txn->RegisterAbortAction([=]() -> void { write_lock->store(current_val); });
    return true;
  }
  txn->SetMustAbort();  // though no changes were written to the storage layer, we'll treat this as a DDL change failure
                        // and force the txn to rollback
  return false;
}

bool DatabaseCatalog::CreateLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                     const std::string &lanname, language_oid_t oid) {
  // Insert into table
  if (!TryLock(txn)) return false;
  const auto name_varlen = storage::StorageUtil::CreateVarlen(lanname);
  // Get & Fill Redo Record
  auto *const redo = txn->StageWrite(db_oid_, postgres::LANGUAGE_TABLE_OID, pg_language_all_cols_pri_);
  *(reinterpret_cast<language_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[postgres::LANOID_COL_OID]))) = oid;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[postgres::LANNAME_COL_OID]))) = name_varlen;

  *(reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[postgres::LANISPL_COL_OID]))) =
      false;
  *(reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[postgres::LANPLTRUSTED_COL_OID]))) = true;
  redo->Delta()->SetNull(pg_language_all_cols_prm_[postgres::LANINLINE_COL_OID]);
  redo->Delta()->SetNull(pg_language_all_cols_prm_[postgres::LANVALIDATOR_COL_OID]);
  redo->Delta()->SetNull(pg_language_all_cols_prm_[postgres::LANPLCALLFOID_COL_OID]);

  const auto tuple_slot = languages_->Insert(txn, redo);

  // Insert into name index
  auto name_pri = languages_name_index_->GetProjectedRowInitializer();
  auto oid_pri = languages_oid_index_->GetProjectedRowInitializer();

  // allocate from largest pri
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  auto *index_pr = name_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;

  if (!languages_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  // Insert into oid index
  index_pr = oid_pri.InitializeRow(buffer);
  // Write the attributes in the ProjectedRow
  *(reinterpret_cast<language_oid_t *>(index_pr->AccessForceNotNull(0))) = oid;
  languages_oid_index_->InsertUnique(txn, *index_pr, tuple_slot);

  delete[] buffer;
  return true;
}

language_oid_t DatabaseCatalog::CreateLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                               const std::string &lanname) {
  auto oid = language_oid_t{next_oid_++};
  if (!CreateLanguage(txn, lanname, oid)) {
    return INVALID_LANGUAGE_OID;
  }

  return oid;
}

language_oid_t DatabaseCatalog::GetLanguageOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                               const std::string &lanname) {
  auto name_pri = languages_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_language_all_cols_pri_.ProjectedRowSize());

  auto name_pr = name_pri.InitializeRow(buffer);
  const auto name_varlen = storage::StorageUtil::CreateVarlen(lanname);

  *reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(0)) = name_varlen;

  std::vector<storage::TupleSlot> results;
  languages_name_index_->ScanKey(*txn, *name_pr, &results);

  auto oid = INVALID_LANGUAGE_OID;
  if (!results.empty()) {
    NOISEPAGE_ASSERT(results.size() == 1, "Unique language name index should return <= 1 result");

    // extract oid from results[0]
    auto found_tuple = results[0];

    // TODO(tanujnay112): Can optimize to not extract all columns.
    // We may need all columns in the future though so doing this for now
    auto all_cols_pr = pg_language_all_cols_pri_.InitializeRow(buffer);
    languages_->Select(txn, found_tuple, all_cols_pr);

    oid = *reinterpret_cast<language_oid_t *>(
        all_cols_pr->AccessForceNotNull(pg_language_all_cols_prm_[postgres::LANOID_COL_OID]));
  }

  if (name_varlen.NeedReclaim()) {
    delete[] name_varlen.Content();
  }

  delete[] buffer;
  return oid;
}

bool DatabaseCatalog::DropLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                   language_oid_t oid) {
  // Delete fom table
  if (!TryLock(txn)) return false;
  NOISEPAGE_ASSERT(oid != INVALID_LANGUAGE_OID, "Invalid oid passed");
  // Delete from oid index
  auto name_pri = languages_name_index_->GetProjectedRowInitializer();
  auto oid_pri = languages_oid_index_->GetProjectedRowInitializer();

  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_language_all_cols_pri_.ProjectedRowSize());
  auto index_pr = oid_pri.InitializeRow(buffer);
  *reinterpret_cast<language_oid_t *>(index_pr->AccessForceNotNull(0)) = oid;

  std::vector<storage::TupleSlot> results;
  languages_oid_index_->ScanKey(*txn, *index_pr, &results);
  if (results.empty()) {
    delete[] buffer;
    return false;
  }

  NOISEPAGE_ASSERT(results.size() == 1, "More than one non-unique result found in unique index.");

  auto to_delete_slot = results[0];
  txn->StageDelete(db_oid_, postgres::LANGUAGE_TABLE_OID, to_delete_slot);

  if (!languages_->Delete(txn, to_delete_slot)) {
    // Someone else has a write-lock. Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  languages_oid_index_->Delete(txn, *index_pr, to_delete_slot);

  auto table_pr = pg_language_all_cols_pri_.InitializeRow(buffer);
  bool UNUSED_ATTRIBUTE visible = languages_->Select(txn, to_delete_slot, table_pr);

  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(
      table_pr->AccessForceNotNull(pg_language_all_cols_prm_[postgres::LANNAME_COL_OID]));

  index_pr = name_pri.InitializeRow(buffer);
  *reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0)) = name_varlen;

  languages_name_index_->Delete(txn, *index_pr, to_delete_slot);

  delete[] buffer;

  return true;
}

proc_oid_t DatabaseCatalog::CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn,
                                            const std::string &procname, language_oid_t language_oid,
                                            namespace_oid_t procns, const std::vector<std::string> &args,
                                            const std::vector<type_oid_t> &arg_types,
                                            const std::vector<type_oid_t> &all_arg_types,
                                            const std::vector<postgres::ProArgModes> &arg_modes, type_oid_t rettype,
                                            const std::string &src, bool is_aggregate) {
  proc_oid_t oid = proc_oid_t{next_oid_++};
  auto result = CreateProcedure(txn, oid, procname, language_oid, procns, args, arg_types, all_arg_types, arg_modes,
                                rettype, src, is_aggregate);
  return result ? oid : INVALID_PROC_OID;
}

bool DatabaseCatalog::CreateProcedure(const common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t oid,
                                      const std::string &procname, language_oid_t language_oid, namespace_oid_t procns,
                                      const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                                      const std::vector<type_oid_t> &all_arg_types,
                                      const std::vector<postgres::ProArgModes> &arg_modes, type_oid_t rettype,
                                      const std::string &src, bool is_aggregate) {
  NOISEPAGE_ASSERT(args.size() < UINT16_MAX, "Number of arguments must fit in a SMALLINT");

  // Insert into table
  if (!TryLock(txn)) return false;
  const auto name_varlen = storage::StorageUtil::CreateVarlen(procname);

  std::vector<std::string> arg_name_vec;
  arg_name_vec.reserve(args.size() * sizeof(storage::VarlenEntry));

  for (auto &arg : args) {
    arg_name_vec.push_back(arg);
  }

  const auto arg_names_varlen = storage::StorageUtil::CreateVarlen(arg_name_vec);
  const auto arg_types_varlen = storage::StorageUtil::CreateVarlen(arg_types);
  const auto all_arg_types_varlen = storage::StorageUtil::CreateVarlen(all_arg_types);
  const auto arg_modes_varlen = storage::StorageUtil::CreateVarlen(arg_modes);
  const auto src_varlen = storage::StorageUtil::CreateVarlen(src);

  auto *const redo = txn->StageWrite(db_oid_, postgres::PRO_TABLE_OID, pg_proc_all_cols_pri_);
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PRONAME_COL_OID]))) = name_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROARGNAMES_COL_OID]))) = arg_names_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROARGTYPES_COL_OID]))) = arg_types_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PROALLARGTYPES_COL_OID]))) = all_arg_types_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROARGMODES_COL_OID]))) = arg_modes_varlen;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROSRC_COL_OID]))) = src_varlen;

  *(reinterpret_cast<proc_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROOID_COL_OID]))) = oid;
  *(reinterpret_cast<language_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROLANG_COL_OID]))) = language_oid;
  *(reinterpret_cast<namespace_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PRONAMESPACE_COL_OID]))) = procns;
  *(reinterpret_cast<type_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PRORETTYPE_COL_OID]))) = rettype;

  *(reinterpret_cast<uint16_t *>(redo->Delta()->AccessForceNotNull(
      pg_proc_all_cols_prm_[postgres::PRONARGS_COL_OID]))) = static_cast<uint16_t>(args.size());

  // setting zero default args
  *(reinterpret_cast<uint16_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PRONARGDEFAULTS_COL_OID]))) = 0;
  redo->Delta()->SetNull(pg_proc_all_cols_prm_[postgres::PROARGDEFAULTS_COL_OID]);

  *reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROISAGG_COL_OID])) =
      is_aggregate;

  // setting defaults of unexposed attributes
  // proiswindow, proisstrict, provolatile, provariadic, prorows, procost, proconfig

  // postgres documentation says this should be 0 if no variadics are there
  *(reinterpret_cast<type_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROVARIADIC_COL_OID]))) = type_oid_t{0};

  *(reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROISWINDOW_COL_OID]))) =
      false;

  // stable by default
  *(reinterpret_cast<char *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROVOLATILE_COL_OID]))) =
      's';

  // strict by default
  *(reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROISSTRICT_COL_OID]))) =
      true;

  *(reinterpret_cast<double *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROROWS_COL_OID]))) =
      0;

  *(reinterpret_cast<double *>(redo->Delta()->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROCOST_COL_OID]))) =
      0;

  redo->Delta()->SetNull(pg_proc_all_cols_prm_[postgres::PROCONFIG_COL_OID]);
  redo->Delta()->SetNull(pg_proc_all_cols_prm_[postgres::PRO_CTX_PTR_COL_OID]);

  const auto tuple_slot = procs_->Insert(txn, redo);

  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();
  auto name_pri = procs_name_index_->GetProjectedRowInitializer();

  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto name_pr = name_pri.InitializeRow(buffer);
  auto name_map = procs_name_index_->GetKeyOidToOffsetMap();
  *(reinterpret_cast<namespace_oid_t *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(1)]))) = procns;
  *(reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(2)]))) =
      name_varlen;

  auto result = procs_name_index_->Insert(txn, *name_pr, tuple_slot);
  if (!result) {
    delete[] buffer;
    return false;
  }

  auto oid_pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<proc_oid_t *>(oid_pr->AccessForceNotNull(0))) = oid;
  result = procs_oid_index_->InsertUnique(txn, *oid_pr, tuple_slot);
  NOISEPAGE_ASSERT(result, "Oid insertion should be unique");

  delete[] buffer;
  return true;
}

bool DatabaseCatalog::DropProcedure(const common::ManagedPointer<transaction::TransactionContext> txn,
                                    proc_oid_t proc) {
  if (!TryLock(txn)) return false;
  NOISEPAGE_ASSERT(proc != INVALID_PROC_OID, "Invalid oid passed");

  auto name_pri = procs_name_index_->GetProjectedRowInitializer();
  auto oid_pri = procs_oid_index_->GetProjectedRowInitializer();

  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_all_cols_pri_.ProjectedRowSize());

  auto oid_pr = oid_pri.InitializeRow(buffer);
  *reinterpret_cast<proc_oid_t *>(oid_pr->AccessForceNotNull(0)) = proc;

  std::vector<storage::TupleSlot> results;
  procs_oid_index_->ScanKey(*txn, *oid_pr, &results);
  if (results.empty()) {
    delete[] buffer;
    return false;
  }

  NOISEPAGE_ASSERT(results.size() == 1, "More than one non-unique result found in unique index.");

  auto to_delete_slot = results[0];
  txn->StageDelete(db_oid_, postgres::LANGUAGE_TABLE_OID, to_delete_slot);

  if (!procs_->Delete(txn, to_delete_slot)) {
    // Someone else has a write-lock. Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  procs_oid_index_->Delete(txn, *oid_pr, to_delete_slot);

  auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);
  bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, to_delete_slot, table_pr);

  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(
      table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PRONAME_COL_OID]));
  auto proc_ns = *reinterpret_cast<namespace_oid_t *>(
      table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PRONAMESPACE_COL_OID]));

  auto ctx_ptr = table_pr->AccessWithNullCheck(pg_proc_all_cols_prm_[postgres::PRO_CTX_PTR_COL_OID]);

  auto name_pr = name_pri.InitializeRow(buffer);

  auto name_map = procs_name_index_->GetKeyOidToOffsetMap();
  *reinterpret_cast<namespace_oid_t *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(1)])) = proc_ns;
  *reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(2)])) = name_varlen;

  procs_name_index_->Delete(txn, *name_pr, to_delete_slot);

  delete[] buffer;

  if (ctx_ptr != nullptr) {
    txn->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
      deferred_action_manager->RegisterDeferredAction(
          [=]() { deferred_action_manager->RegisterDeferredAction([=]() { delete ctx_ptr; }); });
    });
  }
  return true;
}

proc_oid_t DatabaseCatalog::GetProcOid(common::ManagedPointer<transaction::TransactionContext> txn,
                                       namespace_oid_t procns, const std::string &procname,
                                       const std::vector<type_oid_t> &arg_types) {
  auto name_pri = procs_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_proc_all_cols_pri_.ProjectedRowSize());

  auto name_pr = name_pri.InitializeRow(buffer);
  auto name_map = procs_name_index_->GetKeyOidToOffsetMap();

  auto name_varlen = storage::StorageUtil::CreateVarlen(procname);
  auto all_arg_types_varlen = storage::StorageUtil::CreateVarlen(arg_types);
  *reinterpret_cast<namespace_oid_t *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(1)])) = procns;
  *reinterpret_cast<storage::VarlenEntry *>(name_pr->AccessForceNotNull(name_map[indexkeycol_oid_t(2)])) = name_varlen;

  std::vector<storage::TupleSlot> results;
  procs_name_index_->ScanKey(*txn, *name_pr, &results);

  proc_oid_t ret = INVALID_PROC_OID;
  std::vector<proc_oid_t> matching_functions;
  if (!results.empty()) {
    const std::vector<type_oid_t> variadic = {GetTypeOidForType(type::TypeId::VARIADIC)};
    auto variadic_varlen = storage::StorageUtil::CreateVarlen(variadic);

    // Search through results and see if any match the parsed function by argument types
    for (auto &tuple : results) {
      auto table_pr = pg_proc_all_cols_pri_.InitializeRow(buffer);
      bool UNUSED_ATTRIBUTE visible = procs_->Select(txn, tuple, table_pr);
      storage::VarlenEntry index_all_arg_types = *reinterpret_cast<storage::VarlenEntry *>(
          table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROALLARGTYPES_COL_OID]));
      // variadic functions will match any argument types as long as there one or more arguments
      if (index_all_arg_types == all_arg_types_varlen ||
          (index_all_arg_types == variadic_varlen && !arg_types.empty())) {
        proc_oid_t proc_oid = *reinterpret_cast<proc_oid_t *>(
            table_pr->AccessForceNotNull(pg_proc_all_cols_prm_[postgres::PROOID_COL_OID]));
        matching_functions.push_back(proc_oid);
        break;
      }
    }
    if (variadic_varlen.NeedReclaim()) {
      delete[] variadic_varlen.Content();
    }
  }

  if (name_varlen.NeedReclaim()) {
    delete[] name_varlen.Content();
  }

  if (all_arg_types_varlen.NeedReclaim()) {
    delete[] all_arg_types_varlen.Content();
  }

  delete[] buffer;

  if (matching_functions.size() == 1) {
    ret = matching_functions[0];
  } else if (matching_functions.size() > 1) {
    // TODO(Joe Koshakow) would be nice to to include the parsed arg types of the function and the arg types that it
    // matches with
    throw BINDER_EXCEPTION(
        fmt::format(
            "Ambiguous function \"{}\", with given types. It matches multiple function signatures in the catalog",
            procname),
        common::ErrorCode::ERRCODE_DUPLICATE_FUNCTION);
  }

  return ret;
}

template bool DatabaseCatalog::CreateColumn<Schema::Column, table_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t class_oid,
    const col_oid_t col_oid, const Schema::Column &col);
template bool DatabaseCatalog::CreateColumn<IndexSchema::Column, index_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const index_oid_t class_oid,
    const indexkeycol_oid_t col_oid, const IndexSchema::Column &col);

template std::vector<Schema::Column> DatabaseCatalog::GetColumns<Schema::Column, table_oid_t, col_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t class_oid);

template std::vector<IndexSchema::Column>
DatabaseCatalog::GetColumns<IndexSchema::Column, index_oid_t, indexkeycol_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const index_oid_t class_oid);

template bool DatabaseCatalog::DeleteColumns<Schema::Column, table_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t class_oid);

template bool DatabaseCatalog::DeleteColumns<IndexSchema::Column, index_oid_t>(
    const common::ManagedPointer<transaction::TransactionContext> txn, const index_oid_t class_oid);

template Schema::Column DatabaseCatalog::MakeColumn<Schema::Column, col_oid_t>(storage::ProjectedRow *const pr,
                                                                               const storage::ProjectionMap &pr_map);

template IndexSchema::Column DatabaseCatalog::MakeColumn<IndexSchema::Column, indexkeycol_oid_t>(
    storage::ProjectedRow *const pr, const storage::ProjectionMap &pr_map);

}  // namespace noisepage::catalog
