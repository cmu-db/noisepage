#include "catalog/postgres/pg_language_impl.h"

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::catalog::postgres {

PgLanguageImpl::PgLanguageImpl(db_oid_t db_oid) : db_oid_(db_oid) {}

void PgLanguageImpl::BootstrapPRIs() {
  const std::vector<col_oid_t> pg_language_all_oids{PgLanguage::PG_LANGUAGE_ALL_COL_OIDS.cbegin(),
                                                    PgLanguage::PG_LANGUAGE_ALL_COL_OIDS.cend()};
  pg_language_all_cols_pri_ = languages_->InitializerForProjectedRow(pg_language_all_oids);
  pg_language_all_cols_prm_ = languages_->ProjectionMapForOids(pg_language_all_oids);
}

void PgLanguageImpl::Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                               common::ManagedPointer<DatabaseCatalog> dbc) {
  UNUSED_ATTRIBUTE bool retval;

  retval = dbc->CreateTableEntry(txn, PgLanguage::LANGUAGE_TABLE_OID, NAMESPACE_CATALOG_NAMESPACE_OID, "pg_language",
                                 Builder::GetLanguageTableSchema());
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetTablePointer(txn, PgLanguage::LANGUAGE_TABLE_OID, languages_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, PgLanguage::LANGUAGE_TABLE_OID,
                                 PgLanguage::LANGUAGE_OID_INDEX_OID, "pg_languages_oid_index",
                                 Builder::GetLanguageOidIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgLanguage::LANGUAGE_OID_INDEX_OID, languages_oid_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  retval = dbc->CreateIndexEntry(txn, NAMESPACE_CATALOG_NAMESPACE_OID, PgLanguage::LANGUAGE_TABLE_OID,
                                 PgLanguage::LANGUAGE_NAME_INDEX_OID, "pg_languages_name_index",
                                 Builder::GetLanguageNameIndexSchema(db_oid_));
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");
  retval = dbc->SetIndexPointer(txn, PgLanguage::LANGUAGE_NAME_INDEX_OID, languages_name_index_);
  NOISEPAGE_ASSERT(retval, "Bootstrap operations should not fail");

  BootstrapLanguages(txn, dbc);
}

bool PgLanguageImpl::CreateLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                    const std::string &lanname, language_oid_t oid) {
  const auto name_varlen = storage::StorageUtil::CreateVarlen(lanname);
  // Get & Fill Redo Record
  auto *const redo = txn->StageWrite(db_oid_, PgLanguage::LANGUAGE_TABLE_OID, pg_language_all_cols_pri_);
  *(reinterpret_cast<language_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[PgLanguage::LANOID_COL_OID]))) = oid;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[PgLanguage::LANNAME_COL_OID]))) = name_varlen;

  *(reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[PgLanguage::LANISPL_COL_OID]))) = false;
  *(reinterpret_cast<bool *>(
      redo->Delta()->AccessForceNotNull(pg_language_all_cols_prm_[PgLanguage::LANPLTRUSTED_COL_OID]))) = true;
  redo->Delta()->SetNull(pg_language_all_cols_prm_[PgLanguage::LANINLINE_COL_OID]);
  redo->Delta()->SetNull(pg_language_all_cols_prm_[PgLanguage::LANVALIDATOR_COL_OID]);
  redo->Delta()->SetNull(pg_language_all_cols_prm_[PgLanguage::LANPLCALLFOID_COL_OID]);

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

language_oid_t PgLanguageImpl::GetLanguageOid(const common::ManagedPointer<transaction::TransactionContext> txn,
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
        all_cols_pr->AccessForceNotNull(pg_language_all_cols_prm_[PgLanguage::LANOID_COL_OID]));
  }

  if (name_varlen.NeedReclaim()) {
    delete[] name_varlen.Content();
  }

  delete[] buffer;
  return oid;
}

bool PgLanguageImpl::DropLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  language_oid_t oid) {
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
  txn->StageDelete(db_oid_, PgLanguage::LANGUAGE_TABLE_OID, to_delete_slot);

  if (!languages_->Delete(txn, to_delete_slot)) {
    // Someone else has a write-lock. Free the buffer and return false to indicate failure
    delete[] buffer;
    return false;
  }

  languages_oid_index_->Delete(txn, *index_pr, to_delete_slot);

  auto table_pr = pg_language_all_cols_pri_.InitializeRow(buffer);
  bool UNUSED_ATTRIBUTE visible = languages_->Select(txn, to_delete_slot, table_pr);

  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(
      table_pr->AccessForceNotNull(pg_language_all_cols_prm_[PgLanguage::LANNAME_COL_OID]));

  index_pr = name_pri.InitializeRow(buffer);
  *reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0)) = name_varlen;

  languages_name_index_->Delete(txn, *index_pr, to_delete_slot);

  delete[] buffer;

  return true;
}

void PgLanguageImpl::BootstrapLanguages(const common::ManagedPointer<transaction::TransactionContext> txn,
                                        const common::ManagedPointer<DatabaseCatalog> dbc) {
  dbc->CreateLanguage(txn, "plpgsql", PgLanguage::PLPGSQL_LANGUAGE_OID);
  dbc->CreateLanguage(txn, "internal", PgLanguage::INTERNAL_LANGUAGE_OID);
}

}  // namespace noisepage::catalog::postgres
