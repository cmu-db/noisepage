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
  dbc->BootstrapTable(txn, PgLanguage::LANGUAGE_TABLE_OID, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, "pg_language",
                      Builder::GetLanguageTableSchema(), languages_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgLanguage::LANGUAGE_TABLE_OID,
                      PgLanguage::LANGUAGE_OID_INDEX_OID, "pg_languages_oid_index",
                      Builder::GetLanguageOidIndexSchema(db_oid_), languages_oid_index_);
  dbc->BootstrapIndex(txn, PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID, PgLanguage::LANGUAGE_TABLE_OID,
                      PgLanguage::LANGUAGE_NAME_INDEX_OID, "pg_languages_name_index",
                      Builder::GetLanguageNameIndexSchema(db_oid_), languages_name_index_);

  BootstrapLanguages(txn, dbc);
}

bool PgLanguageImpl::CreateLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                    const std::string &lanname, language_oid_t oid) {
  auto *const redo = txn->StageWrite(db_oid_, PgLanguage::LANGUAGE_TABLE_OID, pg_language_all_cols_pri_);
  auto delta = common::ManagedPointer(redo->Delta());
  auto &pm = pg_language_all_cols_prm_;

  const auto name_varlen = storage::StorageUtil::CreateVarlen(lanname);

  // Prepare PR for insertion.
  {
    PgLanguage::LANOID.Set(delta, pm, oid);
    PgLanguage::LANNAME.Set(delta, pm, name_varlen);
    PgLanguage::LANISPL.Set(delta, pm, false);
    PgLanguage::LANPLTRUSTED.Set(delta, pm, true);
    PgLanguage::LANPLCALLFOID.SetNull(delta, pm);
    PgLanguage::LANINLINE.SetNull(delta, pm);
    PgLanguage::LANVALIDATOR.SetNull(delta, pm);
  }

  // Insert into pg_language.
  const auto tuple_slot = languages_->Insert(txn, redo);

  // Allocate enough space for the largest PRI.
  auto name_pri = languages_name_index_->GetProjectedRowInitializer();
  auto oid_pri = languages_oid_index_->GetProjectedRowInitializer();
  NOISEPAGE_ASSERT(name_pri.ProjectedRowSize() >= oid_pri.ProjectedRowSize(), "PRI too small.");
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Insert into pg_languages_name_index.
  {
    auto *index_pr = name_pri.InitializeRow(buffer);
    index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    if (!languages_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {  // Conflict. Request abort.
      delete[] buffer;
      return false;
    }
  }

  // Insert into pg_languages_oid_index.
  {
    auto *index_pr = oid_pri.InitializeRow(buffer);
    index_pr->Set<language_oid_t, false>(0, oid, false);
    if (!languages_oid_index_->InsertUnique(txn, *index_pr, tuple_slot)) {  // Conflict. Request abort.
      delete[] buffer;
      return false;
    }
  }

  delete[] buffer;
  return true;
}

language_oid_t PgLanguageImpl::GetLanguageOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                              const std::string &lanname) {
  auto name_pri = languages_name_index_->GetProjectedRowInitializer();
  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_language_all_cols_pri_.ProjectedRowSize());

  auto name_pr = name_pri.InitializeRow(buffer);
  const auto name_varlen = storage::StorageUtil::CreateVarlen(lanname);

  // Find the OID from pg_languages_name_index if it exists.
  auto oid = INVALID_LANGUAGE_OID;
  {
    std::vector<storage::TupleSlot> results;
    name_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    languages_name_index_->ScanKey(*txn, *name_pr, &results);
    if (!results.empty()) {
      NOISEPAGE_ASSERT(results.size() == 1, "Unique language name index should return <= 1 result.");
      // TODO(tanujnay112): Can optimize to not extract all columns.
      // We may need all columns in the future though so doing this for now
      auto all_cols_pr = common::ManagedPointer(pg_language_all_cols_pri_.InitializeRow(buffer));
      auto found_tuple = results[0];
      languages_->Select(txn, found_tuple, all_cols_pr.Get());
      oid = *PgLanguage::LANOID.Get(all_cols_pr, pg_language_all_cols_prm_);
    }
  }

  if (name_varlen.NeedReclaim()) {
    delete[] name_varlen.Content();
  }

  delete[] buffer;
  return oid;
}

bool PgLanguageImpl::DropLanguage(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  language_oid_t oid) {
  NOISEPAGE_ASSERT(oid != INVALID_LANGUAGE_OID, "Invalid oid passed in to DropLanguage.");
  auto name_pri = languages_name_index_->GetProjectedRowInitializer();
  auto oid_pri = languages_oid_index_->GetProjectedRowInitializer();

  byte *const buffer = common::AllocationUtil::AllocateAligned(pg_language_all_cols_pri_.ProjectedRowSize());
  auto index_pr = oid_pri.InitializeRow(buffer);
  index_pr->Set<language_oid_t, false>(0, oid, false);

  // Find the OID from pg_languages_oid_index.
  std::vector<storage::TupleSlot> results;
  {
    languages_oid_index_->ScanKey(*txn, *index_pr, &results);
    if (results.empty()) {
      delete[] buffer;
      return false;
    }
    NOISEPAGE_ASSERT(results.size() == 1, "More than one non-unique result found in unique index.");
  }

  auto to_delete_slot = results[0];

  // Delete from pg_language.
  {
    txn->StageDelete(db_oid_, PgLanguage::LANGUAGE_TABLE_OID, to_delete_slot);
    if (!languages_->Delete(txn, to_delete_slot)) {
      // Someone else has a write-lock. Free the buffer and return false to indicate failure
      delete[] buffer;
      return false;
    }
  }

  // Delete from pg_languages_oid_index.
  languages_oid_index_->Delete(txn, *index_pr, to_delete_slot);

  // Delete from pg_languages_name_index by obtaining the name from pg_language.
  {
    auto table_pr = common::ManagedPointer(pg_language_all_cols_pri_.InitializeRow(buffer));
    bool UNUSED_ATTRIBUTE visible = languages_->Select(txn, to_delete_slot, table_pr.Get());
    auto name_varlen = *PgLanguage::LANNAME.Get(table_pr, pg_language_all_cols_prm_);

    index_pr = name_pri.InitializeRow(buffer);
    index_pr->Set<storage::VarlenEntry, false>(0, name_varlen, false);
    languages_name_index_->Delete(txn, *index_pr, to_delete_slot);
  }

  delete[] buffer;
  return true;
}

void PgLanguageImpl::BootstrapLanguages(const common::ManagedPointer<transaction::TransactionContext> txn,
                                        const common::ManagedPointer<DatabaseCatalog> dbc) {
  CreateLanguage(txn, "plpgsql", PgLanguage::PLPGSQL_LANGUAGE_OID);
  CreateLanguage(txn, "internal", PgLanguage::INTERNAL_LANGUAGE_OID);
}

}  // namespace noisepage::catalog::postgres
