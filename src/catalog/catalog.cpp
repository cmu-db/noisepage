#include "catalog/catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_cache.h"
#include "catalog/database_catalog.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_database.h"
#include "storage/index/index.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace noisepage::catalog {

Catalog::Catalog(const common::ManagedPointer<transaction::TransactionManager> txn_manager,
                 const common::ManagedPointer<storage::BlockStore> block_store,
                 const common::ManagedPointer<storage::GarbageCollector> garbage_collector)
    : txn_manager_(txn_manager.Get()),
      catalog_block_store_(block_store.Get()),
      garbage_collector_(garbage_collector),
      next_oid_(1) {
  databases_ = new storage::SqlTable(catalog_block_store_, postgres::Builder::GetDatabaseTableSchema());
  databases_oid_index_ = postgres::Builder::BuildUniqueIndex(postgres::Builder::GetDatabaseOidIndexSchema(),
                                                             postgres::DATABASE_OID_INDEX_OID);
  databases_name_index_ = postgres::Builder::BuildUniqueIndex(postgres::Builder::GetDatabaseNameIndexSchema(),
                                                              postgres::DATABASE_NAME_INDEX_OID);
  get_database_oid_pri_ = databases_->InitializerForProjectedRow({postgres::DATOID_COL_OID});
  get_database_catalog_pri_ = databases_->InitializerForProjectedRow({postgres::DAT_CATALOG_COL_OID});

  const std::vector<col_oid_t> pg_database_all_oids{postgres::PG_DATABASE_ALL_COL_OIDS.cbegin(),
                                                    postgres::PG_DATABASE_ALL_COL_OIDS.cend()};
  pg_database_all_cols_pri_ = databases_->InitializerForProjectedRow(pg_database_all_oids);
  pg_database_all_cols_prm_ = databases_->ProjectionMapForOids(pg_database_all_oids);

  const std::vector<col_oid_t> delete_database_entry_oids{postgres::DATNAME_COL_OID, postgres::DAT_CATALOG_COL_OID};
  delete_database_entry_pri_ = databases_->InitializerForProjectedRow(delete_database_entry_oids);
  delete_database_entry_prm_ = databases_->ProjectionMapForOids(delete_database_entry_oids);
}

void Catalog::TearDown() {
  auto *txn = txn_manager_->BeginTransaction();
  // Get a projected column on DatabaseCatalog pointers for scanning the table
  const std::vector<col_oid_t> cols{postgres::DAT_CATALOG_COL_OID};

  // Only one column, so we only need the initializer and not the ProjectionMap
  const auto pci = databases_->InitializerForProjectedColumns(cols, 100);

  // This could potentially be optimized by calculating this size and hard-coding a byte array on the stack
  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  // We've requested a single column so we know the column index is 0, and since
  // we will be reusing this same projected column the pointer to the start of
  // the column is stable.  Therefore we only need to do this cast once before
  // the loop.
  auto db_ptrs = reinterpret_cast<DatabaseCatalog **>(pc->ColumnStart(0));

  // Scan the table and accumulate the pointers into a vector
  std::vector<DatabaseCatalog *> db_cats;
  auto table_iter = databases_->begin();
  while (table_iter != databases_->end()) {
    databases_->Scan(common::ManagedPointer(txn), &table_iter, pc);

    for (uint i = 0; i < pc->NumTuples(); i++) db_cats.emplace_back(db_ptrs[i]);
  }

  // Pass vars by value except for db_cats which we move
  txn->RegisterCommitAction(
      [=, db_cats{std::move(db_cats)}](transaction::DeferredActionManager *deferred_action_manager) {
        for (auto db : db_cats) {
          auto del_action = DeallocateDatabaseCatalog(db);
          deferred_action_manager->RegisterDeferredAction(del_action);
        }
        // Pass vars to the deferral by value
        deferred_action_manager->RegisterDeferredAction([=]() {
          delete databases_oid_index_;   // Delete the OID index
          delete databases_name_index_;  // Delete the name index
          delete databases_;             // Delete the table
        });
      });

  // Deallocate the buffer (not needed if hard-coded to be on stack).
  delete[] buffer;

  // The transaction was read-only and we do not need any side-effects
  // so we use an empty lambda for the callback function.
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

bool Catalog::CreateDatabase(const common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name,
                             const bool bootstrap, const catalog::db_oid_t db_oid) {
  // Instantiate the DatabaseCatalog
  DatabaseCatalog *dbc = postgres::Builder::CreateDatabaseCatalog(catalog_block_store_, db_oid, garbage_collector_);
  txn->RegisterAbortAction([=](transaction::DeferredActionManager *deferred_action_manager) { delete dbc; });
  const bool success = Catalog::CreateDatabaseEntry(common::ManagedPointer(txn), db_oid, name, dbc);
  if (bootstrap) dbc->Bootstrap(txn);  // Bootstrap the created database
  return success;
}

db_oid_t Catalog::CreateDatabase(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const std::string &name, const bool bootstrap) {
  const db_oid_t db_oid = next_oid_++;
  const auto success = Catalog::CreateDatabase(common::ManagedPointer(txn), name, bootstrap, db_oid);
  if (!success) return INVALID_DATABASE_OID;
  return db_oid;
}

bool Catalog::DeleteDatabase(const common::ManagedPointer<transaction::TransactionContext> txn,
                             const db_oid_t database) {
  auto *const dbc = DeleteDatabaseEntry(common::ManagedPointer(txn), database);
  if (dbc == nullptr) return false;

  // Ensure the deferred action is created now in order to eagerly bind references
  // and not rely on the catalog still existing at the time the queue is processed
  auto del_action = DeallocateDatabaseCatalog(dbc);

  // Defer the de-allocation on commit because we need to scan the tables to find
  // live references at deletion that need to be deleted.
  txn->RegisterCommitAction(
      [=, del_action{std::move(del_action)}](transaction::DeferredActionManager *deferred_action_manager) {
        deferred_action_manager->RegisterDeferredAction(del_action);
      });
  return true;
}

bool Catalog::RenameDatabase(const common::ManagedPointer<transaction::TransactionContext> txn, const db_oid_t database,
                             const std::string &name) {
  // Name is indexed so this is a delete and insert at the physical level
  auto *const dbc = DeleteDatabaseEntry(common::ManagedPointer(txn), database);
  if (dbc == nullptr) return false;
  return CreateDatabaseEntry(common::ManagedPointer(txn), database, name, dbc);
}

db_oid_t Catalog::GetDatabaseOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                 const std::string &name) {
  const auto name_pri = databases_name_index_->GetProjectedRowInitializer();

  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto *pr = name_pri.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;

  std::vector<storage::TupleSlot> index_results;
  databases_name_index_->ScanKey(*txn, *pr, &index_results);

  // Clean up the varlen's buffer in the case it wasn't inlined.
  if (!name_varlen.IsInlined()) {
    delete[] name_varlen.Content();
  }

  if (index_results.empty()) {
    delete[] buffer;
    return INVALID_DATABASE_OID;
  }
  NOISEPAGE_ASSERT(index_results.size() == 1, "Database name not unique in index");

  pr = get_database_oid_pri_.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = databases_->Select(common::ManagedPointer(txn), index_results[0], pr);
  NOISEPAGE_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
  const auto db_oid = *(reinterpret_cast<const db_oid_t *const>(pr->AccessForceNotNull(0)));
  delete[] buffer;
  return db_oid;
}

common::ManagedPointer<DatabaseCatalog> Catalog::GetDatabaseCatalog(
    const common::ManagedPointer<transaction::TransactionContext> txn, const db_oid_t database) {
  const auto oid_pri = databases_name_index_->GetProjectedRowInitializer();

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *const buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  auto *pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0))) = database;

  std::vector<storage::TupleSlot> index_results;
  databases_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return common::ManagedPointer<DatabaseCatalog>(nullptr);
  }
  NOISEPAGE_ASSERT(index_results.size() == 1, "Database name not unique in index");

  pr = get_database_catalog_pri_.InitializeRow(buffer);
  const auto UNUSED_ATTRIBUTE result = databases_->Select(common::ManagedPointer(txn), index_results[0], pr);
  NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");

  const auto dbc = *(reinterpret_cast<DatabaseCatalog **>(pr->AccessForceNotNull(0)));
  delete[] buffer;
  return common::ManagedPointer(dbc);
}

std::unique_ptr<CatalogAccessor> Catalog::GetAccessor(const common::ManagedPointer<transaction::TransactionContext> txn,
                                                      db_oid_t database,
                                                      const common::ManagedPointer<CatalogCache> cache) {
  auto dbc = this->GetDatabaseCatalog(common::ManagedPointer(txn), database);
  if (dbc == nullptr) return nullptr;
  if (cache != DISABLED) {
    const auto last_ddl_change = dbc->write_lock_.load();
    const bool invalidate_cache = transaction::TransactionUtil::Committed(last_ddl_change) &&
                                  transaction::TransactionUtil::NewerThan(last_ddl_change, cache->OldestEntry());
    if (invalidate_cache) cache->Reset(txn->StartTime());
  }
  return std::make_unique<CatalogAccessor>(common::ManagedPointer(this), dbc, txn, cache);
}

bool Catalog::CreateDatabaseEntry(const common::ManagedPointer<transaction::TransactionContext> txn, const db_oid_t db,
                                  const std::string &name, DatabaseCatalog *const dbc) {
  const auto name_varlen = storage::StorageUtil::CreateVarlen(name);

  // Create the redo record for inserting into the table

  auto *const redo = txn->StageWrite(INVALID_DATABASE_OID, postgres::DATABASE_TABLE_OID, pg_database_all_cols_pri_);

  // Populate the projected row
  *(reinterpret_cast<db_oid_t *>(
      redo->Delta()->AccessForceNotNull(pg_database_all_cols_prm_[postgres::DATOID_COL_OID]))) = db;
  *(reinterpret_cast<storage::VarlenEntry *>(
      redo->Delta()->AccessForceNotNull(pg_database_all_cols_prm_[postgres::DATNAME_COL_OID]))) = name_varlen;
  *(reinterpret_cast<DatabaseCatalog **>(
      redo->Delta()->AccessForceNotNull(pg_database_all_cols_prm_[postgres::DAT_CATALOG_COL_OID]))) = dbc;

  // Insert into the table to get the tuple slot
  const auto tupleslot = databases_->Insert(common::ManagedPointer(txn), redo);

  const auto name_pri = databases_name_index_->GetProjectedRowInitializer();
  const auto oid_pri = databases_oid_index_->GetProjectedRowInitializer();

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Insert into the name index (checks for name collisions)
  auto *pr = name_pri.InitializeRow(buffer);
  // There's only a single column in the key, so there is no need to check OID to key column
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;

  if (!databases_name_index_->InsertUnique(common::ManagedPointer(txn), *pr, tupleslot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_DATABASE_OID to indicate the database was not created.
    delete[] buffer;
    return false;
  }

  // Insert into the OID index (should never fail)
  pr = oid_pri.InitializeRow(buffer);
  // There's only a single column in the key, so there is no need to check OID to key column
  *(reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0))) = db;

  const bool UNUSED_ATTRIBUTE result = databases_oid_index_->InsertUnique(common::ManagedPointer(txn), *pr, tupleslot);
  NOISEPAGE_ASSERT(result, "Assigned database OID failed to be unique");

  delete[] buffer;
  return true;
}

DatabaseCatalog *Catalog::DeleteDatabaseEntry(const common::ManagedPointer<transaction::TransactionContext> txn,
                                              db_oid_t db) {
  std::vector<storage::TupleSlot> index_results;

  const auto name_pri = databases_name_index_->GetProjectedRowInitializer();
  const auto oid_pri = databases_oid_index_->GetProjectedRowInitializer();

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *const buffer = common::AllocationUtil::AllocateAligned(delete_database_entry_pri_.ProjectedRowSize());
  auto *pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0))) = db;

  databases_oid_index_->ScanKey(*txn, *pr, &index_results);
  NOISEPAGE_ASSERT(
      index_results.size() == 1,
      "Incorrect number of results from index scan. Expect 1 because it's a unique index. 0 implies that "
      "function was called with an oid that doesn't exist in the Catalog, but binding somehow succeeded. That doesn't "
      "make sense. Was a DROP plan node reused twice? IF EXISTS should be handled in the Binder, rather than "
      "pushing logic here.");

  pr = delete_database_entry_pri_.InitializeRow(buffer);
  const auto UNUSED_ATTRIBUTE result = databases_->Select(common::ManagedPointer(txn), index_results[0], pr);

  NOISEPAGE_ASSERT(result, "Index scan did a visibility check, so Select shouldn't fail at this point.");

  txn->StageDelete(INVALID_DATABASE_OID, postgres::DATABASE_TABLE_OID, index_results[0]);
  if (!databases_->Delete(common::ManagedPointer(txn), index_results[0])) {
    // Someone else has a write-lock
    delete[] buffer;
    return nullptr;
  }

  // It is safe to use AccessForceNotNull here because we have checked the
  // tuple's visibility and because the pointer cannot be null in a running
  // database
  auto *dbc = *reinterpret_cast<DatabaseCatalog **>(
      pr->AccessForceNotNull(delete_database_entry_prm_[postgres::DAT_CATALOG_COL_OID]));
  auto name = *reinterpret_cast<storage::VarlenEntry *>(
      pr->AccessForceNotNull(delete_database_entry_prm_[postgres::DATNAME_COL_OID]));

  pr = oid_pri.InitializeRow(buffer);
  *(reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0))) = db;
  databases_oid_index_->Delete(common::ManagedPointer(txn), *pr, index_results[0]);

  pr = name_pri.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name;
  databases_name_index_->Delete(common::ManagedPointer(txn), *pr, index_results[0]);

  delete[] buffer;
  return dbc;
}

std::function<void()> Catalog::DeallocateDatabaseCatalog(DatabaseCatalog *const dbc) {
  return [=]() {
    auto txn = txn_manager_->BeginTransaction();
    dbc->TearDown(common::ManagedPointer(txn));
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    delete dbc;
  };
}

common::ManagedPointer<storage::BlockStore> Catalog::GetBlockStore() const {
  // TODO(Matt): at some point we may decide the Catalog owns this, but right now it doesn't. Taking ownership may
  // introduce life cycle issues (i.e. guaranteeing that all tables are freed and Blocks returned before this object
  // gets deleted)
  return common::ManagedPointer(catalog_block_store_);
}

}  // namespace noisepage::catalog
