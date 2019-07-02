#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_database.h"
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::catalog {

Catalog::Catalog(transaction::TransactionManager *txn_manager, storage::BlockStore *block_store)
    : txn_manager_(txn_manager), catalog_block_store_(block_store), next_oid_(1) {
  databases_ = new storage::SqlTable(block_store, postgres::Builder::GetDatabaseTableSchema());
  databases_oid_index_ =
      postgres::Builder::BuildUniqueIndex(postgres::Builder::GetDatabaseOidIndexSchema(), DATABASE_OID_INDEX_OID);
  databases_name_index_ =
      postgres::Builder::BuildUniqueIndex(postgres::Builder::GetDatabaseNameIndexSchema(), DATABASE_NAME_INDEX_OID);
}

void Catalog::TearDown() {
  auto txn = txn_manager_->BeginTransaction();
  // Get a projected column on DatabaseCatalog pointers for scanning the table
  std::vector<col_oid_t> cols;
  cols.emplace_back(DAT_CATALOG_COL_OID);
  auto [pci, pm] = databases_->InitializerForProjectedColumns(cols, 100);

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
    databases_->Scan(txn, &table_iter, pc);

    for (int i = 0; i < pc->NumTuples()) db_cats.emplace_back(db_ptrs[i]);
  }

  // Pass vars by value except for db_cats which we move
  txn->RegisterCommitAction([=, db_cats{std::move(db_cats)}]() {
    for (auto db : db_cats) {
      auto del_action = DeallocateDatabaseCatalog(db);
      txn_manager_->DeferAction(std::move(del_action));
    }
    // Pass vars to the deferral by value
    txn_manager_->DeferAction([=]() {
      delete databases_oid_index_;   // Delete the OID index
      delete databases_name_index_;  // Delete the name index
      delete databases_;             // Delete the table
    });
  });

  // Deallocate the buffer (not needed if hard-coded to be on stack).
  delete[] buffer;

  // The transaction was read-only and we do not need any side-effects
  // so we use an empty lambda for the callback function.
  txn_manager_->Commit(txn, [](void *) {}, nullptr);
}

db_oid_t Catalog::CreateDatabase(transaction::TransactionContext *txn, const std::string &name) {
  // Instantiate the DatabaseCatalog
  auto *dbc = postgres::Builder::CreateDatabaseCatalog(catalog_block_store_);
  db_oid_t db_oid = next_oid_++;
  return (Catalog::CreateDatabaseEntry(txn, db_oid, name, dbc)) ? db_oid : INVALID_DATABASE_OID;
}

bool Catalog::DeleteDatabase(transaction::TransactionContext *txn, db_oid_t database) {
  auto *dbc = DeleteDatabaseEntry(txn, database);
  if (dbc == nullptr) return false;

  // Ensure the deferred action is created now in order to eagerly bind references
  // and not rely on the catalog still existing at the time the queue is processed
  auto del_action = DeallocateDatabaseCatalog(dbc);

  // Defer the de-allocation on commit because we need to scan the tables to find
  // live references at deletion that need to be deleted.
  txn->RegisterCommitAction(
      [=, del_action{std::move(del_action)}]() { txn_manager_->DeferAction(std::move(del_action)); });
}

bool Catalog::RenameDatabase(transaction::TransactionContext *txn, db_oid_t database, const std::string &name) {
  // Name is indexed so this is a delete and insert at the physical level
  auto *dbc = DeleteDatabaseEntry(txn, database);
  if (dbc == nullptr) return false;
  return CreateDatabaseEntry(txn, database, name, dbc);
}

db_oid_t Catalog::GetDatabaseOid(transaction::TransactionContext *txn, const std::string &name) {
  std::vector<storage::TupleSlot> index_results;
  auto name_pri = databases_name_index_->GetProjectedRowInitializer();

  // Create the necessary varlen for storage operations
  storage::VarlenEntry name_varlen;
  byte *varlen_contents = nullptr;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    varlen_contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(varlen_contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(varlen_contents, name.size(), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>(name.data()), name.size());
  }

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;

  databases_name_index_->ScanKey(*txn, *pr, &index_results);
  if (varlen_contents != nullptr) {
    delete[] varlen_contents;
  }

  if (index_results.empty())
  {
    delete[] buffer;
    return INVALID_DATABASE_OID;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Database name not unique in index");

  const auto table_pri = databases_->InitializerForProjectedRow({DATOID_COL_OID}).first;
  pr = table_pri.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = databases_->Select(txn, index_results[0], pr);
  TERRIER_ASSERT(result, "Index already verified visibility. This shouldn't fail.");
  const auto db_oid = *(reinterpret_cast<const db_oid_t *const>(pr->AccessForceNotNull(0)));
  delete[] buffer;
  return db_oid;
}

common::ManagedPointer<DatabaseCatalog> Catalog::GetDatabaseCatalog(transaction::TransactionContext *txn,
                                                                    db_oid_t database) {
  std::vector<storage::TupleSlot> index_results;
  auto oid_pri = databases_name_index_->GetProjectedRowInitializer();

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *buffer = common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize());
  auto pr = oid_pri.InitializeRow(buffer);
  auto *oid = reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0));
  *oid = database;

  databases_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return common::ManagedPointer<DatabaseCatalog>(nullptr);
  }
  TERRIER_ASSERT(index_results.size() == 1, "Database name not unique in index");

  std::vector<col_oid_t> table_oids;
  table_oids.emplace_back(DAT_CATALOG_COL_OID);
  auto table_pri = databases_->InitializerForProjectedRow(table_oids).first;
  pr = table_pri.InitializeRow(buffer);
  if (!databases_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return common::ManagedPointer<DatabaseCatalog>(nullptr);
  }

  auto dbc = *reinterpret_cast<DatabaseCatalog **>(pr->AccessForceNotNull(0));
  delete[] buffer;
  return common::ManagedPointer(dbc);
}

common::ManagedPointer<DatabaseCatalog> Catalog::GetDatabaseCatalog(transaction::TransactionContext *txn,
                                                                    const std::string &name) {
  std::vector<storage::TupleSlot> index_results;
  auto name_pri = databases_name_index_->GetProjectedRowInitializer();

  // Create the necessary varlen for storage operations
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, uint32_t(name.size()), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline((byte *)(name.data()), uint32_t(name.size()));
  }

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  auto *varlen = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *varlen = name_varlen;

  databases_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return common::ManagedPointer<DatabaseCatalog>(nullptr);
  }
  TERRIER_ASSERT(index_results.size() == 1, "Database name not unique in index");

  std::vector<col_oid_t> table_oids;
  table_oids.emplace_back(DAT_CATALOG_COL_OID);
  auto table_pri = databases_->InitializerForProjectedRow(table_oids).first;
  pr = table_pri.InitializeRow(buffer);
  if (!databases_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return common::ManagedPointer<DatabaseCatalog>(nullptr);
  }

  auto dbc = *reinterpret_cast<DatabaseCatalog **> pr->AccessForceNotNull(0);
  delete[] buffer;
  return common::ManagedPointer(dbc);
}

CatalogAccessor *Catalog::GetAccessor(transaction::TransactionContext *txn, db_oid_t database) {
  auto dbc = this->GetDatabaseCatalog(txn, database);
  if (dbc == nullptr) return nullptr;
  return new CatalogAccessor(this, dbc, txn, database);
}

bool Catalog::CreateDatabaseEntry(transaction::TransactionContext *txn, db_oid_t db, const std::string &name,
                                  DatabaseCatalog *dbc) {
  // Create the necessary varlen for storage operations
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, uint32_t(name.size()), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline((byte *)(name.data()), uint32_t(name.size()));
  }
  // Ensure we delete the database if the transaction aborts
  txn->RegisterAbortAction([=]() { delete dbc; });

  // Create the redo record for inserting into the table
  std::vector<col_oid_t> table_oids;
  table_oids.emplace_back(DATOID_COL_OID);
  table_oids.emplace_back(DATNAME_COL_OID);
  table_oids.emplace_back(DAT_CATALOG_COL_OID);
  auto [pri, pm] = databases_->InitializerForProjectedRow(table_oids);
  auto *redo = txn->StageWrite(INVALID_DATABASE_OID, DATABASE_TABLE_OID, pri);

  // Populate the projected row
  auto *oid = reinterpret_cast<db_oid_t *>(redo->Delta()->AccessForceNotNull(pm[DATOID_COL_OID]));
  auto *name_v = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(pm[DATNAME_COL_OID]));
  auto *ptr = reinterpret_cast<DatabaseCatalog **>(redo->Delta()->AccessForceNotNull(pm[DAT_CATALOG_COL_OID]));
  *oid = db;
  *name_v = name_varlen;
  *ptr = dbc;

  // Insert into the table to get the tuple slot
  auto tupleslot = databases_->Insert(txn, redo);

  auto name_pri = databases_name_index_->GetProjectedRowInitializer();
  auto oid_pri = databases_oid_index_->GetProjectedRowInitializer();

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());

  // Insert into the name index (checks for name collisions)
  auto pr = name_pri.InitializeRow(buffer);
  // There's only a single column in the key, so there is no need to check OID to key column
  name_v = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_v = name_varlen;

  if (!databases_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_DATABASE_OID to indicate the database was not created.
    delete[] buffer;
    return false;
  }

  // Insert into the OID index (should never fail)
  pr = oid_pri.InitializeRow(buffer);
  // There's only a single column in the key, so there is no need to check OID to key column
  oid = reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0));
  *oid = db;

  const bool UNUSED_ATTRIBUTE result = databases_oid_index_->InsertUnique(txn, pr, tupleslot);
  TERRIER_ASSERT(result, "Assigned database OID failed to be unique");

  delete[] buffer;
  return true;
}

DatabaseCatalog *Catalog::DeleteDatabaseEntry(transaction::TransactionContext *txn, db_oid_t db) {
  std::vector<storage::TupleSlot> index_results;

  std::vector<col_oid_t> table_oids;
  table_oids.emplace_back(DATNAME_COL_OID);
  table_oids.emplace_back(DAT_CATALOG_COL_OID);
  auto pair = databases_->InitializerForProjectedRow(table_oids);
  auto table_pri = pair.first;
  auto table_pri_map = pair.second;
  auto name_pri = databases_name_index_->GetProjectedRowInitializer();
  auto oid_pri = databases_oid_index_->GetProjectedRowInitializer();

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  auto pr = oid_pri.InitializeRow(buffer);
  auto *oid = reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0));
  *oid = db;

  databases_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return nullptr;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Database OID not unique in index");

  // TODO (John):  This is wrong.  I need to fetch the pointer and varlen and
  // store both so that we can properly delete the varlen from the name index
  pr = table_pri.InitializeRow(buffer);
  if (!databases_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return nullptr;
  }

  if (!databases_->Delete(txn, index_results[0])) {
    // Someone else has a write-lock
    delete[] buffer;
    return nullptr;
  }

  // It is safe to use AccessForceNotNull here because we have checked the
  // tuple's visibility and because the pointer cannot be null in a running
  // database
  auto *dbc = *reinterpret_cast<DatabaseCatalog **> pr->AccessForceNotNull(table_pri_map[DAT_CATALOG_COL_OID]);
  auto name = *reinterpret_cast<storage::VarlenEntry *> pr->AccessForceNotNull(table_pri_map[DATNAME_COL_OID]);

  pr = oid_pri.InitializeRow(buffer);
  auto *oid_v = reinterpret_cast<db_oid_t *>(pr->AccessForceNotNull(0));
  *oid_v = db;
  // TODO (Ling): Delete function in index.h returns void, probably because the actual deletion will be deferred.
  const bool UNUSED_ATTRIBUTE idx_res_1 = databases_oid_index_->Delete(txn, *pr, index_results[0]);
  TERRIER_ASSERT(idx_res_1, "Failed to remove OID from index");

  pr = name_pri.InitializeRow(buffer);
  auto *key = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *key = name;
  // Need to set the varlen entry in the projected row to the varlen returned above
  const bool UNUSED_ATTRIBUTE idx_res_2 = databases_name_index_->Delete(txn, *pr, index_results[0]);
  TERRIER_ASSERT(idx_res_2, "Failed to remove name from index");

  delete[] buffer;
  return dbc;
}

transaction::Action Catalog::DeallocateDatabaseCatalog(DatabaseCatalog *dbc) {
  auto txn = txn_manager_->BeginTransaction();
  dbc->TearDown(txn);
  txn_manager_->Commit(txn, [](void *) {}, nullptr);
  delete dbc;
}
}  // namespace terrier::catalog
