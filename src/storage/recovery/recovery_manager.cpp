#include <algorithm>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "storage/recovery/recovery_manager.h"

#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "storage/index/index_builder.h"
#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {

uint32_t RecoveryManager::RecoverFromLogs() {
  // Replay logs until the log provider no longer gives us logs
  uint32_t txns_replayed = 0;
  while (true) {
    auto pair = log_provider_->GetNextRecord();
    auto *log_record = pair.first;

    // If we have exhausted all the logs, break from the loop
    if (log_record == nullptr) break;

    // If the record is a commit or abort, we process it by replaying all the records in the case of commits, or
    // cleaning up the records in the case of aborts. If it's not a commit or abort record, we buffer it.
    if (log_record->RecordType() == LogRecordType::COMMIT || log_record->RecordType() == LogRecordType::ABORT) {
      TERRIER_ASSERT(pair.second.empty(), "Commit or Abort records should not have any varlen pointers");
      if (log_record->RecordType() == LogRecordType::COMMIT) txns_replayed++;
      ProcessTransaction(log_record);
    } else {
      buffered_changes_map_[log_record->TxnBegin()].push_back(pair);
    }
  }
  // If we have unprocessed buffered changes, then these transactions were in-process at the time of system shutdown.
  // They are unrecoverable, so we need to clean up the memory of their records.
  if (!buffered_changes_map_.empty()) {
    for (auto &txn : buffered_changes_map_) {
      for (auto &buffered_pair : txn.second) {
        delete[] reinterpret_cast<byte *>(buffered_pair.first);
        for (auto *entry : buffered_pair.second) {
          delete[] entry;
        }
      }
    }
  }

  return txns_replayed;
}

void RecoveryManager::ProcessTransaction(LogRecord *log_record) {
  TERRIER_ASSERT(log_record->RecordType() == LogRecordType::COMMIT || log_record->RecordType() == LogRecordType::ABORT,
                 "Records should only be replayed when a commit or abort record is seen");

  // If we are aborting, we can free and discard all buffered changes. Nothing needs to be replayed
  // We flag this as unlikely as its unlikely that abort records will be flushed to disk
  if (UNLIKELY_BRANCH(log_record->RecordType() == LogRecordType::ABORT)) {
    for (auto &buffered_pair : buffered_changes_map_[log_record->TxnBegin()]) {
      delete[] reinterpret_cast<byte *>(buffered_pair.first);
      for (auto *entry : buffered_pair.second) {
        delete[] entry;
      }
    }
    buffered_changes_map_.erase(log_record->TxnBegin());
  } else {
    TERRIER_ASSERT(log_record->RecordType() == LogRecordType::COMMIT, "Should only replay when we see a commit record");
    // Begin a txn to replay changes with.
    auto *txn = txn_manager_->BeginTransaction();

    // Apply all buffered changes. They should all succeed. After applying we can safely delete the record
    for (uint32_t idx = 0; idx < buffered_changes_map_[log_record->TxnBegin()].size(); idx++) {
      auto *buffered_record = buffered_changes_map_[log_record->TxnBegin()][idx].first;
      TERRIER_ASSERT(buffered_record->RecordType() == LogRecordType::REDO ||
                         buffered_record->RecordType() == LogRecordType::DELETE,
                     "Buffered record must be a redo or delete.");

      if (IsSpecialCaseCatalogRecord(buffered_record)) {
        idx += ProcessSpecialCaseCatalogRecord(txn, &buffered_changes_map_[log_record->TxnBegin()], idx);
      } else if (buffered_record->RecordType() == LogRecordType::REDO) {
        ReplayRedoRecord(txn, buffered_record);
      } else {
        ReplayDeleteRecord(txn, buffered_record);
      }

      delete[] reinterpret_cast<byte *>(buffered_record);
    }
    buffered_changes_map_.erase(log_record->TxnBegin());
    // Commit the txn
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
  delete[] reinterpret_cast<byte *>(log_record);
}

void RecoveryManager::ReplayRedoRecord(transaction::TransactionContext *txn, LogRecord *record) {
  auto *redo_record = record->GetUnderlyingRecordBodyAs<RedoRecord>();
  auto sql_table_ptr = GetSqlTable(txn, redo_record->GetDatabaseOid(), redo_record->GetTableOid());
  if (IsInsertRecord(redo_record)) {
    // Save the old tuple slot, and reset the tuple slot in the record
    auto old_tuple_slot = redo_record->GetTupleSlot();
    redo_record->SetTupleSlot(TupleSlot(nullptr, 0));
    // Stage the write. This way the recovery operation is logged if logging is enabled.
    auto staged_record = txn->StageRecoveryWrite(record);
    // Insert will always succeed
    auto new_tuple_slot = sql_table_ptr->Insert(txn, staged_record);
    UpdateIndexesOnTable(txn, staged_record->GetDatabaseOid(), staged_record->GetTableOid(), new_tuple_slot,
                         staged_record->Delta());
    TERRIER_ASSERT(staged_record->GetTupleSlot() == new_tuple_slot,
                   "Insert should update redo record with new tuple slot");
    // Create a mapping of the old to new tuple. The new tuple slot should be used for future updates and deletes.
    tuple_slot_map_[old_tuple_slot] = new_tuple_slot;
  } else {
    auto new_tuple_slot = tuple_slot_map_[redo_record->GetTupleSlot()];
    redo_record->SetTupleSlot(new_tuple_slot);
    // Stage the write. This way the recovery operation is logged if logging is enabled
    auto staged_record = txn->StageRecoveryWrite(record);
    bool result UNUSED_ATTRIBUTE = sql_table_ptr->Update(txn, staged_record);
    TERRIER_ASSERT(result, "Buffered changes should always succeed during commit");
  }
}

void RecoveryManager::ReplayDeleteRecord(transaction::TransactionContext *txn, LogRecord *record) {
  auto *delete_record = record->GetUnderlyingRecordBodyAs<DeleteRecord>();
  // Get tuple slot
  auto new_tuple_slot = GetTupleSlotMapping(delete_record->GetTupleSlot());

  // Delete the tuple
  auto sql_table_ptr = GetSqlTable(txn, delete_record->GetDatabaseOid(), delete_record->GetTableOid());
  // Stage the delete. This way the recovery operation is logged if logging is enabled
  txn->StageDelete(delete_record->GetDatabaseOid(), delete_record->GetTableOid(), new_tuple_slot);
  bool result UNUSED_ATTRIBUTE = sql_table_ptr->Delete(txn, new_tuple_slot);
  TERRIER_ASSERT(result, "Buffered changes should always succeed during commit");
  UpdateIndexesOnTable(txn, delete_record->GetDatabaseOid(), delete_record->GetTableOid(), new_tuple_slot,
                       nullptr /* delete */);
  // We can delete the TupleSlot from the map
  tuple_slot_map_.erase(delete_record->GetTupleSlot());
}

void RecoveryManager::UpdateIndexesOnTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                                           catalog::table_oid_t table_oid, const TupleSlot &tuple_slot,
                                           ProjectedRow *table_pr) {
  bool insert = table_pr != nullptr;
  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);

  // Stores ptr to index
  std::vector<common::ManagedPointer<storage::index::Index>> indexes;

  // Stores index schemas, used to get indexcol_ids
  std::vector<catalog::IndexSchema> index_schemas;

  // We don't bootstrap the database catalog during recovery, so this means that indexes on catalog tables may not yet
  // be entries in pg_index. Thus, we hardcode these to update
  switch (!table_oid) {
    case (!catalog::DATABASE_TABLE_OID): {
      indexes.emplace_back(catalog_->databases_name_index_);
      index_schemas.push_back(catalog_->databases_name_index_->metadata_.GetSchema());

      indexes.emplace_back(catalog_->databases_oid_index_);
      index_schemas.push_back(catalog_->databases_oid_index_->metadata_.GetSchema());
      break;
    }

    case (!catalog::NAMESPACE_TABLE_OID): {
      indexes.emplace_back(db_catalog_ptr->namespaces_oid_index_);
      index_schemas.push_back(db_catalog_ptr->namespaces_oid_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->namespaces_name_index_);
      index_schemas.push_back(db_catalog_ptr->namespaces_name_index_->metadata_.GetSchema());
      break;
    }

    case (!catalog::CLASS_TABLE_OID): {
      indexes.emplace_back(db_catalog_ptr->classes_oid_index_);
      index_schemas.push_back(db_catalog_ptr->classes_oid_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->classes_name_index_);
      index_schemas.push_back(db_catalog_ptr->classes_name_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->classes_namespace_index_);
      index_schemas.push_back(db_catalog_ptr->classes_namespace_index_->metadata_.GetSchema());
      break;
    }

    case (!catalog::COLUMN_TABLE_OID): {
      indexes.emplace_back(db_catalog_ptr->columns_oid_index_);
      index_schemas.push_back(db_catalog_ptr->columns_oid_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->columns_name_index_);
      index_schemas.push_back(db_catalog_ptr->columns_name_index_->metadata_.GetSchema());
      break;
    }

    case (!catalog::CONSTRAINT_TABLE_OID): {
      indexes.emplace_back(db_catalog_ptr->constraints_oid_index_);
      index_schemas.push_back(db_catalog_ptr->constraints_oid_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->constraints_name_index_);
      index_schemas.push_back(db_catalog_ptr->constraints_name_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->constraints_namespace_index_);
      index_schemas.push_back(db_catalog_ptr->constraints_namespace_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->constraints_table_index_);
      index_schemas.push_back(db_catalog_ptr->constraints_table_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->constraints_index_index_);
      index_schemas.push_back(db_catalog_ptr->constraints_index_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->constraints_foreigntable_index_);
      index_schemas.push_back(db_catalog_ptr->constraints_foreigntable_index_->metadata_.GetSchema());
      break;
    }

    case (!catalog::INDEX_TABLE_OID): {
      indexes.emplace_back(db_catalog_ptr->indexes_oid_index_);
      index_schemas.push_back(db_catalog_ptr->indexes_oid_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->indexes_table_index_);
      index_schemas.push_back(db_catalog_ptr->indexes_table_index_->metadata_.GetSchema());
      break;
    }

    case (!catalog::TYPE_TABLE_OID): {
      indexes.emplace_back(db_catalog_ptr->types_oid_index_);
      index_schemas.push_back(db_catalog_ptr->types_oid_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->types_name_index_);
      index_schemas.push_back(db_catalog_ptr->types_name_index_->metadata_.GetSchema());

      indexes.emplace_back(db_catalog_ptr->types_namespace_index_);
      index_schemas.push_back(db_catalog_ptr->types_namespace_index_->metadata_.GetSchema());
      break;
    }

    default:  // Non-catalog table
      auto index_oids = db_catalog_ptr->GetIndexes(txn, table_oid);
      indexes.reserve(index_oids.size());
      index_schemas.reserve(index_oids.size());

      for (auto &oid : index_oids) {
        // Get index ptr
        auto index_ptr = db_catalog_ptr->GetIndex(txn, oid);
        indexes.push_back(index_ptr);

        // Get index schema
        auto schema = db_catalog_ptr->GetIndexSchema(txn, oid);
        index_schemas.push_back(schema);
      }
  }

  // If there's no indexes on the table, we can return
  if (indexes.empty()) return;

  // TODO(Gus): We can save ourselves a catalog query by passing in the sql table ptr to this function
  auto sql_table_ptr = GetSqlTable(txn, db_oid, table_oid);

  // Buffer for projected row sizes
  uint32_t index_byte_size = 0;

  // Stores all the attributes a table is indexed on so we can fetch their values with a single select
  std::unordered_set<catalog::col_oid_t> all_indexed_attributes;

  // Cache the index key col oid map so we dont have to compute it again later. Calling GetIndexedColOids is potentially
  // expensive
  std::unordered_map<common::ManagedPointer<storage::index::Index>,
                     std::unordered_map<catalog::indexkeycol_oid_t, std::vector<catalog::col_oid_t>>>
      indexed_attributes_map;

  // Determine all indexed attributes. Also compute largest PR size we need for index PRs.
  for (uint32_t i = 0; i < indexes.size(); i++) {
    auto index_ptr = indexes[i];
    auto &schema = index_schemas[i];

    // Get attributes
    auto indexed_attributes = schema.GetIndexedColOids();
    indexed_attributes_map[index_ptr] = indexed_attributes;
    for (auto &iter : indexed_attributes) {
      all_indexed_attributes.insert(iter.second.begin(), iter.second.end());
    }

    // We want to calculate the size of the largest PR we will need to create create
    index_byte_size = std::max(index_byte_size, index_ptr->GetProjectedRowInitializer().ProjectedRowSize());
  }

  // The PR map varies whether we are doing an insert or delete. In an insert, the table_pr has all the attributes. For
  // a delete, we have to query the table, so we only fetch the attributes we need.
  ProjectionMap pr_map;
  byte *table_buffer = nullptr;

  if (insert) {
    pr_map = sql_table_ptr->ProjectionMapForAllOids();
  } else {
    // If we need to delete from the index, then we don't have the table PR a priori, so we need to perform a Select.
    std::vector<catalog::col_oid_t> col_oids =
        std::vector<catalog::col_oid_t>(all_indexed_attributes.begin(), all_indexed_attributes.end());
    pr_map = sql_table_ptr->ProjectionMapForOids(col_oids);
    auto pr_init = sql_table_ptr->InitializerForProjectedRow(col_oids);
    table_buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
    table_pr = pr_init.InitializeRow(table_buffer);
    sql_table_ptr->Select(txn, tuple_slot, table_pr);
  }

  // Allocate index buffer
  auto *index_buffer = common::AllocationUtil::AllocateAligned(index_byte_size);

  // TODO(Gus): We are going to assume no indexes on expressions below. Having indexes on expressions would require to
  // evaluate expressions and that's a fucking nightmare
  for (uint8_t i = 0; i < indexes.size(); i++) {
    auto index = indexes[i];
    auto schema = index_schemas[i];

    // Build the index PR
    auto *index_pr = index->GetProjectedRowInitializer().InitializeRow(index_buffer);

    // Copy in each value from the table select result into the index PR
    for (const auto &col : schema.GetColumns()) {
      auto index_col_oid = col.Oid();
      auto &index_key_oids = indexed_attributes_map[index][col.Oid()];
      TERRIER_ASSERT(index_key_oids.size() == 1, "Only support index keys that are a single column oid");
      auto oid = index_key_oids[0];
      if (table_pr->IsNull(pr_map[oid])) {
        index_pr->SetNull(index->GetKeyOidToOffsetMap().at(index_col_oid));
      } else {
        auto size = col.AttrSize() & INT8_MAX;
        std::memcpy(index_pr->AccessForceNotNull(index->GetKeyOidToOffsetMap().at(index_col_oid)),
                    table_pr->AccessWithNullCheck(pr_map[oid]), size);
      }
    }

    if (insert) {
      bool result UNUSED_ATTRIBUTE = (index->GetConstraintType() == index::ConstraintType::UNIQUE)
                                         ? index->InsertUnique(txn, *index_pr, tuple_slot)
                                         : index->Insert(txn, *index_pr, tuple_slot);
      TERRIER_ASSERT(result, "Insert into index should always succeed for a committed transaction");
    } else {
      index->Delete(txn, *index_pr, tuple_slot);
    }
  }

  delete[] table_buffer;
  delete[] index_buffer;
}

uint32_t RecoveryManager::ProcessSpecialCaseCatalogRecord(
    transaction::TransactionContext *txn, std::vector<std::pair<LogRecord *, std::vector<byte *>>> *buffered_changes,
    uint32_t start_idx) {
  auto *curr_record = buffered_changes->at(start_idx).first;
  if (curr_record->RecordType() == LogRecordType::REDO) {
    auto *redo_record = curr_record->GetUnderlyingRecordBodyAs<RedoRecord>();
    auto table_oid = redo_record->GetTableOid();

    TERRIER_ASSERT(table_oid == catalog::DATABASE_TABLE_OID || table_oid == catalog::CLASS_TABLE_OID,
                   "Special case redo records should only modify pg_class or pg_database");

    if (table_oid == catalog::CLASS_TABLE_OID) {
      TERRIER_ASSERT(!IsInsertRecord(redo_record), "Special case pg_class record should only be updates");
      auto db_catalog = GetDatabaseCatalog(txn, redo_record->GetDatabaseOid());

      // Updates to pg_class will happen in the following 3 cases:
      //  1. If we update the next col oid. In this case, we don't need to do anything special, just apply the update
      //  2. If we update the schema column, we need to check if this a DDL change (add/drop column). We don't do
      //  anything yet because we don't support DDL changes in the catalog
      //  3. If we update the ptr column, this means we've inserted a new object and we need to recreate the object, and
      //  set the pointer again.
      auto pg_class_ptr = db_catalog->classes_;
      auto redo_record_oids = GetOidsForRedoRecord(pg_class_ptr, redo_record);
      TERRIER_ASSERT(redo_record_oids.size() == 1, "Updates to pg_class should only touch one column");
      auto updated_pg_class_oid = redo_record_oids[0];

      if (updated_pg_class_oid == catalog::REL_NEXTCOLOID_COL_OID) {  // Case 1
        ReplayRedoRecord(txn, curr_record);
        return 0;  // No additional logs processed
      }

      if (updated_pg_class_oid == catalog::REL_SCHEMA_COL_OID) {  // Case 2
        // TODO(Gus): Add support for recovering DDL changes.
        return 0;  // No additional logs processed
      }

      if (updated_pg_class_oid == catalog::REL_PTR_COL_OID) {  // Case 3
        // An update to the ptr column of pg_class means that we have inserted all necessary metadata into the other
        // catalog tables, and we can now recreate the object
        // Step 1: Get the class oid and kind for the object we're updating
        std::vector<catalog::col_oid_t> col_oids = {catalog::RELOID_COL_OID, catalog::RELKIND_COL_OID};
        auto pr_init = pg_class_ptr->InitializerForProjectedRow(col_oids);
        auto pr_map = pg_class_ptr->ProjectionMapForOids(col_oids);
        auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
        auto *pr = pr_init.InitializeRow(buffer);
        pg_class_ptr->Select(txn, GetTupleSlotMapping(redo_record->GetTupleSlot()), pr);
        auto class_oid = *(reinterpret_cast<uint32_t *>(pr->AccessWithNullCheck(pr_map[catalog::RELOID_COL_OID])));
        auto class_kind = *(reinterpret_cast<catalog::postgres::ClassKind *>(
            pr->AccessWithNullCheck(pr_map[catalog::RELKIND_COL_OID])));

        // Case on whether we are creating a table or index
        if (class_kind == catalog::postgres::ClassKind::REGULAR_TABLE) {
          // Step 2: Query pg_attribute for the columns of the table
          auto schema_cols = db_catalog->GetColumns<catalog::Schema::Column, catalog::table_oid_t, catalog::col_oid_t>(
              txn, catalog::table_oid_t(class_oid));

          // Step 3: Create and set schema in catalog
          auto *schema = new catalog::Schema(std::move(schema_cols));
          bool result UNUSED_ATTRIBUTE =
              db_catalog->SetTableSchemaPointer(txn, catalog::table_oid_t(class_oid), schema);
          TERRIER_ASSERT(result, "Setting table schema pointer should succeed, entry should be in pg_class already");

          // Step 4: Create and set table pointers in catalog
          storage::SqlTable *sql_table;
          if (class_oid < START_OID) {  // All catalog tables/indexes have OIDS less than START_OID
            sql_table = GetSqlTable(txn, redo_record->GetDatabaseOid(), catalog::table_oid_t(class_oid)).get();
          } else {
            sql_table = new SqlTable(block_store_, *schema);
          }
          result = db_catalog->SetTablePointer(txn, catalog::table_oid_t(class_oid), sql_table);
          TERRIER_ASSERT(result, "Setting table pointer should succeed, entry should be in pg_class already");

          // Step 5: Update catalog oid
          db_catalog->UpdateNextOid(class_oid);

          delete[] buffer;
          return 0;  // No additional records processed
        }

        if (class_kind == catalog::postgres::ClassKind::INDEX) {
          // Step 2: Query pg_attribute for the columns of the index
          auto index_cols =
              db_catalog->GetColumns<catalog::IndexSchema::Column, catalog::index_oid_t, catalog::indexkeycol_oid_t>(
                  txn, catalog::index_oid_t(class_oid));

          // Step 3: Query pg_index for the metadata we need for the index schema
          auto pg_indexes_index = db_catalog->indexes_oid_index_;
          pr = pg_indexes_index->GetProjectedRowInitializer().InitializeRow(buffer);
          *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0))) = class_oid;
          std::vector<TupleSlot> tuple_slot_result;
          pg_indexes_index->ScanKey(*txn, *pr, &tuple_slot_result);
          TERRIER_ASSERT(tuple_slot_result.size() == 1, "Index scan should yield one result");

          // NOLINTNEXTLINE
          col_oids.clear();
          col_oids = {catalog::INDISUNIQUE_COL_OID, catalog::INDISPRIMARY_COL_OID, catalog::INDISEXCLUSION_COL_OID,
                      catalog::INDIMMEDIATE_COL_OID};
          auto pg_index_pr_init = db_catalog->indexes_->InitializerForProjectedRow(col_oids);
          auto pg_index_pr_map = db_catalog->indexes_->ProjectionMapForOids(col_oids);
          delete[] buffer;  // Delete old buffer, it won't be large enough for this PR
          buffer = common::AllocationUtil::AllocateAligned(pg_index_pr_init.ProjectedRowSize());
          pr = pg_index_pr_init.InitializeRow(buffer);
          bool result UNUSED_ATTRIBUTE = db_catalog->indexes_->Select(txn, tuple_slot_result[0], pr);
          TERRIER_ASSERT(result, "Select into pg_index should succeed during recovery");
          bool is_unique =
              *(reinterpret_cast<bool *>(pr->AccessWithNullCheck(pg_index_pr_map[catalog::INDISUNIQUE_COL_OID])));
          bool is_primary =
              *(reinterpret_cast<bool *>(pr->AccessWithNullCheck(pg_index_pr_map[catalog::INDISPRIMARY_COL_OID])));
          bool is_exclusion =
              *(reinterpret_cast<bool *>(pr->AccessWithNullCheck(pg_index_pr_map[catalog::INDISEXCLUSION_COL_OID])));
          bool is_immediate =
              *(reinterpret_cast<bool *>(pr->AccessWithNullCheck(pg_index_pr_map[catalog::INDIMMEDIATE_COL_OID])));

          // Step 4: Create and set IndexSchema in catalog
          auto *index_schema = new catalog::IndexSchema(index_cols, is_unique, is_primary, is_exclusion, is_immediate);
          result = db_catalog->SetIndexSchemaPointer(txn, catalog::index_oid_t(class_oid), index_schema);
          TERRIER_ASSERT(result, "Setting index schema pointer should succeed, entry should be in pg_class already");

          // Step 5: Create and set index pointer in catalog
          storage::index::Index *index;
          if (class_oid < START_OID) {  // All catalog tables/indexes have OIDS less than START_OID
            index = GetCatalogIndex(catalog::index_oid_t(class_oid), db_catalog);
          } else {
            index = index::IndexBuilder()
                        .SetOid(catalog::index_oid_t(class_oid))
                        .SetConstraintType(is_unique ? index::ConstraintType::UNIQUE : index::ConstraintType::DEFAULT)
                        .SetKeySchema(*index_schema)
                        .Build();
          }
          result = db_catalog->SetIndexPointer(txn, catalog::index_oid_t(class_oid), index);
          TERRIER_ASSERT(result, "Setting index pointer should succeed, entry should be in pg_class already");

          // Step 6: Update catalog oid
          db_catalog->UpdateNextOid(class_oid);

          delete[] buffer;
          return 0;
        }

        TERRIER_ASSERT(false, "Only support recovery of regular tables and indexes");

      } else {
        throw std::runtime_error("Unexpected oid updated during replay of update to pg_class");
      }
    }
    // An insert into pg_database is a special case because we need the catalog to actually create the necessary
    // database catalog objects
    TERRIER_ASSERT(redo_record->GetTableOid() == catalog::DATABASE_TABLE_OID,
                   "Special case for Redo should be on pg_class or pg_database");
    TERRIER_ASSERT(IsInsertRecord(redo_record), "Special case on pg_database should only be insert");

    // Step 1: Extract inserted values from the PR in redo record
    storage::SqlTable *pg_database = catalog_->databases_;
    auto pr_map = pg_database->ProjectionMapForOids(GetOidsForRedoRecord(pg_database, redo_record));
    TERRIER_ASSERT(pr_map.find(catalog::DATOID_COL_OID) != pr_map.end(), "PR Map must contain database oid");
    TERRIER_ASSERT(pr_map.find(catalog::DATNAME_COL_OID) != pr_map.end(), "PR Map must contain database name");
    catalog::db_oid_t db_oid(
        *(reinterpret_cast<uint32_t *>(redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::DATOID_COL_OID]))));
    VarlenEntry name_varlen =
        *(reinterpret_cast<VarlenEntry *>(redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::DATNAME_COL_OID])));
    std::string name_string(name_varlen.StringView());

    // Step 2: Recreate the database
    auto result UNUSED_ATTRIBUTE = catalog_->CreateDatabase(txn, name_string, false, db_oid);
    TERRIER_ASSERT(result, "Database recreation should succeed");
    catalog_->UpdateNextOid(db_oid);

    // Step 3: Update metadata. We need to use the indexes on pg_database to find what tuple slot we just inserted into.
    // We get the new tuple slot using the oid index.
    auto pg_database_oid_index = catalog_->databases_oid_index_;
    auto pr_init = pg_database_oid_index->GetProjectedRowInitializer();
    auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
    auto *pr = pr_init.InitializeRow(buffer);
    *(reinterpret_cast<catalog::db_oid_t *>(pr->AccessForceNotNull(0))) = db_oid;
    std::vector<TupleSlot> tuple_slot_result;
    pg_database_oid_index->ScanKey(*txn, *pr, &tuple_slot_result);
    TERRIER_ASSERT(tuple_slot_result.size() == 1, "Index scan should only yield one result");
    tuple_slot_map_[redo_record->GetTupleSlot()] = tuple_slot_result[0];
    delete[] buffer;

    return 0;  // No additional records processed
  }

  TERRIER_ASSERT(curr_record->RecordType() == LogRecordType::DELETE, "Only delete records should reach this point");
  auto *delete_record = curr_record->GetUnderlyingRecordBodyAs<DeleteRecord>();

  // A delete into pg_class has two special cases. Either we are deleting an object, or renaming a table (we assume
  // you can't rename an index). For it to be a table rename, a lot of special conditions need to be true:
  //  1. There is at least one additional record following the delete record
  //  2. This additional record is a redo
  //  3. This additional record is a redo into the same database and pg_class, and is an insert record
  //  4. This additional record inserts an object with the same OID as the one that was being deleted by the redo
  //  record
  // The above conditions are documented in the code below
  // If any of the above conditions are not true, then this is just a drop table/index.
  if (delete_record->GetTableOid() == catalog::CLASS_TABLE_OID) {
    // Step 1: Determine the object oid and type that is being deleted
    storage::SqlTable *pg_class = GetDatabaseCatalog(txn, delete_record->GetDatabaseOid())->classes_;
    std::vector<catalog::col_oid_t> col_oids = {catalog::RELOID_COL_OID, catalog::RELKIND_COL_OID};
    auto pr_init = pg_class->InitializerForProjectedRow(col_oids);
    auto pr_map = pg_class->ProjectionMapForOids(col_oids);
    auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
    auto *pr = pr_init.InitializeRow(buffer);
    pg_class->Select(txn, GetTupleSlotMapping(delete_record->GetTupleSlot()), pr);
    auto class_oid = *(reinterpret_cast<uint32_t *>(pr->AccessWithNullCheck(pr_map[catalog::RELOID_COL_OID])));
    auto class_kind =
        *(reinterpret_cast<catalog::postgres::ClassKind *>(pr->AccessWithNullCheck(pr_map[catalog::RELKIND_COL_OID])));
    delete[] buffer;

    // Step 2: We need to handle the case where we are just renaming a table, in this case we don't wan't to delete
    // the table object. A rename appears as a delete followed by an insert with the same OID.
    if (start_idx + 1 < buffered_changes->size()) {  // Condition 1: there is one more record
      auto *next_record = buffered_changes->at(start_idx + 1).first;
      if (next_record->RecordType() == LogRecordType::REDO) {  // Condition 2: next record is a redo record
        auto *next_redo_record = next_record->GetUnderlyingRecordBodyAs<RedoRecord>();
        if (next_redo_record->GetDatabaseOid() == delete_record->GetDatabaseOid() &&
            next_redo_record->GetTableOid() == delete_record->GetTableOid() &&
            IsInsertRecord(next_redo_record)) {  // Condition 3: next record is an insert into the same pg_class
          // Step 3: Get the oid and kind of the object being inserted
          auto pr_map = pg_class->ProjectionMapForOids(GetOidsForRedoRecord(pg_class, next_redo_record));
          TERRIER_ASSERT(pr_map.find(catalog::RELOID_COL_OID) != pr_map.end(), "PR Map must contain class oid");
          TERRIER_ASSERT(pr_map.find(catalog::RELNAME_COL_OID) != pr_map.end(), "PR Map must contain class name");
          TERRIER_ASSERT(pr_map.find(catalog::RELKIND_COL_OID) != pr_map.end(), "PR Map must contain class kind");
          auto next_class_oid = *(reinterpret_cast<uint32_t *>(
              next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::RELOID_COL_OID])));
          auto next_class_kind UNUSED_ATTRIBUTE = *(reinterpret_cast<catalog::postgres::ClassKind *>(
              next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::RELKIND_COL_OID])));

          if (class_oid == next_class_oid) {  // Condition 4: If the oid matches on the next record, this is a renaming
            TERRIER_ASSERT(class_kind == catalog::postgres::ClassKind::REGULAR_TABLE && class_kind == next_class_kind,
                           "We only allow renaming of tables");
            // Step 4: Extract out the new name
            VarlenEntry name_varlen = *(reinterpret_cast<VarlenEntry *>(
                next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::RELNAME_COL_OID])));
            std::string name_string(name_varlen.StringView());

            // Step 5: Rename the table
            auto result UNUSED_ATTRIBUTE = GetDatabaseCatalog(txn, next_redo_record->GetDatabaseOid())
                                               ->RenameTable(txn, catalog::table_oid_t(next_class_oid), name_string);
            TERRIER_ASSERT(result, "Renaming should always succeed during replaying");

            // Step 6: Update metadata and clean up additional record processed. We need to use the indexes on
            // pg_class to find what tuple slot we just inserted into. We get the new tuple slot using the oid index.
            auto pg_class_oid_index = GetDatabaseCatalog(txn, next_redo_record->GetDatabaseOid())->classes_oid_index_;
            auto pr_init = pg_class_oid_index->GetProjectedRowInitializer();
            buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
            pr = pr_init.InitializeRow(buffer);
            *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0))) = next_class_oid;
            std::vector<TupleSlot> tuple_slot_result;
            pg_class_oid_index->ScanKey(*txn, *pr, &tuple_slot_result);
            TERRIER_ASSERT(tuple_slot_result.size() == 1, "Index scan should only yield one result");
            tuple_slot_map_[next_redo_record->GetTupleSlot()] = tuple_slot_result[0];
            delete[] buffer;
            tuple_slot_map_.erase(delete_record->GetTupleSlot());
            delete[] reinterpret_cast<byte *>(next_redo_record);

            return 1;  // We processed an additional record
          }
        }
      }
    }

    // Step 3: If it was not a renaming, we call to the catalog to delete the class object
    bool result UNUSED_ATTRIBUTE;
    if (class_kind == catalog::postgres::ClassKind::REGULAR_TABLE) {
      result =
          GetDatabaseCatalog(txn, delete_record->GetDatabaseOid())->DeleteTable(txn, catalog::table_oid_t(class_oid));
    } else if (class_kind == catalog::postgres::ClassKind::INDEX) {
      result =
          GetDatabaseCatalog(txn, delete_record->GetDatabaseOid())->DeleteIndex(txn, catalog::index_oid_t(class_oid));
    } else {
      TERRIER_ASSERT(false, "We only support replaying of dropping of tables and indexes");
    }
    TERRIER_ASSERT(result, "Table/index DROP should always succeed");

    // Step 5: Clean up metadata
    tuple_slot_map_.erase(delete_record->GetTupleSlot());

    return 0;  // No additional logs processed
  }

  // A delete into pg_attribute means we are deleting a column. There are two cases:
  //  1. Drop column: This requires some additional processing to actually clean up the column
  //  2. Cascading delete from drop table: In this case, we don't process the record because the DeleteTable catalog
  //  function will clean up the columns
  if (delete_record->GetTableOid() == catalog::COLUMN_TABLE_OID) {
    return 0;  // Case 2, no additional records processed
  }

  // A delete into pg_index means we are dropping an index. In this case, we dont process the record because the
  // DeleteIndex catalog function will cleanup the entry in pg_index for us.
  if (delete_record->GetTableOid() == catalog::INDEX_TABLE_OID) {
    return 0;  // No additional logs processed
  }

  TERRIER_ASSERT(delete_record->GetTableOid() == catalog::DATABASE_TABLE_OID,
                 "Special case for delete should be on pg_class or pg_database");

  // Step 1: Determine the database oid for the database that is being deleted
  storage::SqlTable *pg_database = catalog_->databases_;
  // NOLINTNEXTLINE
  auto pr_init = pg_database->InitializerForProjectedRow({catalog::DATOID_COL_OID});
  auto pr_map = pg_database->ProjectionMapForOids({catalog::DATOID_COL_OID});
  auto *buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
  auto *pr = pr_init.InitializeRow(buffer);
  pg_database->Select(txn, GetTupleSlotMapping(delete_record->GetTupleSlot()), pr);
  auto db_oid = *(reinterpret_cast<catalog::db_oid_t *>(pr->AccessWithNullCheck(pr_map[catalog::DATOID_COL_OID])));
  delete[] buffer;

  // Step 2: We need to handle the case where we are just renaming a database, in this case we don't wan't to delete
  // the database. A rename appears as a delete followed by an insert with the same OID. See code comment above delete
  // records into pg_class for more detailed breakdown.
  if (start_idx + 1 < buffered_changes->size()) {  // there is one more record
    auto *next_record = buffered_changes->at(start_idx + 1).first;
    if (next_record->RecordType() == LogRecordType::REDO) {  // next record is a redo record
      auto *next_redo_record = next_record->GetUnderlyingRecordBodyAs<RedoRecord>();
      if (next_redo_record->GetDatabaseOid() == delete_record->GetDatabaseOid() &&
          next_redo_record->GetTableOid() == delete_record->GetTableOid() &&
          IsInsertRecord(next_redo_record)) {  // next record is an insert into the same pg_class
        // Step 3: Get the oid and name for the database being created
        auto pr_map = pg_database->ProjectionMapForOids(GetOidsForRedoRecord(pg_database, next_redo_record));
        TERRIER_ASSERT(pr_map.find(catalog::DATOID_COL_OID) != pr_map.end(), "PR Map must contain class oid");
        TERRIER_ASSERT(pr_map.find(catalog::DATNAME_COL_OID) != pr_map.end(), "PR Map must contain class name");
        auto next_db_oid = *(reinterpret_cast<catalog::db_oid_t *>(
            next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::DATOID_COL_OID])));

        // If the oid matches on the next record, this is a renaming
        if (db_oid == next_db_oid) {
          // Step 4: Extract out the new name
          VarlenEntry name_varlen = *(reinterpret_cast<VarlenEntry *>(
              next_redo_record->Delta()->AccessWithNullCheck(pr_map[catalog::DATNAME_COL_OID])));
          std::string name_string(name_varlen.StringView());

          // Step 5: Rename the database
          auto result UNUSED_ATTRIBUTE = catalog_->RenameDatabase(txn, next_db_oid, name_string);
          TERRIER_ASSERT(result, "Renaming of database should always succeed during replaying");

          // Step 6: Update metadata and clean up additional record processed. We need to use the indexes on
          // pg_database to find what tuple slot we just inserted into. We get the new tuple slot using the oid
          // index.
          auto pg_database_oid_index = catalog_->databases_oid_index_;
          auto pr_init = pg_database_oid_index->GetProjectedRowInitializer();
          buffer = common::AllocationUtil::AllocateAligned(pr_init.ProjectedRowSize());
          pr = pr_init.InitializeRow(buffer);
          *(reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0))) = static_cast<uint32_t>(next_db_oid);
          std::vector<TupleSlot> tuple_slot_result;
          pg_database_oid_index->ScanKey(*txn, *pr, &tuple_slot_result);
          TERRIER_ASSERT(tuple_slot_result.size() == 1, "Index scan should only yield one result");
          tuple_slot_map_[next_redo_record->GetTupleSlot()] = tuple_slot_result[0];
          delete[] buffer;
          tuple_slot_map_.erase(delete_record->GetTupleSlot());
          delete[] reinterpret_cast<byte *>(next_redo_record);

          return 1;  // We processed an additional record
        }
      }
    }
  }

  // Step 3: If it wasn't a renaming, we simply need to drop the database
  auto result UNUSED_ATTRIBUTE = catalog_->DeleteDatabase(txn, db_oid);
  TERRIER_ASSERT(result, "Database deletion should succeed");

  // Step 4: Clean up any metadata
  tuple_slot_map_.erase(delete_record->GetTupleSlot());
  return 0;  // No additional logs processed
}

common::ManagedPointer<storage::SqlTable> RecoveryManager::GetSqlTable(transaction::TransactionContext *txn,
                                                                       const catalog::db_oid_t db_oid,
                                                                       const catalog::table_oid_t table_oid) {
  if (table_oid == catalog::DATABASE_TABLE_OID) {
    return common::ManagedPointer(catalog_->databases_);
  }

  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);

  common::ManagedPointer<storage::SqlTable> table_ptr = nullptr;

  switch (!table_oid) {
    case (!catalog::CLASS_TABLE_OID): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->classes_);
      break;
    }
    case (!catalog::NAMESPACE_TABLE_OID): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->namespaces_);
      break;
    }
    case (!catalog::COLUMN_TABLE_OID): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->columns_);
      break;
    }
    case (!catalog::CONSTRAINT_TABLE_OID): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->constraints_);
      break;
    }
    case (!catalog::INDEX_TABLE_OID): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->indexes_);
      break;
    }
    case (!catalog::TYPE_TABLE_OID): {
      table_ptr = common::ManagedPointer(db_catalog_ptr->types_);
      break;
    }
    default:
      table_ptr = db_catalog_ptr->GetTable(txn, table_oid);
  }

  TERRIER_ASSERT(table_ptr != nullptr, "Table is not in the catalog for the given oid");
  return table_ptr;
}

std::vector<catalog::col_oid_t> RecoveryManager::GetOidsForRedoRecord(storage::SqlTable *sql_table,
                                                                      RedoRecord *record) {
  std::vector<catalog::col_oid_t> result;
  for (uint16_t i = 0; i < record->Delta()->NumColumns(); i++) {
    col_id_t col_id = record->Delta()->ColumnIds()[i];
    // We should ingore the version pointer column, this is a hidden storage layer column
    if (col_id != VERSION_POINTER_COLUMN_ID) {
      result.emplace_back(sql_table->OidForColId(col_id));
    }
  }
  return result;
}

storage::index::Index *RecoveryManager::GetCatalogIndex(
    const catalog::index_oid_t oid, const common::ManagedPointer<catalog::DatabaseCatalog> &db_catalog) {
  TERRIER_ASSERT((!oid) < START_OID, "Oid must be a valid catalog oid");

  switch (!oid) {
    case (!catalog::NAMESPACE_OID_INDEX_OID): {
      return db_catalog->namespaces_oid_index_;
    }

    case (!catalog::NAMESPACE_NAME_INDEX_OID): {
      return db_catalog->namespaces_name_index_;
    }

    case (!catalog::CLASS_OID_INDEX_OID): {
      return db_catalog->classes_oid_index_;
    }

    case (!catalog::CLASS_NAME_INDEX_OID): {
      return db_catalog->classes_name_index_;
    }

    case (!catalog::CLASS_NAMESPACE_INDEX_OID): {
      return db_catalog->classes_namespace_index_;
    }

    case (!catalog::INDEX_OID_INDEX_OID): {
      return db_catalog->indexes_oid_index_;
    }

    case (!catalog::INDEX_TABLE_INDEX_OID): {
      return db_catalog->indexes_table_index_;
    }

    case (!catalog::COLUMN_OID_INDEX_OID): {
      return db_catalog->columns_oid_index_;
    }

    case (!catalog::COLUMN_NAME_INDEX_OID): {
      return db_catalog->columns_name_index_;
    }

    case (!catalog::TYPE_OID_INDEX_OID): {
      return db_catalog->types_oid_index_;
    }

    case (!catalog::TYPE_NAME_INDEX_OID): {
      return db_catalog->types_name_index_;
    }

    case (!catalog::TYPE_NAMESPACE_INDEX_OID): {
      return db_catalog->types_namespace_index_;
    }

    case (!catalog::CONSTRAINT_OID_INDEX_OID): {
      return db_catalog->constraints_oid_index_;
    }

    case (!catalog::CONSTRAINT_NAME_INDEX_OID): {
      return db_catalog->constraints_name_index_;
    }

    case (!catalog::CONSTRAINT_NAMESPACE_INDEX_OID): {
      return db_catalog->constraints_namespace_index_;
    }

    case (!catalog::CONSTRAINT_TABLE_INDEX_OID): {
      return db_catalog->constraints_table_index_;
    }

    case (!catalog::CONSTRAINT_INDEX_INDEX_OID): {
      return db_catalog->constraints_index_index_;
    }

    case (!catalog::CONSTRAINT_FOREIGNTABLE_INDEX_OID): {
      return db_catalog->constraints_foreigntable_index_;
    }

    default:
      throw std::runtime_error("This oid does not belong to any catalog index");
  }
}

}  // namespace terrier::storage
