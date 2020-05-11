#include "test_util/sql_table_test_util.h"

#include <utility>
#include <vector>

#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "test_util/catalog_test_util.h"

namespace terrier {

RandomSqlTableTransaction::RandomSqlTableTransaction(LargeSqlTableTestObject *test_object)
    : test_object_(test_object), txn_(test_object->txn_manager_->BeginTransaction()), aborted_(false) {}

void RandomSqlTableTransaction::UpdateSchema(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid,
                                             const std::vector<catalog::Schema::Column> &columns,
                                             storage::layout_version_t new_version) {
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];
  auto sql_table_ptr = test_object_->catalog_->GetDatabaseCatalog(common::ManagedPointer(txn_), database_oid)
                           ->GetTable(common::ManagedPointer(txn_), table_oid);

  std::vector<catalog::col_oid_t> new_oids;
  new_oids.reserve(columns.size());
  for (auto &col : columns) {
    new_oids.push_back(col.Oid());
  }

  auto schema = std::make_unique<catalog::Schema>(columns);

  // update schema, with new layout_version, and schema with new column added
  sql_table_ptr->UpdateSchema(common::ManagedPointer<transaction::TransactionContext>(txn_), *schema, new_version);

  auto initializer = sql_table_ptr->InitializerForProjectedRow(new_oids, new_version);
  sql_table_metadata->pris_.push_back(initializer);

  sql_table_metadata->col_oids_[new_version].reserve(schema->GetColumns().size());
  for (const auto &col : schema->GetColumns()) {
    sql_table_metadata->col_oids_[new_version].push_back(col.Oid());
  }

  sql_table_metadata->schemas_[new_version] = std::move(schema);
}

template <class Random>
void RandomSqlTableTransaction::AddColumn(Random *generator, storage::layout_version_t layout_version) {
  if (aborted_) return;

  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];
  auto old_schema = *sql_table_metadata->schemas_[layout_version];

  // add a column to the end
  int default_value = 1;
  catalog::Schema::Column new_col(
      "new_col", type::TypeId::INTEGER, false,
      parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(default_value)));
  std::vector<catalog::Schema::Column> columns(old_schema.GetColumns());

  catalog::col_oid_t max_oid = columns.begin()->Oid();
  for (auto &col : columns) {
    if (col.Oid() > max_oid) {
      max_oid = col.Oid();
    }
  }

  StorageTestUtil::SetOid(&new_col, max_oid + 1);
  columns.push_back(new_col);

  UpdateSchema(database_oid, table_oid, columns, layout_version + 1);
}

template <class Random>
void RandomSqlTableTransaction::DropColumn(Random *generator, storage::layout_version_t layout_version) {
  if (aborted_) return;

  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];
  auto old_schema = *sql_table_metadata->schemas_[layout_version];

  // drop the last column
  auto old_columns = old_schema.GetColumns();
  std::vector<catalog::Schema::Column> columns(old_columns.begin(), old_columns.end() - 1);

  UpdateSchema(database_oid, table_oid, columns, layout_version + 1);
}

template <class Random>
void RandomSqlTableTransaction::RandomInsert(Random *generator, storage::layout_version_t layout_version) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];
  auto sql_table_ptr = test_object_->catalog_->GetDatabaseCatalog(common::ManagedPointer(txn_), database_oid)
                           ->GetTable(common::ManagedPointer(txn_), table_oid);

  // Generate random insert
  auto initializer =
      sql_table_ptr->InitializerForProjectedRow(sql_table_metadata->col_oids_[layout_version], layout_version);
  auto *const record = txn_->StageWrite(database_oid, table_oid, initializer);
  StorageTestUtil::PopulateRandomRow(record->Delta(), sql_table_ptr->GetBlockLayout(layout_version), 0.0, generator);
  record->SetTupleSlot(storage::TupleSlot(nullptr, 0));
  auto tuple_slot = sql_table_ptr->Insert(common::ManagedPointer(txn_), record, layout_version);

  // Defer addition of tuples until commit in case of aborts
  inserted_tuples_[database_oid][table_oid].push_back(tuple_slot);
}

template <class Random>
void RandomSqlTableTransaction::RandomUpdate(Random *generator, storage::layout_version_t layout_version) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];

  // Get random tuple slot to update
  storage::TupleSlot update_slot;
  {
    common::SpinLatch::ScopedSpinLatch guard(&sql_table_metadata->inserted_tuples_latch_);
    if (sql_table_metadata->inserted_tuples_.empty()) return;
    update_slot = *(RandomTestUtil::UniformRandomElement(sql_table_metadata->inserted_tuples_, generator));
  }

  // Generate random update
  // The placement of this get catalog call is important. Its possible that because we take a spin latch above, the OS
  // will serialize the txns by getting the tuple and quickly doing the operation on the tuple immedietly after. Adding
  // an expensive call (Like GetTable) will help in having the OS interleave the threads more.
  auto sql_table_ptr = test_object_->catalog_->GetDatabaseCatalog(common::ManagedPointer(txn_), database_oid)
                           ->GetTable(common::ManagedPointer(txn_), table_oid);
  auto initializer = sql_table_ptr->InitializerForProjectedRow(
      StorageTestUtil::RandomNonEmptySubset(sql_table_metadata->col_oids_[layout_version], generator), layout_version);
  auto *const redo = txn_->StageWrite(database_oid, table_oid, initializer);
  redo->SetTupleSlot(update_slot);
  StorageTestUtil::PopulateRandomRow(redo->Delta(), sql_table_ptr->GetBlockLayout(layout_version), 0.0, generator);

  auto result = sql_table_ptr->Update(common::ManagedPointer(txn_), redo, layout_version, nullptr);
  aborted_ = !result;
}

template <class Random>
void RandomSqlTableTransaction::RandomDelete(Random *generator) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];

  // Get random tuple slot to delete
  storage::TupleSlot deleted;
  {
    common::SpinLatch::ScopedSpinLatch guard(&sql_table_metadata->inserted_tuples_latch_);
    // If we run out of tuples to delete, just return
    if (sql_table_metadata->inserted_tuples_.empty()) return;
    deleted = *(RandomTestUtil::UniformRandomElement(sql_table_metadata->inserted_tuples_, generator));
  }

  // The placement of this get catalog call is important. Its possible that because we take a spin latch above, the OS
  // will serialize the txns by getting the tuple and quickly doing the operation on the tuple immedietly after. Adding
  // an expensive call (Like GetTable) will help in having the OS interleave the threads more.
  auto sql_table_ptr = test_object_->catalog_->GetDatabaseCatalog(common::ManagedPointer(txn_), database_oid)
                           ->GetTable(common::ManagedPointer(txn_), table_oid);
  txn_->StageDelete(database_oid, table_oid, deleted);
  auto result = sql_table_ptr->Delete(common::ManagedPointer(txn_), deleted);
  aborted_ = !result;

  // Delete tuple from list of inserted tuples if successful
  if (result) {
    common::SpinLatch::ScopedSpinLatch guard(&sql_table_metadata->inserted_tuples_latch_);
    auto &tuples = sql_table_metadata->inserted_tuples_;
    for (auto it = tuples.begin(); it != tuples.end(); it++) {
      if (*it == deleted) {
        tuples.erase(it);
        break;
      }
    }
  }
}

template <class Random>
void RandomSqlTableTransaction::RandomSelect(Random *generator, byte *buffer,
                                             storage::layout_version_t layout_version) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];

  storage::TupleSlot slot;
  {
    common::SpinLatch::ScopedSpinLatch guard(&sql_table_metadata->inserted_tuples_latch_);
    if (sql_table_metadata->inserted_tuples_.empty()) return;
    slot = *(RandomTestUtil::UniformRandomElement(sql_table_metadata->inserted_tuples_, generator));
  }

  // The placement of this get catalog call is important. Its possible that because we take a spin latch above, the OS
  // will serialize the txns by getting the tuple and quickly doing the operation on the tuple immedietly after. Adding
  // an expensive call (Like GetTable) will help in having the OS interleave the threads more.
  auto sql_table_ptr = test_object_->catalog_->GetDatabaseCatalog(common::ManagedPointer(txn_), database_oid)
                           ->GetTable(common::ManagedPointer(txn_), table_oid);

  auto initializer =
      sql_table_ptr->InitializerForProjectedRow(sql_table_metadata->col_oids_[layout_version], layout_version);

  if (buffer == nullptr) buffer = sql_table_metadata->singlethread_buffer_;

  storage::ProjectedRow *select_row = initializer.InitializeRow(buffer);
  sql_table_ptr->Select(common::ManagedPointer(txn_), slot, select_row, layout_version);
}

void RandomSqlTableTransaction::Finish() {
  if (aborted_) {
    test_object_->txn_manager_->Abort(txn_);
  } else {
    test_object_->txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    for (const auto &database : inserted_tuples_) {
      for (const auto &table : database.second) {
        auto &metadata = test_object_->tables_[database.first][table.first];
        {
          common::SpinLatch::ScopedSpinLatch guard(&metadata->inserted_tuples_latch_);
          metadata->inserted_tuples_.insert(metadata->inserted_tuples_.end(), table.second.begin(), table.second.end());
        }
      }
    }
  }
}

LargeSqlTableTestObject::LargeSqlTableTestObject(const LargeSqlTableTestConfiguration &config,
                                                 transaction::TransactionManager *txn_manager,
                                                 catalog::Catalog *catalog, storage::BlockStore *block_store,
                                                 std::default_random_engine *generator)
    : txn_length_(config.txn_length_),
      insert_update_select_delete_ratio_(config.insert_update_select_delete_ratio_),
      generator_(generator),
      txn_manager_(txn_manager),
      catalog_(catalog) {
  // Bootstrap the table to have the specified number of tuples
  PopulateInitialTables(config.num_databases_, config.num_tables_, config.max_columns_, config.initial_table_size_,
                        config.varlen_allowed_, block_store, generator_);
}

LargeSqlTableTestObject::~LargeSqlTableTestObject() {
  for (auto buffer : buffers_to_free_) {
    delete[] buffer;
  }

  for (auto &db_pair : tables_) {
    for (auto &table_pair : db_pair.second) {
      auto *metadata = table_pair.second;
      delete[] metadata->singlethread_buffer_;
      delete metadata;
    }
  }
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
uint64_t LargeSqlTableTestObject::SimulateOltpAndUpdateSchema(uint32_t num_transactions, uint32_t num_concurrent_txns) {
  common::WorkerPool thread_pool(num_concurrent_txns, {});
  thread_pool.Startup();
  std::vector<RandomSqlTableTransaction *> txns(num_transactions);
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;

  std::default_random_engine generator(num_concurrent_txns);
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(database_oids_, &generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(table_oids_[database_oid], &generator));
  auto sql_table_metadata = tables_[database_oid][table_oid];
  auto initializer = sql_table_metadata->pris_[latest_layout_version_];

  // Concurrent sqltable::select from different threads must use different buffers
  std::vector<byte *> buffers;
  for (size_t i = 0; i < num_concurrent_txns; i++) {
    buffers.push_back(common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize()));
  }

  // use 1 thread to update the schema to a new schema, while the other threads run transactions on the last schema
  workload = [&](uint32_t thread_id) {
    if (thread_id == 0) {
      auto txn = new RandomSqlTableTransaction(this);

      // alternate between adding and dropping columns
      std::unique_ptr<terrier::catalog::Schema> new_schema(nullptr);
      if (latest_layout_version_ % 2 == 0) {
        txn->AddColumn(&generator, latest_layout_version_);
      } else {
        txn->DropColumn(&generator, latest_layout_version_);
      }

      txn->Finish();
      delete txn;
    } else {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        txns[txn_id] = new RandomSqlTableTransaction(this);

        SimulateOneTransaction(txns[txn_id], txn_id, buffers[thread_id], latest_layout_version_);
      }
    }
  };

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_concurrent_txns, workload);

  // We only need to deallocate, and return, if gc is on, this loop is a no-op
  for (RandomSqlTableTransaction *txn : txns) {
    if (txn->aborted_) abort_count_++;
    delete txn;
  }
  latest_layout_version_++;

  for (auto buffer : buffers) {
    buffers_to_free_.push_back(buffer);
  }

  return abort_count_;
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
uint64_t LargeSqlTableTestObject::SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns) {
  common::WorkerPool thread_pool(num_concurrent_txns, {});
  thread_pool.Startup();
  std::vector<RandomSqlTableTransaction *> txns(num_transactions);
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;

  // Either for correctness checking, or to cleanup memory afterwards, we need to retain these
  // test objects
  workload = [&](uint32_t thread_id) {
    for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
      txns[txn_id] = new RandomSqlTableTransaction(this);
      SimulateOneTransaction(txns[txn_id], txn_id);
    }
  };

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_concurrent_txns, workload);

  // We only need to deallocate, and return, if gc is on, this loop is a no-op
  for (RandomSqlTableTransaction *txn : txns) {
    if (txn->aborted_) abort_count_++;
    delete txn;
  }
  return abort_count_;
}

void LargeSqlTableTestObject::SimulateOneTransaction(terrier::RandomSqlTableTransaction *txn, uint32_t txn_id,
                                                     byte *buffer, storage::layout_version_t layout_version) {
  std::default_random_engine thread_generator(txn_id);

  auto insert = [&] { txn->RandomInsert(&thread_generator, layout_version); };
  auto update = [&] { txn->RandomUpdate(&thread_generator, layout_version); };
  auto select = [&] { txn->RandomSelect(&thread_generator, buffer, layout_version); };
  auto remove = [&] { txn->RandomDelete(&thread_generator); };

  RandomTestUtil::InvokeWorkloadWithDistribution({insert, update, select, remove}, insert_update_select_delete_ratio_,
                                                 &thread_generator, txn_length_);
  txn->Finish();
}

template <class Random>
void LargeSqlTableTestObject::PopulateInitialTables(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns,
                                                    uint32_t num_tuples, bool varlen_allowed,
                                                    storage::BlockStore *block_store, Random *generator) {
  initial_txn_ = txn_manager_->BeginTransaction();
  auto namespace_name = "test_namespace";

  for (uint16_t db_idx = 0; db_idx < num_databases; db_idx++) {
    // Create database in catalog
    auto database_oid =
        catalog_->CreateDatabase(common::ManagedPointer(initial_txn_), "database" + std::to_string(db_idx), true);
    TERRIER_ASSERT(database_oid != catalog::INVALID_DATABASE_OID, "Database creation should always succeed");
    database_oids_.emplace_back(database_oid);

    // Create test namespace
    auto db_catalog_ptr = catalog_->GetDatabaseCatalog(common::ManagedPointer(initial_txn_), database_oid);
    auto namespace_oid = db_catalog_ptr->CreateNamespace(common::ManagedPointer(initial_txn_), namespace_name);

    for (uint16_t table_idx = 0; table_idx < num_tables; table_idx++) {
      // Create Database in catalog
      auto *schema = varlen_allowed ? StorageTestUtil::RandomSchemaWithVarlens(max_columns, generator)
                                    : StorageTestUtil::RandomSchemaNoVarlen(max_columns, generator);
      auto table_oid = db_catalog_ptr->CreateTable(common::ManagedPointer(initial_txn_), namespace_oid,
                                                   "table" + std::to_string(table_idx), *schema);
      TERRIER_ASSERT(table_oid != catalog::INVALID_TABLE_OID, "Table creation should always succeed");
      // schemas_[storage::layout_version_t(0)] = std::unique_ptr<catalog::Schema>(schema);

      table_oids_[database_oid].emplace_back(table_oid);
      auto catalog_schema = db_catalog_ptr->GetSchema(common::ManagedPointer(initial_txn_), table_oid);
      auto *sql_table = new storage::SqlTable(common::ManagedPointer(block_store), catalog_schema);
      auto result UNUSED_ATTRIBUTE =
          db_catalog_ptr->SetTablePointer(common::ManagedPointer(initial_txn_), table_oid, sql_table);
      TERRIER_ASSERT(result, "Setting table pointer in catalog should succeed");

      // Create metadata object
      auto *metadata = new SqlTableMetadata();
      metadata->col_oids_.resize(terrier::storage::MAX_NUM_VERSIONS);
      metadata->col_oids_[0].reserve(catalog_schema.GetColumns().size());
      for (const auto &col : catalog_schema.GetColumns()) {
        metadata->col_oids_[0].push_back(col.Oid());
      }
      metadata->schemas_[storage::layout_version_t(0)] = std::unique_ptr<catalog::Schema>(schema);

      // Create row initializer
      auto initializer = sql_table->InitializerForProjectedRow(metadata->col_oids_[0]);
      metadata->pris_.push_back(initializer);

      // Populate table
      std::vector<storage::TupleSlot> inserted_tuples;
      for (uint32_t i = 0; i < num_tuples; i++) {
        auto *const redo = initial_txn_->StageWrite(database_oid, table_oid, initializer);
        StorageTestUtil::PopulateRandomRow(redo->Delta(), sql_table->tables_.begin()->layout_, 0.0, generator);
        const storage::TupleSlot inserted = sql_table->Insert(common::ManagedPointer(initial_txn_), redo);
        inserted_tuples.emplace_back(inserted);
      }

      // Update metadata object
      metadata->inserted_tuples_ = std::move(inserted_tuples);
      metadata->singlethread_buffer_ = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      tables_[database_oid][table_oid] = metadata;
    }
  }
  txn_manager_->Commit(initial_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier
