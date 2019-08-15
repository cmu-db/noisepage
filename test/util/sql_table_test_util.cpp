#include "util/sql_table_test_util.h"
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "util/catalog_test_util.h"

namespace terrier {

RandomSqlTableTransaction::RandomSqlTableTransaction(LargeSqlTableTestObject *test_object)
    : test_object_(test_object), txn_(test_object->txn_manager_.BeginTransaction()), aborted_(false) {}

template <class Random>
void RandomSqlTableTransaction::RandomUpdate(Random *generator) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];
  auto sql_table_ptr = test_object_->catalog_.GetDatabaseCatalog(txn_, database_oid)->GetTable(txn_, table_oid);
  auto &layout = sql_table_ptr->table_.layout;

  // Get random tuple slot to update
  storage::TupleSlot updated;
  {
    common::SpinLatch::ScopedSpinLatch guard(&sql_table_metadata->inserted_tuples_latch_);
    if (sql_table_metadata->inserted_tuples_.empty()) return;
    updated = *(RandomTestUtil::UniformRandomElement(sql_table_metadata->inserted_tuples_, generator));
  }
  // Generate random update
  std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, generator);
  storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, update_col_ids);
  auto *const record = txn_->StageWrite(database_oid, table_oid, initializer);
  record->SetTupleSlot(updated);
  StorageTestUtil::PopulateRandomRow(record->Delta(), layout, 0.0, generator);
  auto result = sql_table_ptr->Update(txn_, record);
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

  // Generate random delete
  auto sql_table_ptr = test_object_->catalog_.GetDatabaseCatalog(txn_, database_oid)->GetTable(txn_, table_oid);
  txn_->StageDelete(database_oid, table_oid, deleted);
  auto result = sql_table_ptr->Delete(txn_, deleted);
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
void RandomSqlTableTransaction::RandomSelect(Random *generator) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto &sql_table_metadata = test_object_->tables_[database_oid][table_oid];

  storage::TupleSlot selected;
  {
    common::SpinLatch::ScopedSpinLatch guard(&sql_table_metadata->inserted_tuples_latch_);
    if (sql_table_metadata->inserted_tuples_.empty()) return;
    selected = *(RandomTestUtil::UniformRandomElement(sql_table_metadata->inserted_tuples_, generator));
  }

  auto sql_table_ptr = test_object_->catalog_.GetDatabaseCatalog(txn_, database_oid)->GetTable(txn_, table_oid);
  auto initializer = storage::ProjectedRowInitializer::Create(
      sql_table_ptr->table_.layout, StorageTestUtil::ProjectionListAllColumns(sql_table_ptr->table_.layout));

  storage::ProjectedRow *select = initializer.InitializeRow(sql_table_metadata->buffer_);
  sql_table_ptr->Select(txn_, selected, select);
}

void RandomSqlTableTransaction::Finish() {
  if (aborted_)
    test_object_->txn_manager_.Abort(txn_);
  else
    test_object_->txn_manager_.Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

LargeSqlTableTestObject::LargeSqlTableTestObject(const LargeSqlTableTestConfiguration &config,
                                                 storage::BlockStore *block_store,
                                                 storage::RecordBufferSegmentPool *buffer_pool,
                                                 std::default_random_engine *generator,
                                                 storage::LogManager *log_manager)
    : txn_length_(config.txn_length_),
      update_select_delete_ratio_(config.update_select_delete_ratio_),
      generator_(generator),
      txn_manager_(buffer_pool, true /* gc on */, log_manager),
      gc_(storage::GarbageCollector(&txn_manager_, nullptr)),
      catalog_(catalog::Catalog(&txn_manager_, block_store)) {
  // Bootstrap the table to have the specified number of tuples
  TERRIER_ASSERT(update_select_delete_ratio_.size() == 3, "Update/Select/Delete ratio should be three numbers");
  PopulateInitialTables(config.num_databases_, config.num_tables_, config.max_columns_, config.initial_table_size_,
                        config.varlen_allowed_, block_store, generator_);
}

LargeSqlTableTestObject::~LargeSqlTableTestObject() {
  for (auto &db_pair : tables_) {
    for (auto &table_pair : db_pair.second) {
      auto *metadata = table_pair.second;
      delete[] metadata->buffer_;
      delete metadata;
    }
  }
  catalog_.TearDown();

  gc_.PerformGarbageCollection();
  gc_.PerformGarbageCollection();
  gc_.PerformGarbageCollection();
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
uint64_t LargeSqlTableTestObject::SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns) {
  common::WorkerPool thread_pool(num_concurrent_txns, {});
  std::vector<RandomSqlTableTransaction *> txns(num_transactions);
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;
  // Either for correctness checking, or to cleanup memory afterwards, we need to retain these
  // test objects
  workload = [&](uint32_t /*unused*/) {
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

void LargeSqlTableTestObject::SimulateOneTransaction(terrier::RandomSqlTableTransaction *txn, uint32_t txn_id) {
  std::default_random_engine thread_generator(txn_id);

  auto update = [&] { txn->RandomUpdate(&thread_generator); };
  auto select = [&] { txn->RandomSelect(&thread_generator); };
  auto remove = [&] { txn->RandomDelete(&thread_generator); };

  RandomTestUtil::InvokeWorkloadWithDistribution({update, select, remove}, update_select_delete_ratio_,
                                                 &thread_generator, txn_length_);
  txn->Finish();
}

template <class Random>
void LargeSqlTableTestObject::PopulateInitialTables(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns,
                                                    uint32_t num_tuples, bool varlen_allowed,
                                                    storage::BlockStore *block_store, Random *generator) {
  initial_txn_ = txn_manager_.BeginTransaction();

  for (uint16_t db_idx = 0; db_idx < num_databases; db_idx++) {
    // Create database in catalog
    auto database_oid = catalog_.CreateDatabase(initial_txn_, "database" + std::to_string(db_idx), true);
    TERRIER_ASSERT(database_oid != catalog::INVALID_DATABASE_OID, "Database creation should always succeed");
    database_oids_.emplace_back(database_oid);

    for (uint16_t table_idx = 0; table_idx < num_tables; table_idx++) {
      // Create Database in catalog
      auto *schema = varlen_allowed ? StorageTestUtil::RandomSchemaWithVarlens(max_columns, generator)
                                    : StorageTestUtil::RandomSchemaNoVarlen(max_columns, generator);
      auto db_catalog_ptr = catalog_.GetDatabaseCatalog(initial_txn_, database_oid);
      auto table_oid = db_catalog_ptr->CreateTable(initial_txn_, CatalogTestUtil::test_namespace_oid,
                                                   "table" + std::to_string(table_idx), *schema);
      TERRIER_ASSERT(table_oid != catalog::INVALID_TABLE_OID, "Table creation should always succeed");
      table_oids_[database_oid].emplace_back(table_oid);
      auto *sql_table = new storage::SqlTable(block_store, *schema);
      auto result UNUSED_ATTRIBUTE = db_catalog_ptr->SetTablePointer(initial_txn_, table_oid, sql_table);
      TERRIER_ASSERT(result, "Setting table pointer in catalog should succeed");
      delete schema;

      // Create row initializer
      auto &layout = sql_table->table_.layout;
      auto initializer =
          storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));

      // Populate table
      std::vector<storage::TupleSlot> inserted_tuples;
      for (uint32_t i = 0; i < num_tuples; i++) {
        auto *const redo = initial_txn_->StageWrite(database_oid, table_oid, initializer);
        StorageTestUtil::PopulateRandomRow(redo->Delta(), layout, 0.0, generator);
        const storage::TupleSlot inserted = sql_table->Insert(initial_txn_, redo);
        inserted_tuples.emplace_back(inserted);
      }

      // Create metadata object
      auto *metadata = new SqlTableMetadata();
      metadata->inserted_tuples_ = std::move(inserted_tuples);
      metadata->buffer_ = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      tables_[database_oid][table_oid] = metadata;
    }
  }
  txn_manager_.Commit(initial_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier
