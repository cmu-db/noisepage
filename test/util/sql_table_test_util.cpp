#include <utility>
#include <vector>
#include "util/sql_table_test_util.h"
#include "storage/sql_table.h"
#include "util/catalog_test_util.h"

namespace terrier {

RandomSqlTableTransaction::RandomSqlTableTransaction(LargeSqlTableTestObject *test_object)
    : test_object_(test_object), txn_(test_object->txn_manager_.BeginTransaction()), aborted_(false) {}

RandomSqlTableTransaction::~RandomSqlTableTransaction() {
  if (!test_object_->gc_on_) delete txn_;
}

template <class Random>
void RandomSqlTableTransaction::RandomUpdate(Random *generator) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto *sql_table = test_object_->tables_[database_oid][table_oid];
  auto &layout = sql_table->Layout();

  // Get random tuple slot to update
  const storage::TupleSlot updated =
      *(RandomTestUtil::UniformRandomElement(test_object_->inserted_tuples_[database_oid][table_oid], generator));

  // Generate random update
  std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, generator);
  storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, update_col_ids);
  auto *const record = txn_->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, initializer);
  record->SetTupleSlot(updated);
  StorageTestUtil::PopulateRandomRow(record->Delta(), layout, 0.0, generator);

  auto result = sql_table->Update(txn_, record);
  aborted_ = !result;
}

template <class Random>
void RandomSqlTableTransaction::RandomSelect(Random *generator) {
  if (aborted_) return;
  // Generate random database and table
  const auto database_oid = *(RandomTestUtil::UniformRandomElement(test_object_->database_oids_, generator));
  const auto table_oid = *(RandomTestUtil::UniformRandomElement(test_object_->table_oids_[database_oid], generator));
  auto *sql_table = test_object_->tables_[database_oid][table_oid];

  const storage::TupleSlot selected =
      *(RandomTestUtil::UniformRandomElement(test_object_->inserted_tuples_[database_oid][table_oid], generator));
  auto initializer = storage::ProjectedRowInitializer::Create(
      sql_table->Layout(), StorageTestUtil::ProjectionListAllColumns(sql_table->Layout()));

  auto buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  storage::ProjectedRow *select = initializer.InitializeRow(buffer);
  sql_table->Select(txn_, selected, select);
}

void RandomSqlTableTransaction::Finish() {
  if (aborted_)
    test_object_->txn_manager_.Abort(txn_);
  else
    test_object_->txn_manager_.Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

LargeSqlTableTestObject::LargeSqlTableTestObject(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns,
                                                 uint32_t initial_table_size, uint32_t txn_length,
                                                 std::vector<double> update_select_ratio,
                                                 storage::BlockStore *block_store,
                                                 storage::RecordBufferSegmentPool *buffer_pool,
                                                 std::default_random_engine *generator, bool gc_on,
                                                 storage::LogManager *log_manager, bool varlen_allowed)
    : txn_length_(txn_length),
      update_select_ratio_(std::move(update_select_ratio)),
      generator_(generator),
      txn_manager_(buffer_pool, gc_on, log_manager),
      gc_on_(gc_on) {
  // Bootstrap the table to have the specified number of tuples
  PopulateInitialTables(num_databases, num_tables, max_columns, initial_table_size, varlen_allowed, block_store,
                        generator_);
}

LargeSqlTableTestObject::~LargeSqlTableTestObject() {
  if (!gc_on_) delete initial_txn_;
}

// Caller is responsible for freeing the returned results if bookkeeping is on.
uint64_t LargeSqlTableTestObject::SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns) {
  common::WorkerPool thread_pool(num_concurrent_txns, {});
  std::vector<RandomSqlTableTransaction *> txns;
  std::function<void(uint32_t)> workload;
  std::atomic<uint32_t> txns_run = 0;
  if (gc_on_) {
    // Then there is no need to keep track of RandomDataTableTransaction objects
    workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        RandomSqlTableTransaction txn(this);
        SimulateOneTransaction(&txn, txn_id);
      }
    };
  } else {
    txns.resize(num_transactions);
    // Either for correctness checking, or to cleanup memory afterwards, we need to retain these
    // test objects
    workload = [&](uint32_t) {
      for (uint32_t txn_id = txns_run++; txn_id < num_transactions; txn_id = txns_run++) {
        txns[txn_id] = new RandomSqlTableTransaction(this);
        SimulateOneTransaction(txns[txn_id], txn_id);
      }
    };
  }

  MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_concurrent_txns, workload);

  // We only need to deallocate, and return, if gc is on, this loop is a no-op
  for (RandomSqlTableTransaction *txn : txns) {
    if (txn->aborted_) abort_count_++;
    delete txn;
  }
  // This result is meaningless if bookkeeping is not turned on.
  return abort_count_;
}

void LargeSqlTableTestObject::SimulateOneTransaction(terrier::RandomSqlTableTransaction *txn, uint32_t txn_id) {
  std::default_random_engine thread_generator(txn_id);

  auto update = [&] { txn->RandomUpdate(&thread_generator); };
  auto select = [&] { txn->RandomSelect(&thread_generator); };
  RandomTestUtil::InvokeWorkloadWithDistribution({update, select}, update_select_ratio_, &thread_generator,
                                                 txn_length_);
  txn->Finish();
}

template <class Random>
void LargeSqlTableTestObject::PopulateInitialTables(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns,
                                                    uint32_t num_tuples, bool varlen_allowed,
                                                    storage::BlockStore *block_store, Random *generator) {
  initial_txn_ = txn_manager_.BeginTransaction();

  uint16_t initial_table_oid = 0;

  for (uint16_t db_idx = 0; db_idx < num_databases; db_idx++) {
    auto database_oid = catalog::db_oid_t(db_idx);
    database_oids_.emplace_back(database_oid);
    for (uint16_t table_idx = 0; table_idx < num_tables; table_idx++) {
      // Create table
      auto table_oid = catalog::table_oid_t(initial_table_oid + table_idx);
      table_oids_[database_oid].emplace_back(table_oid);
      auto schema = varlen_allowed ? StorageTestUtil::RandomSchemaWithVarlens(max_columns, generator)
                                   : StorageTestUtil::RandomSchemaNoVarlen(max_columns, generator);
      auto *sql_table = new storage::SqlTable(block_store, schema, table_oid);
      tables_[database_oid][table_oid] = sql_table;

      // Create row initializer
      auto &layout = sql_table->Layout();
      auto initializer =
          storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));

      // Populate table
      for (uint32_t i = 0; i < num_tuples; i++) {
        auto *const redo =
            initial_txn_->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, initializer);
        StorageTestUtil::PopulateRandomRow(redo->Delta(), layout, 0.0, generator);
        const storage::TupleSlot inserted = sql_table->Insert(initial_txn_, redo);
        redo->SetTupleSlot(inserted);
        inserted_tuples_[database_oid][table_oid].emplace_back(inserted);
      }

      initial_table_oid++;
    }
  }
  txn_manager_.Commit(initial_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

LargeSqlTableTestObject LargeSqlTableTestObject::Builder::build() {
  return {builder_num_databases_, builder_num_tables_,
          builder_max_columns_,   builder_initial_table_size_,
          builder_txn_length_,    builder_update_select_ratio_,
          builder_block_store_,   builder_buffer_pool_,
          builder_generator_,     builder_gc_on_,
          builder_log_manager_,   varlen_allowed_};
}

}  // namespace terrier
