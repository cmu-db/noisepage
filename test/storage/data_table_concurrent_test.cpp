#include <memory>
#include <unordered_map>
#include <vector>

#include "storage/data_table.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"

namespace noisepage {
class FakeTransaction {
 public:
  FakeTransaction(const storage::BlockLayout &layout, storage::DataTable *table, const double null_bias,
                  const transaction::timestamp_t start_time, const transaction::timestamp_t txn_id,
                  storage::RecordBufferSegmentPool *buffer_pool)
      : layout_(layout),
        table_(table),
        null_bias_(null_bias),
        txn_(start_time, txn_id, common::ManagedPointer(buffer_pool), DISABLED) {}

  ~FakeTransaction() {
    for (auto ptr : loose_pointers_) delete[] ptr;
  }

  template <class Random>
  storage::TupleSlot InsertRandomTuple(Random *generator) {
    // generate a random redo ProjectedRow to Insert
    auto *redo_buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
    loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow *redo = redo_initializer_.InitializeRow(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);

    storage::TupleSlot slot = table_->Insert(common::ManagedPointer(&txn_), *redo);
    inserted_slots_.push_back(slot);
    reference_tuples_.emplace(slot, redo);
    return slot;
  }

  template <class Random>
  bool RandomlyUpdateTuple(const storage::TupleSlot slot, Random *generator) {
    // generate random update
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    storage::ProjectedRowInitializer update_initializer =
        storage::ProjectedRowInitializer::Create(layout_, update_col_ids);
    auto *update_buffer = common::AllocationUtil::AllocateAligned(update_initializer.ProjectedRowSize());
    storage::ProjectedRow *update = update_initializer.InitializeRow(update_buffer);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);

    bool result = table_->Update(common::ManagedPointer(&txn_), slot, *update);
    // the update buffer does not need to live past this scope
    delete[] update_buffer;
    return result;
  }

  transaction::TransactionContext *GetTxn() { return &txn_; }

  const std::vector<storage::TupleSlot> &InsertedTuples() const { return inserted_slots_; }

  const storage::ProjectedRow *GetReferenceTuple(const storage::TupleSlot slot) {
    NOISEPAGE_ASSERT(reference_tuples_.find(slot) != reference_tuples_.end(), "Slot not found.");
    return reference_tuples_[slot];
  }

 private:
  const storage::BlockLayout &layout_;
  storage::DataTable *table_;
  double null_bias_;
  storage::ProjectedRowInitializer redo_initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  // All data structures here are only accessed thread-locally so no need
  // for concurrent versions
  std::vector<storage::TupleSlot> inserted_slots_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> reference_tuples_;
  std::vector<byte *> loose_pointers_;
  transaction::TransactionContext txn_;
};

struct DataTableConcurrentTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

// Spawns multiple transactions. The timestamps of the transactions don't matter,
// because every transaction is just inserting a new random tuple.
// Therefore all transactions should successfully insert their tuples, which is what we test for.
// NOLINTNEXTLINE
TEST_F(DataTableConcurrentTests, ConcurrentInsert) {
  const uint32_t num_iterations = 50;
  const uint32_t num_inserts = 10000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});
  thread_pool.Startup();

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(max_columns, &generator_);
    storage::DataTable tested(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                              storage::layout_version_t(0));
    std::vector<std::unique_ptr<FakeTransaction>> fake_txns;
    for (uint32_t thread = 0; thread < num_threads; thread++)
      // timestamps are irrelevant for inserts
      fake_txns.emplace_back(std::make_unique<FakeTransaction>(layout, &tested, null_ratio_(generator_),
                                                               transaction::timestamp_t(0), transaction::timestamp_t(0),
                                                               &buffer_pool_));
    auto workload = [&](uint32_t id) {
      std::default_random_engine thread_generator(id);
      for (uint32_t i = 0; i < num_inserts / num_threads; i++) fake_txns[id]->InsertRandomTuple(&thread_generator);
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    storage::ProjectedRowInitializer select_initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    auto *select_buffer = common::AllocationUtil::AllocateAligned(select_initializer.ProjectedRowSize());
    for (auto &fake_txn : fake_txns) {
      for (auto slot : fake_txn->InsertedTuples()) {
        storage::ProjectedRow *select_row = select_initializer.InitializeRow(select_buffer);
        tested.Select(common::ManagedPointer(fake_txn->GetTxn()), slot, select_row);
        EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(layout, fake_txn->GetReferenceTuple(slot), select_row));
      }
    }
    delete[] select_buffer;
  }
}

// Spawns multiple transactions that all begin at the same time.
// Each transaction attempts to update the same tuple.
// Therefore only one transaction should win, which is what we test for.
// NOLINTNEXTLINE
TEST_F(DataTableConcurrentTests, ConcurrentUpdateOneWriterWins) {
  const uint32_t num_iterations = 100;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});
  thread_pool.Startup();

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(max_columns, &generator_);
    storage::DataTable tested(common::ManagedPointer<storage::BlockStore>(&block_store_), layout,
                              storage::layout_version_t(0));
    FakeTransaction insert_txn(layout, &tested, null_ratio_(generator_), transaction::timestamp_t(0),
                               transaction::timestamp_t(1), &buffer_pool_);
    // Insert one tuple, the timestamp needs to show committed
    storage::TupleSlot slot = insert_txn.InsertRandomTuple(&generator_);

    std::vector<std::unique_ptr<FakeTransaction>> fake_txns;
    for (uint64_t thread = 0; thread < num_threads; thread++)
      // need negative timestamp to denote uncommitted
      fake_txns.emplace_back(std::make_unique<FakeTransaction>(
          layout, &tested, null_ratio_(generator_), transaction::timestamp_t(2),
          transaction::timestamp_t(static_cast<uint64_t>(-thread - 1)), &buffer_pool_));
    std::atomic<uint32_t> success = 0, fail = 0;
    auto workload = [&](uint32_t id) {
      std::default_random_engine thread_generator(id);
      // log whether update is successful
      if (fake_txns[id]->RandomlyUpdateTuple(slot, &thread_generator))
        success++;
      else
        fail++;
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(1, success);
    EXPECT_EQ(num_threads - 1, fail);
  }
}

}  // namespace noisepage
