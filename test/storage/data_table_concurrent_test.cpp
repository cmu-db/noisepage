#include <unordered_map>
#include <vector>
#include "storage/data_table.h"
#include "util/storage_test_util.h"
#include "util/multi_threaded_test_util.h"

namespace terrier {
class FakeTransaction {
 public:
  FakeTransaction(const storage::BlockLayout &layout,
                  storage::DataTable *table,
                  const double null_bias, const timestamp_t start_time, const timestamp_t txn_id)
      : layout_(layout), table_(table), null_bias_(null_bias), start_time_(start_time), txn_id_(txn_id) {}

  ~FakeTransaction() {
    for (auto ptr : loose_pointers_)
      delete[] ptr;
  }

  template<class Random>
  storage::TupleSlot InsertRandomTuple(Random *generator) {
    // generate a random redo ProjectedRow to Insert
    byte *redo_buffer = new byte[redo_size_];
    loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);

    // generate an undo DeltaRecord to populate on Insert
    byte *undo_buffer = new byte[undo_size_];
    loose_pointers_.push_back(undo_buffer);
    storage::DeltaRecord *undo =
        storage::DeltaRecord::InitializeDeltaRecord(undo_buffer, txn_id_, layout_, all_col_ids_);

    storage::TupleSlot slot = table_->Insert(*redo, undo);
    inserted_slots_.push_back(slot);
    reference_tuples_.emplace(slot, redo);
    return slot;
  }

  template<class Random>
  bool RandomlyUpdateTuple(const storage::TupleSlot slot, Random *generator) {
    // generate random update
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    byte *update_buffer = new byte[storage::ProjectedRow::Size(layout_, update_col_ids)];
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(update_buffer, update_col_ids, layout_);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);

    byte *undo_buffer = new byte[storage::DeltaRecord::Size(layout_, update_col_ids)];
    loose_pointers_.push_back(undo_buffer);
    storage::DeltaRecord *undo =
        storage::DeltaRecord::InitializeDeltaRecord(undo_buffer, txn_id_, layout_, update_col_ids);
    bool result = table_->Update(slot, *update, undo);
    // the update buffer does not need to live past this scope
    delete[] update_buffer;
    return result;
  }

  const std::vector<storage::TupleSlot> &InsertedTuples() const { return inserted_slots_; }

  const storage::ProjectedRow *GetReferenceTuple(const storage::TupleSlot slot) {
    PELOTON_ASSERT(reference_tuples_.find(slot) != reference_tuples_.end(), "Slot not found.");
    return reference_tuples_[slot];
  }

 private:
  const storage::BlockLayout &layout_;
  storage::DataTable *table_;
  double null_bias_;
  // Only running transaction will be faked
  timestamp_t start_time_, txn_id_;
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  uint32_t undo_size_ = storage::DeltaRecord::Size(layout_, all_col_ids_);
  // All data structures here are only accessed thread-locally so no need
  // for concurrent versions
  std::vector<storage::TupleSlot> inserted_slots_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> reference_tuples_;
  std::vector<byte *> loose_pointers_;
};
struct DataTableConcurrentTests : public ::testing::Test {
  storage::BlockStore block_store_{100};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

TEST_F(DataTableConcurrentTests, ConcurrentInsert) {
  const uint32_t num_iterations = 10;
  const uint32_t num_inserts = 10000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = 8;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(max_columns, &generator_);
    storage::DataTable tested(&block_store_, layout);
    std::vector<FakeTransaction> fake_txns;
    for (uint32_t thread = 0; thread < num_threads; thread++)
      // timestamps are irrelevant for inserts
      fake_txns.emplace_back(layout, &tested, null_ratio_(generator_), timestamp_t(0), timestamp_t(0));
    auto workload = [&](uint32_t id) {
      std::default_random_engine thread_generator(id);
      for (uint32_t i = 0; i < num_inserts / num_threads; i++)
        fake_txns[id].InsertRandomTuple(&thread_generator);
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads, workload);
    std::vector<uint16_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    byte *select_buffer = new byte[storage::ProjectedRow::Size(layout, all_col_ids)];
    for (auto &fake_txn : fake_txns) {
      for (auto slot : fake_txn.InsertedTuples()) {
        storage::ProjectedRow
            *select_row = storage::ProjectedRow::InitializeProjectedRow(select_buffer, all_col_ids, layout);
        tested.Select(timestamp_t(1), slot, select_row);
        EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(layout, fake_txn.GetReferenceTuple(slot), select_row));
      }
    }
    delete[] select_buffer;
  }
}

TEST_F(DataTableConcurrentTests, ConcurrentUpdateOneWriterWins) {
  const uint32_t num_iterations = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = 8;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(max_columns, &generator_);
    storage::DataTable tested(&block_store_, layout);
    FakeTransaction insert_txn(layout, &tested, null_ratio_(generator_), timestamp_t(0), timestamp_t(1));
    // Insert one tuple, the timestamp needs to show committed
    storage::TupleSlot slot = insert_txn.InsertRandomTuple(&generator_);

    std::vector<FakeTransaction> fake_txns;
    for (uint64_t thread = 0; thread < num_threads; thread++)
      // need negative timestamp to denote uncommitted
      fake_txns.emplace_back(layout,
                             &tested,
                             null_ratio_(generator_),
                             timestamp_t(0),
                             timestamp_t(static_cast<uint64_t>(-thread - 1)));
    std::atomic<uint32_t> success = 0, fail = 0;
    auto workload = [&](uint32_t id) {
      std::default_random_engine thread_generator(id);
      // log whether update is successful
      if (fake_txns[id].RandomlyUpdateTuple(slot, &thread_generator))
        success++;
      else
        fail++;
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads, workload);
    EXPECT_EQ(1, success);
    EXPECT_EQ(num_threads - 1, fail);
  }
}

}  // namespace terrier
