#include "storage/data_table.h"

#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier {
// Not thread-safe
class RandomDataTableTestObject {
 public:
  template <class Random>
  RandomDataTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, const double null_bias,
                            Random *generator)
      : layout_(StorageTestUtil::RandomLayoutNoVarlen(max_col, generator)),
        table_(common::ManagedPointer(block_store), layout_, storage::layout_version_t(0)),
        null_bias_(null_bias) {}

  ~RandomDataTableTestObject() {
    for (auto ptr : loose_pointers_) delete[] ptr;
    for (auto ptr : loose_txns_) delete ptr;
    delete[] select_buffer_;
  }

  template <class Random>
  storage::TupleSlot InsertRandomTuple(const transaction::timestamp_t timestamp, Random *generator,
                                       storage::RecordBufferSegmentPool *buffer_pool) {
    // generate a random redo ProjectedRow to Insert
    auto *redo_buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
    loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow *redo = redo_initializer_.InitializeRow(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);

    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    loose_txns_.push_back(txn);

    storage::TupleSlot slot = table_.Insert(common::ManagedPointer(txn), *redo);
    inserted_slots_.push_back(slot);
    tuple_versions_[slot].emplace_back(timestamp, redo);

    return slot;
  }

  // Generate an insert using the given transaction context. This is equivalent to the version of the call taking in
  // a timestamp, but more space efficient if the insertions are large.
  template <class Random>
  storage::TupleSlot InsertRandomTuple(transaction::TransactionContext *txn, Random *generator,
                                       storage::RecordBufferSegmentPool *buffer_pool) {
    // generate a random redo ProjectedRow to Insert
    auto *redo_buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
    loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow *redo = redo_initializer_.InitializeRow(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);

    storage::TupleSlot slot = table_.Insert(common::ManagedPointer(txn), *redo);
    inserted_slots_.push_back(slot);
    tuple_versions_[slot].emplace_back(txn->StartTime(), redo);

    return slot;
  }

  // be sure to only update tuple incrementally (cannot go back in time)
  template <class Random>
  bool RandomlyUpdateTuple(const transaction::timestamp_t timestamp, const storage::TupleSlot slot, Random *generator,
                           storage::RecordBufferSegmentPool *buffer_pool) {
    // tuple must already exist
    TERRIER_ASSERT(tuple_versions_.find(slot) != tuple_versions_.end(), "Slot not found.");

    // generate a random redo ProjectedRow to Update
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    storage::ProjectedRowInitializer update_initializer =
        storage::ProjectedRowInitializer::Create(layout_, update_col_ids);
    auto *update_buffer = common::AllocationUtil::AllocateAligned(update_initializer.ProjectedRowSize());
    storage::ProjectedRow *update = update_initializer.InitializeRow(update_buffer);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);

    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    loose_txns_.push_back(txn);

    bool result = table_.Update(common::ManagedPointer(txn), slot, *update);

    if (result) {
      // manually apply the delta in an append-only fashion
      auto *version_buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
      loose_pointers_.push_back(version_buffer);
      // Copy previous version
      std::memcpy(version_buffer, tuple_versions_[slot].back().second, redo_initializer_.ProjectedRowSize());
      auto *version = reinterpret_cast<storage::ProjectedRow *>(version_buffer);
      // apply delta
      storage::StorageUtil::ApplyDelta(layout_, *update, version);
      tuple_versions_[slot].emplace_back(timestamp, version);
    }

    // the update buffer does not need to live past this scope
    delete[] update_buffer;
    return result;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

  const std::vector<storage::TupleSlot> &InsertedTuples() const { return inserted_slots_; }

  // or nullptr of no version of this tuple is visible to the timestamp
  const storage::ProjectedRow *GetReferenceVersionedTuple(const storage::TupleSlot slot,
                                                          const transaction::timestamp_t timestamp) {
    TERRIER_ASSERT(tuple_versions_.find(slot) != tuple_versions_.end(), "Slot not found.");
    auto &versions = tuple_versions_[slot];
    // search backwards so the first entry with smaller timestamp can be returned
    for (auto i = static_cast<int64_t>(versions.size() - 1); i >= 0; i--)
      if (transaction::TransactionUtil::NewerThan(timestamp, versions[i].first) || timestamp == versions[i].first)
        return versions[i].second;
    return nullptr;
  }

  storage::ProjectedRow *SelectIntoBuffer(const storage::TupleSlot slot, const transaction::timestamp_t timestamp,
                                          storage::RecordBufferSegmentPool *buffer_pool) {
    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    loose_txns_.push_back(txn);

    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = redo_initializer_.InitializeRow(select_buffer_);
    table_.Select(common::ManagedPointer(txn), slot, select_row);
    return select_row;
  }

  void Scan(storage::DataTable::SlotIterator *begin, const transaction::timestamp_t timestamp,
            storage::ProjectedColumns *buffer, storage::RecordBufferSegmentPool *buffer_pool) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    loose_txns_.push_back(txn);
    table_.Scan(common::ManagedPointer(txn), begin, buffer);
  }

  storage::DataTable &GetTable() { return table_; }

 private:
  storage::BlockLayout layout_;
  storage::DataTable table_;
  std::vector<storage::TupleSlot> inserted_slots_;
  using tuple_version = std::pair<transaction::timestamp_t, storage::ProjectedRow *>;
  // oldest to newest
  std::unordered_map<storage::TupleSlot, std::vector<tuple_version>> tuple_versions_;
  std::vector<byte *> loose_pointers_;
  std::vector<transaction::TransactionContext *> loose_txns_;
  double null_bias_;
  // These always over-provision in the case of partial selects or deltas, which is fine.
  storage::ProjectedRowInitializer redo_initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  byte *select_buffer_ = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
};

struct DataTableTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{500000, 50000};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

// Generates a random table layout and coin flip bias for an attribute being null, inserts num_inserts random tuples
// into an empty DataTable. Then, Selects the inserted TupleSlots and compares the results to the original inserted
// random tuple. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(DataTableTests, SimpleInsertSelect) {
  const uint32_t num_iterations = 50;
  const uint32_t num_inserts = 1000;
  const uint16_t max_columns = 100;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);

    // Populate the table with random tuples
    for (uint32_t i = 0; i < num_inserts; ++i)
      tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);

    EXPECT_EQ(num_inserts, tested.InsertedTuples().size());

    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    for (const auto &inserted_tuple : tested.InsertedTuples()) {
      storage::ProjectedRow *stored =
          tested.SelectIntoBuffer(inserted_tuple, transaction::timestamp_t(1), &buffer_pool_);
      const storage::ProjectedRow *ref = tested.GetReferenceVersionedTuple(inserted_tuple, transaction::timestamp_t(1));
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), stored, ref));
    }
  }
}

// Insert some number of tuples and sequentially scan for them down the table
// NOLINTNEXTLINE
TEST_F(DataTableTests, SimpleSequentialScan) {
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 20;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    // make sure we test the edge case where a block is filled
    uint32_t num_inserts = iteration == 0
                               ? tested.Layout().NumSlots()
                               : std::uniform_int_distribution<uint32_t>(1, tested.Layout().NumSlots())(generator_);

    // Populate the table with random tuples
    for (uint32_t i = 0; i < num_inserts; ++i)
      tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);

    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    EXPECT_NE(all_cols[all_cols.size() - 1].UnderlyingValue(), -1);
    storage::ProjectedColumnsInitializer initializer(tested.Layout(), all_cols, num_inserts);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns = initializer.Initialize(buffer);
    auto it = tested.GetTable().begin();
    tested.Scan(&it, transaction::timestamp_t(1), columns, &buffer_pool_);
    EXPECT_EQ(num_inserts, columns->NumTuples());
    // Test that the scan ends as soon as there are no more valid tuples,
    if (it != tested.GetTable().end()) {
      EXPECT_EQ(it, tested.GetTable().end());
    }
    for (uint32_t i = 0; i < tested.InsertedTuples().size(); i++) {
      storage::ProjectedColumns::RowView stored = columns->InterpretAsRow(i);
      const storage::ProjectedRow *ref =
          tested.GetReferenceVersionedTuple(columns->TupleSlots()[i], transaction::timestamp_t(1));
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), &stored, ref));
    }
    delete[] buffer;
  }
}

// Insert some number of tuples and sequentially scan for them down the table, scanning ranges of RawBlock at a time.
// NOLINTNEXTLINE
TEST_F(DataTableTests, SimpleSequentialScanBlocks) {
  const uint16_t max_columns = 20;
  for (uint32_t num_inserts_multiplier : {4, 5}) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    // Make sure we have at least 2 blocks, up to 5 blocks.
    uint32_t num_inserts = num_inserts_multiplier * tested.Layout().NumSlots();

    // Populate the table with random tuples
    for (uint32_t i = 0; i < num_inserts; ++i)
      tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);

    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    EXPECT_NE(all_cols[all_cols.size() - 1].UnderlyingValue(), -1);
    storage::ProjectedColumnsInitializer initializer(tested.Layout(), all_cols, num_inserts);

    auto *buffer1 = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns1 = initializer.Initialize(buffer1);
    auto *buffer2 = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns2 = initializer.Initialize(buffer2);

    // We want at least two blocks to be present so that this will actually test something.
    EXPECT_GT(tested.GetTable().GetNumBlocks(), 1);
    auto midpoint = tested.GetTable().GetNumBlocks() / 2;
    auto it1 = tested.GetTable().GetBlockedSlotIterator(0, midpoint);
    auto it2 = tested.GetTable().GetBlockedSlotIterator(midpoint, tested.GetTable().GetNumBlocks());

    tested.Scan(&it1, transaction::timestamp_t(1), columns1, &buffer_pool_);
    tested.Scan(&it2, transaction::timestamp_t(1), columns2, &buffer_pool_);

    EXPECT_EQ(num_inserts, columns1->NumTuples() + columns2->NumTuples());

    // Test that the scan ends when it should.
    EXPECT_EQ(it1, tested.GetTable().end());
    EXPECT_EQ(it2, tested.GetTable().end());

    for (uint32_t i = 0; i < columns1->NumTuples(); i++) {
      storage::ProjectedColumns::RowView stored = columns1->InterpretAsRow(i);
      const storage::ProjectedRow *ref =
          tested.GetReferenceVersionedTuple(columns1->TupleSlots()[i], transaction::timestamp_t(1));
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), &stored, ref));
    }
    for (uint32_t i = 0; i < columns2->NumTuples(); i++) {
      storage::ProjectedColumns::RowView stored = columns2->InterpretAsRow(i);
      const storage::ProjectedRow *ref =
          tested.GetReferenceVersionedTuple(columns2->TupleSlots()[i], transaction::timestamp_t(1));
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), &stored, ref));
    }

    delete[] buffer1;
    delete[] buffer2;
  }
}

// Generates a random table layout and coin flip bias for an attribute being null, inserts 1 random tuple into an empty
// DataTable. Then, randomly updates the tuple num_updates times. Finally, Selects at each timestamp to verify that the
// delta chain produces the correct tuple. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(DataTableTests, SimpleVersionChain) {
  const uint32_t num_iterations = 50;
  const uint32_t num_updates = 10;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    transaction::timestamp_t timestamp(0);

    storage::TupleSlot tuple = tested.InsertRandomTuple(timestamp++, &generator_, &buffer_pool_);
    EXPECT_EQ(1, tested.InsertedTuples().size());

    for (uint32_t i = 0; i < num_updates; ++i)
      tested.RandomlyUpdateTuple(timestamp++, tuple, &generator_, &buffer_pool_);

    std::vector<byte *> select_buffers(num_updates + 1);

    uint32_t num_versions = num_updates + 1;
    std::vector<storage::col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    for (uint32_t i = 0; i < num_versions; i++) {
      const storage::ProjectedRow *reference_version =
          tested.GetReferenceVersionedTuple(tuple, transaction::timestamp_t(i));
      storage::ProjectedRow *stored_version =
          tested.SelectIntoBuffer(tuple, transaction::timestamp_t(i), &buffer_pool_);
      EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), reference_version, stored_version));
    }
  }
}

// Generates a random table layout and coin flip bias for an attribute being null, inserts 1 random tuple into an empty
// DataTable. Then, randomly updates the tuple with a negative timestamp, representing an uncommitted transaction. Then
// a second update attempts to change the tuple and should fail. Then, the first transaction's timestamp is updated to a
// positive number representing a commit. Then, the second transaction updates again and should succeed, and its
// timestamp is changed to positive. Lastly, Selects at first timestamp to verify that the delta chain produces the
// correct tuple. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(DataTableTests, WriteWriteConflictUpdateFails) {
  const uint32_t num_iterations = 50;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    storage::TupleSlot tuple = tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);
    // take the write lock by updating with "negative" timestamp
    EXPECT_TRUE(tested.RandomlyUpdateTuple(transaction::timestamp_t(UINT64_MAX), tuple, &generator_, &buffer_pool_));
    // second transaction attempts to write, should fail
    EXPECT_FALSE(tested.RandomlyUpdateTuple(transaction::timestamp_t(1), tuple, &generator_, &buffer_pool_));

    std::vector<storage::col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    storage::ProjectedRow *stored = tested.SelectIntoBuffer(tuple, transaction::timestamp_t(UINT64_MAX), &buffer_pool_);
    const storage::ProjectedRow *ref = tested.GetReferenceVersionedTuple(tuple, transaction::timestamp_t(UINT64_MAX));
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), ref, stored));
  }
}

// Test that insertion into a block does not wrap around even in the presence of deleted slots. This makes compaction
// a lot easier to write.
// NOLINTNEXTLINE
TEST_F(DataTableTests, InsertNoWrap) {
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 10;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    storage::RawBlock *block = nullptr;
    // fill the block. bypass the test object to be more efficient with buffers
    transaction::timestamp_t timestamp(0);
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(&buffer_pool_), DISABLED);
    for (uint32_t i = 0; i < tested.Layout().NumSlots(); i++) {
      storage::RawBlock *inserted_block = tested.InsertRandomTuple(txn, &generator_, &buffer_pool_).GetBlock();
      if (block == nullptr) block = inserted_block;
      EXPECT_EQ(inserted_block, block);
    }

    // Bypass concurrency control and remove some tuples
    storage::TupleAccessStrategy accessor(tested.Layout());
    accessor.Deallocate({block, 0});

    // Even though there is still space available, we should insert into a new block
    EXPECT_NE(block, tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_).GetBlock());
    delete txn;
  }
}

// tests to make sure that the correct number of tuple slots are iterated through by slot iterator
// NOLINTNEXTLINE
TEST_F(DataTableTests, SlotIteraterSingleThreadedTest) {
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 10;
  const uint64_t max_tuples_inserted = 10000;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    transaction::timestamp_t timestamp(0);
    //    transaction::TimestampManager timestamp_manager;
    //    transaction::TransactionManager txn_manager(timestamp_manager);
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(&buffer_pool_), DISABLED);
    uint64_t size;
    // number of tuples in the table
    for (uint64_t i = 0; i < max_tuples_inserted; i++) {
      // check that iterator sees correct number of tuples
      size = 0;
      for (auto UNUSED_ATTRIBUTE _ : tested.GetTable()) size++;
      EXPECT_EQ(size, i);

      // add new tuple and make sure that expected element is in table
      auto slot = tested.InsertRandomTuple(txn, &generator_, &buffer_pool_);
      auto it = tested.GetTable().begin();
      for (uint64_t num_slots = 0; it != tested.GetTable().end() && num_slots < size; num_slots++) it++;
      EXPECT_EQ(slot, *it);
    }

    size = 0;
    for (auto UNUSED_ATTRIBUTE _ : tested.GetTable()) size++;
    EXPECT_EQ(size, max_tuples_inserted);

    delete txn;
  }
}
}  // namespace terrier
