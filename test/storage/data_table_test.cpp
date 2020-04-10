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
#include "transaction/transaction_util.h"

#ifndef __APPLE__
#include <numa.h>
#include <numaif.h>
#endif

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
    common::SpinLatch::ScopedSpinLatch l(&latch_);
    // generate a random redo ProjectedRow to Insert
    auto *redo_buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
    loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow *redo = redo_initializer_.InitializeRow(redo_buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);

    // generate a txn with an UndoRecord to populate on Insert
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    loose_txns_.emplace_back(txn);

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

  void NUMAScan(const transaction::timestamp_t timestamp, storage::ProjectedColumns *result_colunm,
                storage::ProjectedColumnsInitializer *initializer, storage::RecordBufferSegmentPool *buffer_pool) {
    auto *txn =
        new transaction::TransactionContext(timestamp, timestamp, common::ManagedPointer(buffer_pool), DISABLED);
    loose_txns_.push_back(txn);
    std::vector<storage::numa_region_t> regions;
    std::vector<storage::ProjectedColumns *> colunms;
    table_.GetNUMARegions(&regions);
    for (storage::numa_region_t _ UNUSED_ATTRIBUTE : regions) {
      auto *buffer = common::AllocationUtil::AllocateAligned(initializer->ProjectedColumnsSize());
      storage::ProjectedColumns *column = initializer->Initialize(buffer);
      colunms.emplace_back(column);
    }
    table_.NUMAScan(common::ManagedPointer(txn), &colunms, result_colunm);
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
  common::SpinLatch latch_;
};

struct DataTableTests : public TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
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
    EXPECT_NE((!all_cols[all_cols.size() - 1]), -1);
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

// Test that insertion into a block does not wrap around even in the presence of deleted slots. This makes compaction
// a lot easier to write.
// NOLINTNEXTLINE
TEST_F(DataTableTests, SimpleNumaTest) {
  const uint32_t num_iterations = 3;
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
    EXPECT_NE((!all_cols[all_cols.size() - 1]), -1);
    storage::ProjectedColumnsInitializer initializer(tested.Layout(), all_cols, num_inserts);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
    storage::ProjectedColumns *columns = initializer.Initialize(buffer);

    std::vector<storage::numa_region_t> numa_regions;
    tested.GetTable().GetNUMARegions(&numa_regions);
    EXPECT_GE(numa_regions.size(), 1);

#ifdef __APPLE__
    EXPECT_EQ(numa_regions.size(), 1);
    EXPECT_EQ(numa_regions[0], storage::UNSUPPORTED_NUMA_REGION);
#else
    bool numa_available_unsupported =
        numa_available() != -1 && numa_regions.size() == 1 && numa_regions[0] == storage::UNSUPPORTED_NUMA_REGION;
    for (auto numa_region : numa_regions) {
      if (numa_available() != -1) {
        EXPECT_TRUE(numa_available_unsupported || numa_region != storage::UNSUPPORTED_NUMA_REGION);
      }
    }
#endif

    for (storage::numa_region_t numa_region : numa_regions) {
      tested.NUMAScan(transaction::timestamp_t(1), columns, &initializer, &buffer_pool_);
      for (auto it = tested.GetTable().begin(numa_region); it != tested.GetTable().end(numa_region); ++it) {
        EXPECT_NE((*it).GetBlock(), nullptr);
        EXPECT_EQ((*it).GetBlock()->numa_region_, numa_region);
#ifndef __APPLE__
        if (numa_available() != -1) {
          int status;
          auto *page = static_cast<void *>((*it).GetBlock());
          if (move_pages(0, 1, &page, nullptr, &status, 0) != -1) {
            EXPECT_EQ(static_cast<int>(static_cast<int16_t>(numa_region)), status);
          } else {
            EXPECT_TRUE(numa_available_unsupported);
          }
        }
#endif
      }
    }
    EXPECT_EQ(num_inserts, columns->NumTuples());

    delete[] buffer;
  }
}

TEST_F(DataTableTests, ConcurrentNumaTest) {
  const uint32_t num_iterations = 10;
  const uint32_t num_threads = std::thread::hardware_concurrency();
  const uint16_t max_columns = 20;
  const uint32_t object_pool_size = 100000;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    // make sure we test the edge case where a block is filled
    uint32_t num_inserts = iteration == 0
                               ? tested.Layout().NumSlots()
                               : std::uniform_int_distribution<uint32_t>(1, tested.Layout().NumSlots())(generator_);

    if (num_inserts > object_pool_size / num_threads) num_inserts = object_pool_size / num_threads;

    std::thread threads[num_threads];
    for (uint32_t t = 0; t < num_threads; t++) {
      threads[t] = std::thread([&] {
        // Populate the table with random tuples
        for (uint32_t i = 0; i < num_inserts; i++) {
          tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);
        }
      });
    }

    for (uint32_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    uint32_t num_slots = 0;
    for (auto it = tested.GetTable().begin(); it != tested.GetTable().end(); ++it) {
      num_slots++;
    }

    EXPECT_EQ(num_slots, num_inserts * num_threads);

    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    EXPECT_NE((!all_cols[all_cols.size() - 1]), -1);

    std::vector<storage::numa_region_t> numa_regions;
    tested.GetTable().GetNUMARegions(&numa_regions);
    EXPECT_GE(numa_regions.size(), 1);

#ifdef __APPLE__
    EXPECT_EQ(numa_regions.size(), 1);
    EXPECT_EQ(numa_regions[0], storage::UNSUPPORTED_NUMA_REGION);
#else
    bool numa_available_unsupported =
        numa_available() != -1 && numa_regions.size() == 1 && numa_regions[0] == storage::UNSUPPORTED_NUMA_REGION;
    for (auto numa_region : numa_regions) {
      if (numa_available() != -1) {
        EXPECT_TRUE(numa_available_unsupported || numa_region != storage::UNSUPPORTED_NUMA_REGION);
      }
    }
#endif

    std::thread numa_threads[numa_regions.size()];
    std::atomic<uint32_t> counted_numa_iteration = 0;

    for (uint32_t i = 0; i < numa_regions.size(); i++) {
      numa_threads[i] = std::thread([&, i] {
        storage::numa_region_t numa_region = numa_regions[i];
        storage::ProjectedColumnsInitializer initializer(tested.Layout(), all_cols, 10 * num_inserts * num_threads);
        auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
        storage::ProjectedColumns *columns = initializer.Initialize(buffer);
        tested.NUMAScan(transaction::timestamp_t(1), columns, &initializer, &buffer_pool_);
        EXPECT_EQ(columns->NumTuples(), num_inserts * num_threads);
        for (auto it = tested.GetTable().begin(numa_region); it != tested.GetTable().end(numa_region); it++) {
          counted_numa_iteration++;
          EXPECT_NE((*it).GetBlock(), nullptr);
          EXPECT_EQ((*it).GetBlock()->numa_region_, numa_region);
#ifndef __APPLE__
          if (numa_available() != -1) {
            int status;
            auto *page = static_cast<void *>((*it).GetBlock());
            if (move_pages(0, 1, &page, nullptr, &status, 0) != -1) {
              EXPECT_EQ(static_cast<int>(static_cast<int16_t>(numa_region)), status);
            } else {
              EXPECT_TRUE(numa_available_unsupported);
            }
          }
#endif
        }

        delete[] buffer;
      });
    }

    for (uint32_t i = 0; i < numa_regions.size(); i++) {
      numa_threads[i].join();
    }

    EXPECT_EQ(num_inserts * num_threads, counted_numa_iteration);
  }
}

TEST_F(DataTableTests, ConcurrentNumaAwareScanTest) {
  const uint32_t num_iterations = 10;
  const uint32_t num_threads = std::thread::hardware_concurrency();
  const uint16_t max_columns = 20;
  const uint32_t object_pool_size = 100000;
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids(std::thread::hardware_concurrency());
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) cpu_ids.emplace_back(i);
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    // make sure we test the edge case where a block is filled
    uint32_t num_inserts = iteration == 0
                               ? tested.Layout().NumSlots()
                               : std::uniform_int_distribution<uint32_t>(1, tested.Layout().NumSlots())(generator_);

    if (num_inserts > object_pool_size / num_threads) num_inserts = object_pool_size / num_threads;

    std::promise<void> threads[num_threads];
    for (uint32_t t = 0; t < num_threads; t++) {
      thread_pool.SubmitTask(&threads[t], [&] {
        // Populate the table with random tuples
        for (uint32_t i = 0; i < num_inserts; i++) {
          tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);
        }
      });
    }

    // NOLINTNEXTLINE
    for (auto &promise : threads) {  // NOLINT
      promise.get_future().get();
    }

    uint32_t num_slots = 0;
    for (auto it = tested.GetTable().begin(); it != tested.GetTable().end(); ++it) {
      num_slots++;
    }

    EXPECT_EQ(num_slots, num_inserts * num_threads);

    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    EXPECT_NE((!all_cols[all_cols.size() - 1]), -1);

    std::vector<storage::numa_region_t> numa_regions;
    tested.GetTable().GetNUMARegions(&numa_regions);
    EXPECT_GE(numa_regions.size(), 1);

#ifdef __APPLE__
    EXPECT_EQ(numa_regions.size(), 1);
    EXPECT_EQ(numa_regions[0], storage::UNSUPPORTED_NUMA_REGION);
#else
    bool numa_available_unsupported =
        numa_available() != -1 && numa_regions.size() == 1 && numa_regions[0] == storage::UNSUPPORTED_NUMA_REGION;
    for (auto numa_region : numa_regions) {
      if (numa_available() != -1) {
        EXPECT_TRUE(numa_available_unsupported || numa_region != storage::UNSUPPORTED_NUMA_REGION);
      }
    }
#endif

    std::promise<void> numa_threads[numa_regions.size()];
    std::atomic<uint32_t> counted_numa_iteration = 0;

    for (uint32_t i = 0; i < numa_regions.size(); i++) {
      storage::numa_region_t numa_region = numa_regions[i];
      thread_pool.SubmitTask(
          &numa_threads[i],
          [&, numa_region] {
            storage::ProjectedColumnsInitializer initializer(tested.Layout(), all_cols, 10 * num_inserts * num_threads);
            auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
            storage::ProjectedColumns *columns = initializer.Initialize(buffer);
            tested.NUMAScan(transaction::timestamp_t(1), columns, &initializer, &buffer_pool_);
            EXPECT_EQ(columns->NumTuples(), num_inserts * num_threads);
            for (auto it = tested.GetTable().begin(numa_region); it != tested.GetTable().end(numa_region); it++) {
              counted_numa_iteration++;
              EXPECT_NE((*it).GetBlock(), nullptr);
              EXPECT_EQ((*it).GetBlock()->numa_region_, numa_region);
#ifndef __APPLE__
              if (numa_available() != -1) {
                int status;
                auto *page = static_cast<void *>((*it).GetBlock());
                if (move_pages(0, 1, &page, nullptr, &status, 0) != -1) {
                  EXPECT_EQ(static_cast<int>(static_cast<int16_t>(numa_region)), status);
                } else {
                  EXPECT_TRUE(numa_available_unsupported);
                }
              }
#endif
            }

            delete[] buffer;
          },
          numa_region);
    }

    // NOLINTNEXTLINE
    for (auto &promise : numa_threads) {  // NOLINT
      promise.get_future().get();
    }

    EXPECT_EQ(num_inserts * num_threads, counted_numa_iteration);
  }
}

TEST_F(DataTableTests, ConcurrentNumaAwareScanTest) {
  const uint32_t num_iterations = 10;
  const uint32_t num_threads = std::thread::hardware_concurrency();
  const uint16_t max_columns = 20;
  const uint32_t object_pool_size = 100000;
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) cpu_ids.emplace_back(i);
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    RandomDataTableTestObject tested(&block_store_, max_columns, null_ratio_(generator_), &generator_);
    // make sure we test the edge case where a block is filled
    uint32_t num_inserts = iteration == 0
                               ? tested.Layout().NumSlots()
                               : std::uniform_int_distribution<uint32_t>(1, tested.Layout().NumSlots())(generator_);

    if (num_inserts > object_pool_size / num_threads) num_inserts = object_pool_size / num_threads;

    std::promise<void> threads[num_threads];
    for (uint32_t t = 0; t < num_threads; t++) {
      thread_pool.SubmitTask(&threads[t], [&] {
        // Populate the table with random tuples
        for (uint32_t i = 0; i < num_inserts; i++) {
          tested.InsertRandomTuple(transaction::timestamp_t(0), &generator_, &buffer_pool_);
        }
      });
    }

    for (uint32_t i = 0; i < num_threads; i++) {
      threads[i].get_future().get();
    }

    uint32_t num_slots = 0;
    for (auto it = tested.GetTable().begin(); it != tested.GetTable().end(); ++it) {
      num_slots++;
    }

    EXPECT_EQ(num_slots, num_inserts * num_threads);

    std::vector<storage::col_id_t> all_cols = StorageTestUtil::ProjectionListAllColumns(tested.Layout());
    EXPECT_NE((!all_cols[all_cols.size() - 1]), -1);

    std::vector<storage::numa_region_t> numa_regions;
    tested.GetTable().GetNUMARegions(&numa_regions);
    EXPECT_GE(numa_regions.size(), 1);

#ifdef __APPLE__
    EXPECT_EQ(numa_regions.size(), 1);
    EXPECT_EQ(numa_regions[0], storage::UNSUPPORTED_NUMA_REGION);
#else
    bool numa_available_unsupported =
        numa_available() != -1 && numa_regions.size() == 1 && numa_regions[0] == storage::UNSUPPORTED_NUMA_REGION;
    for (uint64_t i = 0; i < numa_regions.size(); i++) {
      if (numa_available() != -1) {
        EXPECT_TRUE(numa_available_unsupported || numa_regions[i] != storage::UNSUPPORTED_NUMA_REGION);
      }
    }
#endif

    std::promise<void> numa_threads[numa_regions.size()];
    std::atomic<uint32_t> counted_numa_iteration = 0;

    for (uint32_t i = 0; i < numa_regions.size(); i++) {
      storage::numa_region_t numa_region = numa_regions[i];
      thread_pool.SubmitTask(
          &numa_threads[i],
          [&, numa_region] {
            storage::ProjectedColumnsInitializer initializer(tested.Layout(), all_cols, 10 * num_inserts * num_threads);
            auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedColumnsSize());
            storage::ProjectedColumns *columns = initializer.Initialize(buffer);
            tested.NUMAScan(transaction::timestamp_t(1), columns, &initializer, &buffer_pool_);
            EXPECT_EQ(columns->NumTuples(), num_inserts * num_threads);
            for (auto it = tested.GetTable().begin(numa_region); it != tested.GetTable().end(numa_region); it++) {
              counted_numa_iteration++;
              EXPECT_NE((*it).GetBlock(), nullptr);
              EXPECT_EQ((*it).GetBlock()->numa_region_, numa_region);
#ifndef __APPLE__
              if (numa_available() != -1) {
                int status;
                auto *page = static_cast<void *>((*it).GetBlock());
                if (move_pages(0, 1, &page, NULL, &status, 0) != -1) {
                  EXPECT_EQ(static_cast<int>(static_cast<int16_t>(numa_region)), status);
                } else {
                  EXPECT_TRUE(numa_available_unsupported);
                }
              }
#endif
            }

            delete[] buffer;
          },
          numa_region);
    }

    for (uint32_t i = 0; i < numa_regions.size(); i++) {
      numa_threads[i].get_future().get();
    }

    EXPECT_EQ(num_inserts * num_threads, counted_numa_iteration);
  }
}
}  // namespace terrier
