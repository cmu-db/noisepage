#include <atomic>
#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

#include "catalog/schema.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/hash.h"
#include "tbb/tbb.h"

namespace terrier::execution::sql {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  int64_t key, col_a;

  explicit InputTuple(uint64_t key, uint64_t col_a) : key(key), col_a(col_a) {}

  hash_t Hash() const noexcept { return util::Hasher::Hash(key); }
};

/**
 * This is the tuple tracking aggregate values
 */
struct AggTuple {
  int64_t key;
  uint64_t count1, count2, count3;

  explicit AggTuple(const InputTuple &input) : key(input.key), count1(0), count2(0), count3(0) { Advance(input); }

  void Advance(const InputTuple &input) {
    count1 += input.col_a;
    count2 += input.col_a * 2;
    count3 += input.col_a * 10;
  }

  void Merge(const AggTuple &input) {
    count1 += input.count1;
    count2 += input.count2;
    count3 += input.count3;
  }

  bool operator==(const AggTuple &other) {
    return key == other.key && count1 == other.count1 && count2 == other.count2 && count3 == other.count3;
  }
};

// The function to determine whether an aggregate stored in the hash table and
// an input have equivalent keys.
static inline bool AggTupleKeyEq(const void *table_tuple, const void *probe_tuple) {
  auto *lhs = reinterpret_cast<const AggTuple *>(table_tuple);
  auto *rhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  return lhs->key == rhs->key;
}

// The function to determine whether two aggregates stored in overflow
// partitions or hash tables have equivalent keys.
static inline bool AggAggKeyEq(const void *agg_tuple_1, const void *agg_tuple_2) {
  auto *lhs = reinterpret_cast<const AggTuple *>(agg_tuple_1);
  auto *rhs = reinterpret_cast<const AggTuple *>(agg_tuple_2);
  return lhs->key == rhs->key;
}

class AggregationHashTableTest : public SqlBasedTest {
 public:
  AggregationHashTableTest() : agg_table_(exec_settings_, &memory_, sizeof(AggTuple)) {}

  MemoryPool *Memory() { return &memory_; }

  AggregationHashTable *AggTable() { return &agg_table_; }

 private:
  exec::ExecutionSettings exec_settings_{};
  MemoryPool memory_{nullptr};
  AggregationHashTable agg_table_;
};

TEST_F(AggregationHashTableTest, SimpleRandomInsertionTest) {
  const uint32_t num_tuples = 10000;

  // The reference table
  std::unordered_map<uint64_t, std::unique_ptr<AggTuple>> ref_agg_table;

  std::mt19937 generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 9);

  // Insert a few random tuples
  for (uint32_t idx = 0; idx < num_tuples; idx++) {
    auto input = InputTuple(distribution(generator), 1);
    auto hash_val = input.Hash();
    auto *existing = reinterpret_cast<AggTuple *>(
        AggTable()->Lookup(hash_val, AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      // The reference table should have an equivalent aggregate tuple
      auto ref_iter = ref_agg_table.find(input.key);
      EXPECT_TRUE(ref_iter != ref_agg_table.end());
      EXPECT_TRUE(*ref_iter->second == *existing);
      existing->Advance(input);
      ref_iter->second->Advance(input);
    } else {
      // The reference table shouldn't have the aggregate
      EXPECT_EQ(ref_agg_table.find(input.key), ref_agg_table.end());
      new (AggTable()->AllocInputTuple(hash_val)) AggTuple(input);
      ref_agg_table.emplace(input.key, std::make_unique<AggTuple>(input));
    }
  }
}

TEST_F(AggregationHashTableTest, IterationTest) {
  //
  // SELECT key, SUM(cola), SUM(cola*2), SUM(cola*10) FROM table GROUP BY key
  //
  // Keys are selected continuously from the range [0, 10); all cola values are
  // equal to 1.
  //
  // Each group will receive G=num_inserts/10 tuples, count1 will be G,
  // count2 will be G*2, and count3 will be G*10.
  //

  const uint32_t num_inserts = 10000;
  const uint32_t num_groups = 10;
  const uint32_t tuples_per_group = num_inserts / num_groups;
  ASSERT_EQ(0u, num_inserts % num_groups);

  {
    for (uint32_t idx = 0; idx < num_inserts; idx++) {
      InputTuple input(idx % num_groups, 1);
      auto *existing = reinterpret_cast<AggTuple *>(
          AggTable()->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

      if (existing != nullptr) {
        existing->Advance(input);
      } else {
        auto *new_agg = AggTable()->AllocInputTuple(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  }

  //
  // Iterate resulting aggregates. There should be exactly 10
  //

  {
    uint32_t group_count = 0;
    for (AHTIterator iter(*AggTable()); iter.HasNext(); iter.Next()) {
      auto *agg_tuple = reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
      EXPECT_EQ(tuples_per_group, agg_tuple->count1);
      EXPECT_EQ(tuples_per_group * 2, agg_tuple->count2);
      EXPECT_EQ(tuples_per_group * 10, agg_tuple->count3);
      group_count++;
    }

    EXPECT_EQ(num_groups, group_count);
  }
}

TEST_F(AggregationHashTableTest, SimplePartitionedInsertionTest) {
  const uint32_t num_tuples = 10000;

  for (uint32_t idx = 0; idx < num_tuples; idx++) {
    InputTuple input(idx, 1);
    auto *existing = reinterpret_cast<AggTuple *>(
        AggTable()->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      existing->Advance(input);
    } else {
      auto *new_agg = AggTable()->AllocInputTuplePartitioned(input.Hash());
      new (new_agg) AggTuple(input);
    }
  }

  // We had to have flushed
  EXPECT_GT(AggTable()->GetStatistics()->num_flushes_, 0);
}

TEST_F(AggregationHashTableTest, BatchProcessTest) {
  constexpr uint32_t num_groups = 512;
  constexpr uint32_t num_group_updates_per_batch = common::Constants::K_DEFAULT_VECTOR_SIZE / num_groups;
  constexpr uint32_t count1_scale = 1;
  constexpr uint32_t count2_scale = 2;
  constexpr uint32_t count3_scale = 3;
  constexpr uint32_t num_batches = 10;

  // We'll try to create an aggregation table with 511 groups. To do this, we'll
  // create create a batch (2048) of tuples that rotate keys between [0,511],
  // but apply the filter: key != 511, thus having the 511 unique keys in the
  // range [0,510]. Each group will receive 2048/512 updates per batch. We'll
  // issue 10 updates.

  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::Integer, TypeId::Integer});
  vector_projection.Reset(common::Constants::K_DEFAULT_VECTOR_SIZE);

  for (uint32_t run = 0; run < num_batches; run++) {
    {
      auto keys = reinterpret_cast<uint32_t *>(vector_projection.GetColumn(0)->GetData());
      auto values = reinterpret_cast<uint32_t *>(vector_projection.GetColumn(1)->GetData());
      for (uint32_t i = 0; i < common::Constants::K_DEFAULT_VECTOR_SIZE; i++) {
        keys[i] = i % num_groups;
        values[i] = 1;
      }

      // Shuffle keys.
      std::shuffle(keys, keys + common::Constants::K_DEFAULT_VECTOR_SIZE, std::random_device());

      // Filter out ONLY the last grouping key.
      TupleIdList tids(common::Constants::K_DEFAULT_VECTOR_SIZE);
      for (uint32_t i = 0; i < common::Constants::K_DEFAULT_VECTOR_SIZE; i++) {
        tids.Enable(i, keys[i] != num_groups - 1);
      }
      EXPECT_FALSE(tids.IsFull());  // Some keys have been filtered.
      vector_projection.SetFilteredSelections(tids);
    }

    // Process
    VectorProjectionIterator vpi(&vector_projection);
    AggTable()->ProcessBatch(
        &vpi,  // Input batch
        {0},   // Key indexes
        // Aggregate initialization function
        [](VectorProjectionIterator *new_aggs, VectorProjectionIterator *input) {
          VectorProjectionIterator::SynchronizedForEach({new_aggs, input}, [&]() {
            auto *e = *new_aggs->GetValue<sql::HashTableEntry *, false>(1, nullptr);
            auto agg = const_cast<AggTuple *>(e->PayloadAs<AggTuple>());
            agg->key = *input->GetValue<uint32_t, false>(0, nullptr);
            agg->count1 = agg->count2 = agg->count3 = 0;
          });
        },
        // Aggregate update function
        [](VectorProjectionIterator *aggs, VectorProjectionIterator *input) {
          VectorProjectionIterator::SynchronizedForEach({aggs, input}, [&]() {
            auto *e = *aggs->GetValue<sql::HashTableEntry *, false>(1, nullptr);
            auto agg = const_cast<AggTuple *>(e->PayloadAs<AggTuple>());
            agg->count1 += count1_scale;
            agg->count2 += count2_scale;
            agg->count3 += count3_scale;
          });
        },
        false  // Partitioned?
    );
  }

  EXPECT_EQ(num_groups - 1, AggTable()->GetTupleCount());
  for (auto iter = AHTIterator(*AggTable()); iter.HasNext(); iter.Next()) {
    auto agg = reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
    EXPECT_EQ(num_group_updates_per_batch * num_batches * count1_scale, agg->count1);
    EXPECT_EQ(num_group_updates_per_batch * num_batches * count2_scale, agg->count2);
    EXPECT_EQ(num_group_updates_per_batch * num_batches * count3_scale, agg->count3);
  }
}

TEST_F(AggregationHashTableTest, OverflowPartitonIteratorTest) {
  struct Data {
    uint32_t key{5};
    uint32_t val{10};
  };

  struct TestEntry : public HashTableEntry {
    Data data;
    TestEntry() : HashTableEntry(), data{} {}
    TestEntry(uint32_t key, uint32_t val) : HashTableEntry(), data{key, val} {}
  };

  constexpr uint32_t nparts = 50;
  constexpr uint32_t nentries_per_part = 10;

  // Allocate partitions
  std::array<HashTableEntry *, nparts> partitions{};
  partitions.fill(nullptr);

  // -------------------------------------------------------
  // Test: check iteration over an empty partitions array
  {
    uint32_t count = 0;
    AHTOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      count++;
    }
    EXPECT_EQ(0u, count);
  }

  // -------------------------------------------------------
  // Test: insert one entry in the middle partition and ensure we find it
  {
    std::vector<std::unique_ptr<TestEntry>> entries;
    entries.emplace_back(std::make_unique<TestEntry>(100, 200));

    HashTableEntry *entry = entries[0].get();
    const uint32_t part_idx = nparts / 2;
    entry->next_ = partitions[part_idx];
    partitions[part_idx] = entry;

    // Check
    uint32_t count = 0;
    AHTOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      EXPECT_EQ(100u, iter.GetRowAs<Data>()->key);
      EXPECT_EQ(200u, iter.GetRowAs<Data>()->val);
      count++;
    }
    EXPECT_EQ(1u, count);
  }

  partitions.fill(nullptr);

  // -------------------------------------------------------
  // Test: create a list of nparts partitions and vary the number of entries in
  //       each partition from [0, nentries_per_part). Ensure the counts match.
  {
    std::vector<std::unique_ptr<TestEntry>> entries;

    // Populate each partition
    std::random_device random;
    uint32_t num_entries = 0;
    for (uint32_t part_idx = 0; part_idx < nparts; part_idx++) {
      const uint32_t nentries = (random() % nentries_per_part);
      for (uint32_t i = 0; i < nentries; i++) {
        // Create entry
        entries.emplace_back(std::make_unique<TestEntry>());
        HashTableEntry *entry = entries[entries.size() - 1].get();

        // Link it into partition
        entry->next_ = partitions[part_idx];
        partitions[part_idx] = entry;
        num_entries++;
      }
    }

    // Check
    uint32_t count = 0;
    AHTOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      count++;
    }
    EXPECT_EQ(num_entries, count);
  }
}

TEST_F(AggregationHashTableTest, ParallelAggregationTest) {
  auto exec_ctx = MakeExecCtx();
  tbb::task_scheduler_init sched;

  // The whole-query state.
  struct QueryState {
    std::atomic<uint32_t> row_count;
  };

  QueryState query_state{0};
  MemoryPool memory(nullptr);
  ThreadStateContainer container(exec_ctx->GetExecutionSettings(), &memory);

  // -------------------------------------------------------
  // Step 1: thread-local container contains only an aggregation hash table.
  container.Reset(
      sizeof(AggregationHashTable),
      // Init function.
      [](void *ctx, void *aht) {
        auto exec_ctx = reinterpret_cast<exec::ExecutionContext *>(ctx);
        new (aht) AggregationHashTable(exec_ctx->GetExecutionSettings(), exec_ctx->GetMemoryPool(), sizeof(AggTuple));
      },
      // Tear-down function.
      [](void *ctx, void *aht) { std::destroy_at(reinterpret_cast<AggregationHashTable *>(aht)); }, exec_ctx.get());

  // -------------------------------------------------------
  // Step 2: build 4 thread-local aggregation hash tables.
  constexpr uint32_t num_aggs = 100;
  LaunchParallel(4, [&](auto tid) {
    // The thread-local table.
    auto agg_table = container.AccessCurrentThreadStateAs<AggregationHashTable>();

    std::mt19937 generator;
    std::uniform_int_distribution<uint64_t> distribution(0, num_aggs - 1);

    for (uint32_t idx = 0; idx < 10000; idx++) {
      InputTuple input(distribution(generator), 1);
      auto *existing = reinterpret_cast<AggTuple *>(
          agg_table->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));
      if (existing != nullptr) {
        existing->Advance(input);
      } else {
        auto *new_agg = agg_table->AllocInputTuplePartitioned(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  });

  // -------------------------------------------------------
  // Step 2: Transfer thread-local memory into global/main hash table.
  AggregationHashTable main_table(*exec_ctx->GetExecutionSettings(), &memory, sizeof(AggTuple));
  main_table.TransferMemoryAndPartitions(
      &container, 0,
      // Merging function merges a set of overflow partitions into the provided
      // table.
      [](void *ctx, AggregationHashTable *table, AHTOverflowPartitionIterator *iter) {
        for (; iter->HasNext(); iter->Next()) {
          auto *partial_agg = iter->GetRowAs<AggTuple>();
          auto *existing = reinterpret_cast<AggTuple *>(table->Lookup(iter->GetRowHash(), AggAggKeyEq, partial_agg));
          if (existing != nullptr) {
            existing->Merge(*partial_agg);
          } else {
            table->Insert(iter->GetEntryForRow());
          }
        }
      });

  // Clear thread-local container to ensure all memory has been moved
  container.Clear();

  // -------------------------------------------------------
  // Step 3: scan main table and ensure all data exists.
  main_table.ExecuteParallelPartitionedScan(
      &query_state, &container, [](void *query_state, void *thread_state, const AggregationHashTable *agg_table) {
        auto *qs = reinterpret_cast<QueryState *>(query_state);
        qs->row_count += agg_table->GetTupleCount();
      });

  // Check
  EXPECT_EQ(num_aggs, query_state.row_count.load(std::memory_order_seq_cst));
}

}  // namespace terrier::execution::sql
