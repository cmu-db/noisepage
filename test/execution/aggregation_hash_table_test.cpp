#include <atomic>
#include <memory>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/sql_test.h"

#include <tbb/tbb.h>  // NOLINT

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/util/hash.h"

namespace terrier::execution::sql::test {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  uint64_t key_, col_a_;

  explicit InputTuple(uint64_t key, uint64_t col_a) : key_(key), col_a_(col_a) {}

  hash_t Hash() const noexcept { return util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&key_), sizeof(key_)); }
};

/**
 * This is the tuple tracking aggregate values
 */
struct AggTuple {
  uint64_t key_, count1_, count2_, count3_;

  explicit AggTuple(const InputTuple &input) : key_(input.key_), count1_(0), count2_(0), count3_(0) { Advance(input); }

  void Advance(const InputTuple &input) {
    count1_ += input.col_a_;
    count2_ += input.col_a_ * 2;
    count3_ += input.col_a_ * 10;
  }

  void Merge(const AggTuple &input) {
    count1_ += input.count1_;
    count2_ += input.count2_;
    count3_ += input.count3_;
  }

  bool operator==(const AggTuple &other) {
    return key_ == other.key_ && count1_ == other.count1_ && count2_ == other.count2_ && count3_ == other.count3_;
  }
};

// The function to determine whether an aggregate stored in the hash table and
// an input have equivalent keys.
static bool AggTupleKeyEq(const void *table_tuple, const void *probe_tuple) {
  auto *lhs = reinterpret_cast<const AggTuple *>(table_tuple);
  auto *rhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  return lhs->key_ == rhs->key_;
}

// The function to determine whether two aggregates stored in overflow
// partitions or hash tables have equivalent keys.
static bool AggAggKeyEq(const void *agg_tuple_1, const void *agg_tuple_2) {
  auto *lhs = reinterpret_cast<const AggTuple *>(agg_tuple_1);
  auto *rhs = reinterpret_cast<const AggTuple *>(agg_tuple_2);
  return lhs->key_ == rhs->key_;
}

class AggregationHashTableTest : public SqlBasedTest {
 public:
  AggregationHashTableTest() : memory_(nullptr), agg_table_(&memory_, sizeof(AggTuple)) {}

  MemoryPool *Memory() { return &memory_; }

  AggregationHashTable *AggTable() { return &agg_table_; }

  void SetUp() override {
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
  }

  // Helper to make a PCI
  storage::ProjectedColumns *MakeProjectedColumns() {
    // TODO(Amadou): Come up with an easier way to create ProjectedColumns.
    // Create column metadata for every column.
    catalog::Schema::Column key_col("key", type::TypeId::INTEGER, false, DummyCVE());
    catalog::Schema::Column val_col("val", type::TypeId::INTEGER, false, DummyCVE());

    // Create the table in the catalog.
    catalog::Schema tmp_schema({key_col, val_col});
    auto table_oid = exec_ctx_->GetAccessor()->CreateTable(NSOid(), "test_table", tmp_schema);
    auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
    auto sql_table = new storage::SqlTable(BlockStore(), schema);
    exec_ctx_->GetAccessor()->SetTablePointer(table_oid, sql_table);

    // Create a ProjectedColumns
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &col : schema.GetColumns()) {
      col_oids.emplace_back(col.Oid());
    }
    auto pc_init = sql_table->InitializerForProjectedColumns(col_oids, common::Constants::K_DEFAULT_VECTOR_SIZE);
    buffer_ = common::AllocationUtil::AllocateAligned(pc_init.ProjectedColumnsSize());
    projected_columns_ = pc_init.Initialize(buffer_);
    projected_columns_->SetNumTuples(common::Constants::K_DEFAULT_VECTOR_SIZE);
    return projected_columns_;
  }

  void FreeProjectedColumns() { delete[] buffer_; }

 protected:
  /**
   * Execution ctx of this test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;

 private:
  MemoryPool memory_;

  AggregationHashTable agg_table_;

  // Helpers to create a table and get an PCI
  storage::ProjectedColumns *projected_columns_{nullptr};
  byte *buffer_{nullptr};
};

// NOLINTNEXTLINE
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
      auto ref_iter = ref_agg_table.find(input.key_);
      EXPECT_TRUE(ref_iter != ref_agg_table.end());
      EXPECT_TRUE(*ref_iter->second == *existing);
      existing->Advance(input);
      ref_iter->second->Advance(input);
    } else {
      // The reference table shouldn't have the aggregate
      auto ref_iter = ref_agg_table.find(input.key_);
      if (ref_iter != ref_agg_table.end()) {
        FAIL();
      }
      EXPECT_TRUE(ref_iter == ref_agg_table.end());
      new (AggTable()->Insert(hash_val)) AggTuple(input);
      ref_agg_table.emplace(input.key_, std::make_unique<AggTuple>(input));
    }
  }
}

// NOLINTNEXTLINE
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
        auto *new_agg = AggTable()->Insert(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  }

  //
  // Iterate resulting aggregates. There should be exactly 10
  //

  {
    uint32_t group_count = 0;
    for (AggregationHashTableIterator iter(*AggTable()); iter.HasNext(); iter.Next()) {
      auto *agg_tuple = reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
      EXPECT_EQ(tuples_per_group, agg_tuple->count1_);
      EXPECT_EQ(tuples_per_group * 2, agg_tuple->count2_);
      EXPECT_EQ(tuples_per_group * 10, agg_tuple->count3_);
      group_count++;
    }

    EXPECT_EQ(num_groups, group_count);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableTest, SimplePartitionedInsertionTest) {
  const uint32_t num_tuples = 10000;

  std::mt19937 generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 9);

  for (uint32_t idx = 0; idx < num_tuples; idx++) {
    InputTuple input(distribution(generator), 1);
    auto *existing = reinterpret_cast<AggTuple *>(
        AggTable()->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      existing->Advance(input);
    } else {
      auto *new_agg = AggTable()->InsertPartitioned(input.Hash());
      new (new_agg) AggTuple(input);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableTest, BatchProcessTest) {
  const uint32_t num_groups = 16;

  const auto hash_fn = [](void *x) {
    auto iters = reinterpret_cast<ProjectedColumnsIterator **>(x);
    auto key = iters[0]->Get<uint32_t, false>(0, nullptr);
    return util::Hasher::Hash(reinterpret_cast<const uint8_t *>(key), sizeof(uint32_t));
  };

  const auto key_eq = [](const void *agg, const void *x) {
    auto agg_tuple = reinterpret_cast<const AggTuple *>(agg);
    auto iters = reinterpret_cast<const ProjectedColumnsIterator *const *>(x);
    auto pci_key = iters[0]->Get<uint32_t, false>(0, nullptr);
    return agg_tuple->key_ == *pci_key;
  };

  const auto init_agg = [](void *agg, void *x) {
    auto iters = reinterpret_cast<ProjectedColumnsIterator **>(x);
    auto key = iters[0]->Get<uint32_t, false>(0, nullptr);
    auto val = iters[0]->Get<uint32_t, false>(1, nullptr);
    new (agg) AggTuple(InputTuple(*key, *val));
  };

  const auto advance_agg = [](void *agg, void *x) {
    auto agg_tuple = reinterpret_cast<AggTuple *>(agg);
    auto iters = reinterpret_cast<ProjectedColumnsIterator **>(x);
    auto key = iters[0]->Get<uint32_t, false>(0, nullptr);
    auto val = iters[0]->Get<uint32_t, false>(1, nullptr);
    agg_tuple->Advance(InputTuple(*key, *val));
  };

  auto *projected_columns = MakeProjectedColumns();

  alignas(common::Constants::CACHELINE_SIZE) uint32_t keys[common::Constants::K_DEFAULT_VECTOR_SIZE];
  alignas(common::Constants::CACHELINE_SIZE) uint32_t vals[common::Constants::K_DEFAULT_VECTOR_SIZE];

  for (uint32_t run = 0; run < 10; run++) {
    // Fill keys and value
    std::random_device random;
    for (uint32_t idx = 0; idx < common::Constants::K_DEFAULT_VECTOR_SIZE; idx++) {
      keys[idx] = idx % num_groups;
      vals[idx] = 1;
    }

    // Setup projection
    std::memcpy(projected_columns->ColumnStart(0), keys, common::Constants::K_DEFAULT_VECTOR_SIZE);
    std::memcpy(projected_columns->ColumnStart(1), vals, common::Constants::K_DEFAULT_VECTOR_SIZE);

    // Process
    ProjectedColumnsIterator pci(projected_columns);
    ProjectedColumnsIterator *iters[] = {&pci};
    AggTable()->ProcessBatch(iters, hash_fn, key_eq, init_agg, advance_agg);
  }
  FreeProjectedColumns();
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableTest, OverflowPartitonIteratorTest) {
  struct Data {
    uint32_t key_{5};
    uint32_t val_{10};
  };

  struct TestEntry : public HashTableEntry {
    Data data_;
    TestEntry() : HashTableEntry(), data_{} {}
    TestEntry(uint32_t key, uint32_t val) : HashTableEntry(), data_{key, val} {}
  };

  constexpr uint32_t nparts = 50;
  constexpr uint32_t nentries_per_part = 10;

  // Allocate partitions
  std::array<HashTableEntry *, nparts> partitions{};
  partitions.fill(nullptr);

  //
  // Test: check iteration over an empty partitions array
  //

  {
    uint32_t count = 0;
    AggregationOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      count++;
    }
    EXPECT_EQ(0u, count);
  }

  //
  // Test: insert one entry in the middle partition and ensure we find it
  //

  {
    std::vector<std::unique_ptr<TestEntry>> entries;
    entries.emplace_back(std::make_unique<TestEntry>(100, 200));

    HashTableEntry *entry = entries[0].get();
    const uint32_t part_idx = nparts / 2;
    entry->next_ = partitions[part_idx];
    partitions[part_idx] = entry;

    // Check
    uint32_t count = 0;
    AggregationOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      EXPECT_EQ(100u, iter.GetPayloadAs<Data>()->key_);
      EXPECT_EQ(200u, iter.GetPayloadAs<Data>()->val_);
      count++;
    }
    EXPECT_EQ(1u, count);
  }

  partitions.fill(nullptr);

  //
  // Test: create a list of nparts partitions and vary the number of entries in
  //       each partition from [0, nentries_per_part). Ensure the counts match.
  //

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
    AggregationOverflowPartitionIterator iter(partitions.begin(), partitions.end());
    for (; iter.HasNext(); iter.Next()) {
      count++;
    }
    EXPECT_EQ(num_entries, count);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableTest, ParallelAggregationTest) {
  const uint32_t num_aggs = 100;

  auto init_ht = [](void *ctx, void *aht) {
    auto exec_ctx = reinterpret_cast<exec::ExecutionContext *>(ctx);
    new (aht) AggregationHashTable(exec_ctx->GetMemoryPool(), sizeof(AggTuple));
  };

  auto destroy_ht = [](void *ctx, void *aht) {
    reinterpret_cast<AggregationHashTable *>(aht)->~AggregationHashTable();
  };

  auto build_agg_table = [&](AggregationHashTable *agg_table) {
    std::mt19937 generator;
    std::uniform_int_distribution<uint64_t> distribution(0, num_aggs - 1);

    for (uint32_t idx = 0; idx < 10000; idx++) {
      InputTuple input(distribution(generator), 1);
      auto *existing = reinterpret_cast<AggTuple *>(
          agg_table->Lookup(input.Hash(), AggTupleKeyEq, reinterpret_cast<const void *>(&input)));
      if (existing != nullptr) {
        existing->Advance(input);
      } else {
        auto *new_agg = agg_table->InsertPartitioned(input.Hash());
        new (new_agg) AggTuple(input);
      }
    }
  };

  auto merge = [](void *ctx, AggregationHashTable *table, AggregationOverflowPartitionIterator *iter) {
    for (; iter->HasNext(); iter->Next()) {
      auto *partial_agg = iter->GetPayloadAs<AggTuple>();
      auto *existing = reinterpret_cast<AggTuple *>(table->Lookup(iter->GetHash(), AggAggKeyEq, partial_agg));
      if (existing != nullptr) {
        existing->Merge(*partial_agg);
      } else {
        auto *new_agg = table->Insert(iter->GetHash());
        new (new_agg) AggTuple(*partial_agg);
      }
    }
  };

  struct QS {
    std::atomic<uint32_t> row_count_;
  };

  auto scan = [](void *query_state, void *thread_state, const AggregationHashTable *agg_table) {
    auto *qs = reinterpret_cast<QS *>(query_state);
    qs->row_count_ += static_cast<uint32_t>(agg_table->NumElements());
  };

  QS qstate{0};
  // Create container
  ThreadStateContainer container(exec_ctx_->GetMemoryPool());

  // Build thread-local tables
  container.Reset(sizeof(AggregationHashTable), init_ht, destroy_ht, exec_ctx_.get());
  auto aggs = {0, 1, 2, 3};
  tbb::task_scheduler_init sched;
  tbb::parallel_for_each(aggs.begin(), aggs.end(), [&](UNUSED_ATTRIBUTE auto x) {
    auto aht = container.AccessThreadStateOfCurrentThreadAs<AggregationHashTable>();
    build_agg_table(aht);
  });

  AggregationHashTable main_table(exec_ctx_->GetMemoryPool(), sizeof(AggTuple));

  // Move memory
  main_table.TransferMemoryAndPartitions(&container, 0, merge);
  container.Clear();

  // Scan
  main_table.ExecuteParallelPartitionedScan(&qstate, &container, scan);

  // Check
  EXPECT_EQ(num_aggs, qstate.row_count_.load(std::memory_order_seq_cst));
}

}  // namespace terrier::execution::sql::test
