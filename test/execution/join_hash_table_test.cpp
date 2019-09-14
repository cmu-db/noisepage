#include <random>
#include <vector>

#include "execution/tpl_test.h"

#include <tbb/tbb.h>  // NOLINT

#include "execution/sql/join_hash_table.h"
#include "execution/sql/thread_state_container.h"
#include "execution/util/hash.h"

namespace terrier::execution::sql::test {

/**
 * This is the tuple we insert into the hash table
 */
struct Tuple {
  uint64_t a_, b_, c_, d_;
};

/**
 * The function to determine whether two tuples have equivalent keys
 */
static bool TupleKeyEq(UNUSED_ATTRIBUTE void *_, void *probe_tuple, void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a_ == rhs->a_;
}

class JoinHashTableTest : public TplTest {
 public:
  JoinHashTableTest() : memory_(nullptr) {}

  MemoryPool *Memory() { return &memory_; }

  GenericHashTable *GenericTableFor(JoinHashTable *join_hash_table) { return &join_hash_table->generic_hash_table_; }

  ConciseHashTable *ConciseTableFor(JoinHashTable *join_hash_table) { return &join_hash_table->concise_hash_table_; }

  BloomFilter *BloomFilterFor(JoinHashTable *join_hash_table) { return &join_hash_table->bloom_filter_; }

 private:
  MemoryPool memory_;
};

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, LazyInsertionTest) {
  // Test data
  const uint32_t num_tuples = 10;
  std::vector<Tuple> tuples(num_tuples);

  // Populate test data
  {
    std::mt19937 generator;
    std::uniform_int_distribution<uint64_t> distribution;

    for (uint32_t i = 0; i < num_tuples; i++) {
      tuples[i].a_ = distribution(generator);
      tuples[i].b_ = distribution(generator);
      tuples[i].c_ = distribution(generator);
      tuples[i].d_ = distribution(generator);
    }
  }

  JoinHashTable join_hash_table(Memory(), sizeof(Tuple));

  // The table
  for (const auto &tuple : tuples) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&tuple.a_), sizeof(tuple.a_));
    auto *space = join_hash_table.AllocInputTuple(hash_val);
    *reinterpret_cast<Tuple *>(space) = tuple;
  }

  // Before build, the generic hash table shouldn't be populated, but the join
  // table's storage should have buffered all input tuples
  EXPECT_EQ(num_tuples, join_hash_table.NumElements());
  EXPECT_EQ(0u, GenericTableFor(&join_hash_table)->NumElements());

  // Try to build
  join_hash_table.Build();

  // Post-build, the sizes_ should be synced up since all tuples were inserted
  // into the GHT
  EXPECT_EQ(num_tuples, join_hash_table.NumElements());
  EXPECT_EQ(num_tuples, GenericTableFor(&join_hash_table)->NumElements());
}

void PopulateJoinHashTable(JoinHashTable *jht, uint32_t num_tuples, uint32_t dup_scale_factor) {
  for (uint32_t rep = 0; rep < dup_scale_factor; rep++) {
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto hash_val = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&i), sizeof(i));
      auto *space = jht->AllocInputTuple(hash_val);
      auto *tuple = reinterpret_cast<Tuple *>(space);
      tuple->a_ = i;
    }
  }
}

template <bool UseConciseHashTable>
void BuildAndProbeTest(uint32_t num_tuples, uint32_t dup_scale_factor) {
  //
  // The join table
  //

  MemoryPool memory(nullptr);
  JoinHashTable join_hash_table(&memory, sizeof(Tuple), UseConciseHashTable);

  //
  // Populate
  //

  PopulateJoinHashTable(&join_hash_table, num_tuples, dup_scale_factor);

  //
  // Build
  //

  join_hash_table.Build();

  //
  // Do some successful lookups
  //

  for (uint32_t i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&i), sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    uint32_t count = 0;
    const HashTableEntry *entry = nullptr;
    for (auto iter = join_hash_table.Lookup<UseConciseHashTable>(hash_val);
         iter.HasNext(TupleKeyEq, nullptr, reinterpret_cast<void *>(&probe_tuple));) {
      entry = iter.NextMatch();
      ASSERT_TRUE(entry != nullptr);
      auto *matched = reinterpret_cast<const Tuple *>(entry->payload_);
      EXPECT_EQ(i, matched->a_);
      count++;
    }
    EXPECT_EQ(dup_scale_factor, count) << "Expected to find " << dup_scale_factor << " matches, but key [" << i
                                       << "] found " << count << " matches";
  }

  //
  // Do some unsuccessful lookups.
  //

  for (uint32_t i = num_tuples; i < num_tuples + 1000; i++) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&i), sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    for (auto iter = join_hash_table.Lookup<UseConciseHashTable>(hash_val);
         iter.HasNext(TupleKeyEq, nullptr, reinterpret_cast<void *>(&probe_tuple));) {
      FAIL() << "Should not find any matches for key [" << i << "] that was not inserted into the join hash table";
    }
  }
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, UniqueKeyLookupTest) { BuildAndProbeTest<false>(400, 1); }

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, DuplicateKeyLookupTest) { BuildAndProbeTest<false>(400, 5); }

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, UniqueKeyConciseTableTest) { BuildAndProbeTest<true>(400, 1); }

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, DuplicateKeyLookupConciseTableTest) { BuildAndProbeTest<true>(400, 5); }

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, ParallelBuildTest) {
  const uint32_t num_tuples = 100000;

  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);

  container.Reset(
      sizeof(JoinHashTable),
      [](auto *ctx, auto *s) { new (s) JoinHashTable(reinterpret_cast<MemoryPool *>(ctx), sizeof(Tuple)); },
      [](auto *ctx, auto *s) { reinterpret_cast<JoinHashTable *>(s)->~JoinHashTable(); }, &memory);

  // Parallel populate hash tables
  tbb::task_scheduler_init sched;
  tbb::blocked_range<std::size_t> block_range(0, 4, 1);
  tbb::parallel_for(block_range, [&](const auto &range) {
    auto *jht = container.AccessThreadStateOfCurrentThreadAs<JoinHashTable>();
    EXECUTION_LOG_INFO("JHT @ {:p}", (void *)jht);
    PopulateJoinHashTable(jht, num_tuples, 1);
  });

  JoinHashTable main_jht(&memory, sizeof(Tuple), false);
  main_jht.MergeParallel(&container, 0);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, DISABLED_PerfTest) {
  const uint32_t num_tuples = 10000000;

  auto bench = [this](bool concise, uint32_t num_tuples) {
    JoinHashTable join_hash_table(Memory(), sizeof(Tuple), concise);

    //
    // Build random input
    //

    std::random_device random;
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = random();
      auto hash_val = util::Hasher::Hash<util::HashMethod::Crc>(reinterpret_cast<const uint8_t *>(&key), sizeof(key));
      auto *space = join_hash_table.AllocInputTuple(hash_val);
      auto *tuple = reinterpret_cast<Tuple *>(space);

      tuple->a_ = hash_val;
      tuple->b_ = i + 1;
      tuple->c_ = i + 2;
      tuple->d_ = i + 3;
    }

    util::Timer<std::milli> timer;
    timer.Start();

    join_hash_table.Build();

    timer.Stop();

    auto mtps = (num_tuples / timer.Elapsed()) / 1000.0;
    auto size_in_kb = static_cast<double>(concise ? ConciseTableFor(&join_hash_table)->GetTotalMemoryUsage()
                                                  : GenericTableFor(&join_hash_table)->GetTotalMemoryUsage()) /
                      1024.0;
    EXECUTION_LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    EXECUTION_LOG_INFO("# Tuples    : {}", num_tuples)
    EXECUTION_LOG_INFO("Table size  : {} common::Constants::KB", size_in_kb);
    EXECUTION_LOG_INFO("Insert+Build: {} ms ({:.2f} Mtps)", timer.Elapsed(), mtps);
  };

  bench(false, num_tuples);
  bench(true, num_tuples);
}

}  // namespace terrier::execution::sql::test
