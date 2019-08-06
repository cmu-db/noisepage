#include <random>
#include <vector>

#include "execution/tpl_test.h"  // NOLINT

#include <tbb/tbb.h>  // NOLINT

#include "execution/sql/join_hash_table.h"
#include "execution/sql/thread_state_container.h"
#include "execution/util/hash.h"

namespace terrier::sql::test {

/**
 * This is the tuple we insert into the hash table
 */
struct Tuple {
  u64 a, b, c, d;
};

/**
 * The function to determine whether two tuples have equivalent keys
 */
static inline bool TupleKeyEq(UNUSED void *_, void *probe_tuple, void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a == rhs->a;
}

class JoinHashTableTest : public TplTest {
 public:
  JoinHashTableTest() : memory_(nullptr) {}

  MemoryPool *memory() { return &memory_; }

  GenericHashTable *GenericTableFor(JoinHashTable *join_hash_table) { return &join_hash_table->generic_hash_table_; }

  ConciseHashTable *ConciseTableFor(JoinHashTable *join_hash_table) { return &join_hash_table->concise_hash_table_; }

  BloomFilter *BloomFilterFor(JoinHashTable *join_hash_table) { return &join_hash_table->bloom_filter_; }

 private:
  MemoryPool memory_;
};

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, LazyInsertionTest) {
  // Test data
  const u32 num_tuples = 10;
  std::vector<Tuple> tuples(num_tuples);

  // Populate test data
  {
    std::mt19937 generator;
    std::uniform_int_distribution<u64> distribution;

    for (u32 i = 0; i < num_tuples; i++) {
      tuples[i].a = distribution(generator);
      tuples[i].b = distribution(generator);
      tuples[i].c = distribution(generator);
      tuples[i].d = distribution(generator);
    }
  }

  JoinHashTable join_hash_table(memory(), sizeof(Tuple));

  // The table
  for (const auto &tuple : tuples) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const u8 *>(&tuple.a), sizeof(tuple.a));
    auto *space = join_hash_table.AllocInputTuple(hash_val);
    *reinterpret_cast<Tuple *>(space) = tuple;
  }

  // Before build, the generic hash table shouldn't be populated, but the join
  // table's storage should have buffered all input tuples
  EXPECT_EQ(num_tuples, join_hash_table.num_elements());
  EXPECT_EQ(0u, GenericTableFor(&join_hash_table)->num_elements());

  // Try to build
  join_hash_table.Build();

  // Post-build, the sizes should be synced up since all tuples were inserted
  // into the GHT
  EXPECT_EQ(num_tuples, join_hash_table.num_elements());
  EXPECT_EQ(num_tuples, GenericTableFor(&join_hash_table)->num_elements());
}

void PopulateJoinHashTable(JoinHashTable *jht, u32 num_tuples, u32 dup_scale_factor) {
  for (u32 rep = 0; rep < dup_scale_factor; rep++) {
    for (u32 i = 0; i < num_tuples; i++) {
      auto hash_val = util::Hasher::Hash(reinterpret_cast<const u8 *>(&i), sizeof(i));
      auto *space = jht->AllocInputTuple(hash_val);
      auto *tuple = reinterpret_cast<Tuple *>(space);
      tuple->a = i;
    }
  }
}

template <bool UseConciseHashTable>
void BuildAndProbeTest(u32 num_tuples, u32 dup_scale_factor) {
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

  for (u32 i = 0; i < num_tuples; i++) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const u8 *>(&i), sizeof(i));
    Tuple probe_tuple = {i, 0, 0, 0};
    u32 count = 0;
    const HashTableEntry *entry = nullptr;
    for (auto iter = join_hash_table.Lookup<UseConciseHashTable>(hash_val);
         iter.HasNext(TupleKeyEq, nullptr, reinterpret_cast<void *>(&probe_tuple));) {
      entry = iter.NextMatch();
      ASSERT_TRUE(entry != nullptr);
      auto *matched = reinterpret_cast<const Tuple *>(entry->payload);
      EXPECT_EQ(i, matched->a);
      count++;
    }
    EXPECT_EQ(dup_scale_factor, count) << "Expected to find " << dup_scale_factor << " matches, but key [" << i
                                       << "] found " << count << " matches";
  }

  //
  // Do some unsuccessful lookups.
  //

  for (u32 i = num_tuples; i < num_tuples + 1000; i++) {
    auto hash_val = util::Hasher::Hash(reinterpret_cast<const u8 *>(&i), sizeof(i));
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
  const u32 num_tuples = 100000;

  MemoryPool memory(nullptr);
  ThreadStateContainer container(&memory);

  container.Reset(sizeof(JoinHashTable),
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
  const u32 num_tuples = 10000000;

  auto bench = [this](bool concise, u32 num_tuples) {
    JoinHashTable join_hash_table(memory(), sizeof(Tuple), concise);

    //
    // Build random input
    //

    std::random_device random;
    for (u32 i = 0; i < num_tuples; i++) {
      auto key = random();
      auto hash_val = util::Hasher::Hash<util::HashMethod::Crc>(reinterpret_cast<const u8 *>(&key), sizeof(key));
      auto *space = join_hash_table.AllocInputTuple(hash_val);
      auto *tuple = reinterpret_cast<Tuple *>(space);

      tuple->a = hash_val;
      tuple->b = i + 1;
      tuple->c = i + 2;
      tuple->d = i + 3;
    }

    util::Timer<std::milli> timer;
    timer.Start();

    join_hash_table.Build();

    timer.Stop();

    auto mtps = (num_tuples / timer.elapsed()) / 1000.0;
    auto size_in_kb = static_cast<double>(concise ? ConciseTableFor(&join_hash_table)->GetTotalMemoryUsage()
                                                  : GenericTableFor(&join_hash_table)->GetTotalMemoryUsage()) /
                      1024.0;
    EXECUTION_LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    EXECUTION_LOG_INFO("# Tuples    : {}", num_tuples)
    EXECUTION_LOG_INFO("Table size  : {} KB", size_in_kb);
    EXECUTION_LOG_INFO("Insert+Build: {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
  };

  bench(false, num_tuples);
  bench(true, num_tuples);
}

}  // namespace terrier::sql::test
