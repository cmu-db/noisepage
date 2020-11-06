#include <tbb/tbb.h>

#include <random>
#include <vector>

#include "common/hash_util.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql_test.h"

// TODO(WAN): can't FRIEND_TEST unless in the same namespace
namespace noisepage::execution::sql {

/// This is the tuple we insert into the hash table
struct Tuple {
  uint64_t a_, b_, c_, d_;

  hash_t Hash() const { return common::HashUtil::Hash(a_); }
};

class JoinHashTableTest : public SqlBasedTest {
 public:
  JoinHashTableTest() = default;
};

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, LazyInsertionTest) {
  auto exec_ctx = MakeExecCtx();
  exec::ExecutionSettings exec_settings{};
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

  JoinHashTable join_hash_table(exec_settings, exec_ctx.get(), sizeof(Tuple));

  // The table
  for (const auto &tuple : tuples) {
    // Allocate
    auto *space = join_hash_table.AllocInputTuple(tuple.Hash());
    // Insert (by copying) into table
    *reinterpret_cast<Tuple *>(space) = tuple;
  }

  // Before build, the generic hash table shouldn't be populated, but the join
  // table's storage should have buffered all input tuples
  EXPECT_EQ(num_tuples, join_hash_table.GetTupleCount());
  EXPECT_EQ(0u, join_hash_table.chaining_hash_table_.GetElementCount());

  // Try to build
  join_hash_table.Build();

  // Post-build, the sizes should be synced up since all tuples were inserted
  // into the GHT
  EXPECT_EQ(num_tuples, join_hash_table.GetTupleCount());
  EXPECT_EQ(num_tuples, join_hash_table.chaining_hash_table_.GetElementCount());
}

void PopulateJoinHashTable(JoinHashTable *jht, uint32_t num_tuples, uint32_t dup_scale_factor) {
  for (uint32_t rep = 0; rep < dup_scale_factor; rep++) {
    for (uint32_t i = 0; i < num_tuples; i++) {
      // Create tuple
      auto tuple = Tuple{i, 1, 2};
      // Allocate in hash table
      auto *space = jht->AllocInputTuple(tuple.Hash());
      // Copy contents into hash
      *reinterpret_cast<Tuple *>(space) = tuple;
    }
  }
}

template <bool UseCHT>
void BuildAndProbeTest(exec::ExecutionContext *exec_ctx, uint32_t num_tuples, uint32_t dup_scale_factor) {
  exec::ExecutionSettings exec_settings{};
  //
  // The join table
  //

  JoinHashTable join_hash_table(exec_settings, exec_ctx, sizeof(Tuple), UseCHT);

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
    // The probe tuple
    Tuple probe_tuple = {i, 0, 0, 0};
    // Perform probe
    uint32_t count = 0;
    for (auto iter = join_hash_table.Lookup<UseCHT>(probe_tuple.Hash()); iter.HasNext();) {
      auto *matched = reinterpret_cast<const Tuple *>(iter.GetMatchPayload());
      if (matched->a_ == probe_tuple.a_) {
        count++;
      }
    }
    EXPECT_EQ(dup_scale_factor, count) << "Expected to find " << dup_scale_factor << " matches, but key [" << i
                                       << "] found " << count << " matches";
  }

  //
  // Do some unsuccessful lookups.
  //

  for (uint32_t i = num_tuples; i < num_tuples + 1000; i++) {
    // A tuple that should NOT find any join partners
    Tuple probe_tuple = {i, 0, 0, 0};
    for (auto iter = join_hash_table.Lookup<UseCHT>(probe_tuple.Hash()); iter.HasNext();) {
      FAIL() << "Should not find any matches for key [" << i << "] that was not inserted into the join hash table";
    }
  }
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, UniqueKeyLookupTest) {
  auto exec_ctx = MakeExecCtx();
  BuildAndProbeTest<false>(exec_ctx.get(), 400, 1);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, DuplicateKeyLookupTest) {
  auto exec_ctx = MakeExecCtx();
  BuildAndProbeTest<false>(exec_ctx.get(), 400, 5);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, UniqueKeyConciseTableTest) {
  auto exec_ctx = MakeExecCtx();
  BuildAndProbeTest<true>(exec_ctx.get(), 400, 1);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, DuplicateKeyLookupConciseTableTest) {
  auto exec_ctx = MakeExecCtx();
  BuildAndProbeTest<true>(exec_ctx.get(), 400, 5);
}

// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, ParallelBuildTest) {
  auto exec_ctx = MakeExecCtx();
  exec::ExecutionSettings exec_settings{};
  tbb::task_scheduler_init sched;

  constexpr bool use_concise_ht = false;
  const uint32_t num_tuples = 10000;
  const uint32_t num_thread_local_tables = 4;

  ThreadStateContainer container(exec_ctx->GetMemoryPool());

  struct Context {
    exec::ExecutionContext *exec_ctx_;
    exec::ExecutionSettings *settings_;
  };

  Context ctx{exec_ctx.get(), &exec_settings};

  container.Reset(
      sizeof(JoinHashTable),
      [](auto *ctx, auto *s) {
        auto context = reinterpret_cast<Context *>(ctx);
        new (s) JoinHashTable(*context->settings_, context->exec_ctx_, sizeof(Tuple), use_concise_ht);
      },
      [](auto *ctx, auto *s) { reinterpret_cast<JoinHashTable *>(s)->~JoinHashTable(); }, &ctx);

  // Parallel populate each of the thread-local hash tables
  LaunchParallel(num_thread_local_tables, [&](auto tid) {
    auto *jht = container.AccessCurrentThreadStateAs<JoinHashTable>();
    PopulateJoinHashTable(jht, num_tuples, 1);
  });

  JoinHashTable main_jht(exec_settings, exec_ctx.get(), sizeof(Tuple), false);
  main_jht.MergeParallel(&container, 0);

  // Each of the thread-local tables inserted the same data, i.e., tuples whose
  // keys are in the range [0, num_tuples). Thus, in the final table there
  // should be num_thread_local_tables * num_tuples keys, where each of the
  // num_tuples tuples have num_thread_local_tables duplicates.
  //
  // Check now.

  EXPECT_EQ(num_tuples * num_thread_local_tables, main_jht.GetTupleCount());

  for (uint32_t i = 0; i < num_tuples; i++) {
    auto probe = Tuple{i, 1, 2, 3};
    uint32_t count = 0;
    for (auto iter = main_jht.Lookup<use_concise_ht>(probe.Hash()); iter.HasNext();) {
      auto *matched = reinterpret_cast<const Tuple *>(iter.GetMatchPayload());
      if (matched->a_ == probe.a_) {
        count++;
      }
    }
    EXPECT_EQ(num_thread_local_tables, count);
  }
}

#if 0
// NOLINTNEXTLINE
TEST_F(JoinHashTableTest, PerfTest) {
  const uint32_t num_tuples = 10000000;

  auto bench = [this](bool concise, uint32_t num_tuples) {
    JoinHashTable join_hash_table(Memory(), sizeof(Tuple), concise);

    //
    // Build random input
    //

    std::random_device random;
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = random();
      auto *tuple = reinterpret_cast<Tuple *>(
          join_hash_table.AllocInputTuple(common::HashUtil::Hash(key)));

      tuple->a = key;
      tuple->b = i + 1;
      tuple->c = i + 2;
      tuple->d = i + 3;
    }

    util::Timer<std::milli> timer;
    timer.Start();

    join_hash_table.Build();

    timer.Stop();

    auto mtps = (num_tuples / timer.elapsed()) / 1000.0;
    auto size_in_kb =
        (concise ? ConciseTableFor(&join_hash_table)->GetTotalMemoryUsage()
                 : GenericTableFor(&join_hash_table)->GetTotalMemoryUsage()) /
        1024.0;
    LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    LOG_INFO("# Tuples    : {}", num_tuples)
    LOG_INFO("Table size  : {} KB", size_in_kb);
    LOG_INFO("Insert+Build: {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
  };

  bench(false, num_tuples);
  bench(true, num_tuples);
}
#endif

}  // namespace noisepage::execution::sql
