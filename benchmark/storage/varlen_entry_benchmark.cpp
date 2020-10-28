#include <memory>

#include "benchmark/benchmark.h"
#include "storage/storage_defs.h"
#include "test_util/storage_test_util.h"

namespace noisepage {

/**
 * Currently exercises 2 key areas of VarlenEntry performance: hashing and equality comparisons. The former is mostly
 * exercising the hash functions used for various content lengths. If hashing algorithms are changed/updated, we should
 * run this benchmark. The other part of this benchmark evaluates comparisons of VarlenEntrys, exercising if it just
 * looks at the length, prefix, content, or all of the above. If the logic is changed, we should rerun the benchmark.
 *
 * The benchmark is not currently part of CI because it proved too noisy in Jenkins runs.
 */
class VarlenEntryBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {}

  void TearDown(const benchmark::State &state) final {}

  std::default_random_engine generator_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, HashInline)(benchmark::State &state) {
  const auto varlen_bytes = state.range(0);

  byte random_buffer[varlen_bytes];
  StorageTestUtil::FillWithRandomBytes(varlen_bytes, random_buffer, &generator_);

  const auto varlen_entry = storage::VarlenEntry::CreateInline(random_buffer, varlen_bytes);
  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(varlen_entry.Hash());
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * varlen_bytes);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, HashNotInline)(benchmark::State &state) {
  const auto varlen_bytes = state.range(0);

  byte random_buffer[varlen_bytes];
  StorageTestUtil::FillWithRandomBytes(varlen_bytes, random_buffer, &generator_);

  const auto varlen_entry = storage::VarlenEntry::Create(random_buffer, varlen_bytes, false);
  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(varlen_entry.Hash());
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * (varlen_bytes));
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, EqualityInlineEqualPrefixOnly)(benchmark::State &state) {
  constexpr std::string_view wan = "wan";

  const auto lhs = storage::VarlenEntry::Create(wan);
  const auto rhs = storage::VarlenEntry::Create(wan);

  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs == rhs);
  }

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, EqualityInlineEqualPrefixDifferentLength)(benchmark::State &state) {
  constexpr std::string_view matt = "matt";
  constexpr std::string_view matthew = "matthew";

  const auto lhs = storage::VarlenEntry::Create(matt);
  const auto rhs = storage::VarlenEntry::Create(matthew);

  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs == rhs);
  }

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, EqualityInlineEqualPrefixEqualLength)(benchmark::State &state) {
  constexpr std::string_view johnny = "johnny";
  constexpr std::string_view johnie = "johnie";

  const auto lhs = storage::VarlenEntry::Create(johnny);
  const auto rhs = storage::VarlenEntry::Create(johnie);

  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs == rhs);
  }

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, EqualityInlineDifferentPrefixEqualLength)(benchmark::State &state) {
  constexpr std::string_view matt = "matt";
  constexpr std::string_view john = "john";

  const auto lhs = storage::VarlenEntry::Create(matt);
  const auto rhs = storage::VarlenEntry::Create(john);

  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs == rhs);
  }

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, EqualityNotInlineEqualContentEqualLength)(benchmark::State &state) {
  constexpr std::string_view matthew_was_here = "matthew_was_here";

  const auto lhs = storage::VarlenEntry::Create(matthew_was_here);
  const auto rhs = storage::VarlenEntry::Create(matthew_was_here);

  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs == rhs);
  }

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryBenchmark, EqualityNotInlineDifferentContentEqualLength)(benchmark::State &state) {
  constexpr std::string_view matthew_was_here = "matthew_was_here";
  constexpr std::string_view matthew_was_gone = "matthew_was_gone";

  const auto lhs = storage::VarlenEntry::Create(matthew_was_here);
  const auto rhs = storage::VarlenEntry::Create(matthew_was_gone);

  /* NOLINTNEXTLINE */
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs == rhs);
  }

  state.SetItemsProcessed(state.iterations());
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, HashInline)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(12);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, HashNotInline)->RangeMultiplier(2)->Range(16, 4096);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, EqualityInlineEqualPrefixOnly);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, EqualityInlineEqualPrefixDifferentLength);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, EqualityInlineEqualPrefixEqualLength);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, EqualityInlineDifferentPrefixEqualLength);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, EqualityNotInlineEqualContentEqualLength);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, EqualityNotInlineDifferentContentEqualLength);
// clang-format on

}  // namespace noisepage
