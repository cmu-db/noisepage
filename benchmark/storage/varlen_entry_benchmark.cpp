#include <memory>

#include "benchmark/benchmark.h"
#include "storage/storage_defs.h"
#include "test_util/storage_test_util.h"

namespace terrier {

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

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, HashInline)->DenseRange(1,12,1);
BENCHMARK_REGISTER_F(VarlenEntryBenchmark, HashNotInline)->RangeMultiplier(2)->Range(16,4096);;
// clang-format on

}  // namespace terrier
