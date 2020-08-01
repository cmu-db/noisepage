#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/allocator.h"
#include "common/scoped_timer.h"
#include "storage/storage_defs.h"
#include "test_util/storage_test_util.h"

namespace terrier {

class VarlenEntryHashBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {}

  void TearDown(const benchmark::State &state) final { delete[] buffer_; }

  std::default_random_engine generator_;

  byte *buffer_;
};

#define VARLEN_HASH_BENCHMARK(varlen_bytes)                                                      \
                                                                                                 \
  if constexpr ((varlen_bytes) <= storage::VarlenEntry::InlineThreshold()) {                     \
    byte random_buffer[(varlen_bytes)];                                                          \
    StorageTestUtil::FillWithRandomBytes((varlen_bytes), random_buffer, &generator_);            \
    const auto varlen_entry = storage::VarlenEntry::CreateInline(random_buffer, (varlen_bytes)); \
    /* NOLINTNEXTLINE */                                                                         \
    for (auto _ : state) {                                                                       \
      benchmark::DoNotOptimize(varlen_entry.Hash());                                             \
    }                                                                                            \
  } else {                                                                                       \
    buffer_ = common::AllocationUtil::AllocateAligned((varlen_bytes));                           \
    StorageTestUtil::FillWithRandomBytes((varlen_bytes), buffer_, &generator_);                  \
    const auto varlen_entry = storage::VarlenEntry::Create(buffer_, (varlen_bytes), false);      \
    /* NOLINTNEXTLINE */                                                                         \
    for (auto _ : state) {                                                                       \
      benchmark::DoNotOptimize(varlen_entry.Hash());                                             \
    }                                                                                            \
  }                                                                                              \
                                                                                                 \
  state.SetItemsProcessed(state.iterations());                              \
  state.SetBytesProcessed(state.iterations() * (varlen_bytes));

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, Inline4Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(4); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, Inline8Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(8); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, Inline12Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(12); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline16Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(16); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline32Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(32); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline64Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(64); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline128Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(128); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline256Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(256); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline512Bytes)(benchmark::State &state) { VARLEN_HASH_BENCHMARK(512); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline1024Bytes)(benchmark::State &state) {
  VARLEN_HASH_BENCHMARK(1024);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline2048Bytes)(benchmark::State &state) {
  VARLEN_HASH_BENCHMARK(2048);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(VarlenEntryHashBenchmark, NotInline4096Bytes)(benchmark::State &state) {
  VARLEN_HASH_BENCHMARK(4096);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, Inline4Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, Inline8Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, Inline12Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline16Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline32Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline64Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline128Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline256Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline512Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline1024Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline2048Bytes);
BENCHMARK_REGISTER_F(VarlenEntryHashBenchmark, NotInline4096Bytes);
// clang-format on

}  // namespace terrier
