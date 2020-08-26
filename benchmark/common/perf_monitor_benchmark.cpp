#include "benchmark/benchmark.h"
#include "common/perf_monitor.h"

namespace terrier {

class PerfMonitorBenchmark : public benchmark::Fixture {};

/**
 * Benchmark with inherit flag (count children) false
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmark, Basic)(benchmark::State &state) {
  common::PerfMonitor<false> monitor;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    monitor.Start();
    monitor.Stop();
    monitor.Counters();
  }
  state.SetItemsProcessed(state.iterations());
}

/**
 * Benchmark with inherit flag (count children) true
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmark, Inherit)(benchmark::State &state) {
  common::PerfMonitor<true> monitor;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    monitor.Start();
    monitor.Stop();
    monitor.Counters();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(PerfMonitorBenchmark, Basic);

BENCHMARK_REGISTER_F(PerfMonitorBenchmark, Inherit);
}  // namespace terrier
