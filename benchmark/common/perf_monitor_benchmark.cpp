#include "benchmark/benchmark.h"
#include "common/perf_monitor.h"

namespace noisepage {

/**
 * These benchmarks exist to verify the performance difference between grouped and ungrouped perf counters. We do not
 * include them in our CI regression checks since their behavior is determined more by the OS than our wrapper.
 */
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
}  // namespace noisepage
