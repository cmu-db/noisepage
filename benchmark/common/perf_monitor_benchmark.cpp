#include "benchmark/benchmark.h"
#include "common/perf_monitor.h"
#include "common/big_perf_monitor.h"

namespace terrier {

class PerfMonitorBenchmarks : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {

  }

  void TearDown(const benchmark::State &state) final {}
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmarks, PerfMonitor)(benchmark::State &state) {
  common::PerfMonitor monitor;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    monitor.StartEvents();
    monitor.StopEvents();
  }
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmarks, PerfMonitorWithConstruction)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    common::PerfMonitor monitor;
    monitor.StartEvents();
    monitor.StopEvents();
  }
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmarks, BigPerfMonitor)(benchmark::State &state) {
  common::BigPerfMonitor monitor;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    monitor.StartEvents();
    monitor.StopEvents();
  }
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmarks, BigPerfMonitorWithConstruction)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    common::BigPerfMonitor monitor;
    monitor.StartEvents();
    monitor.StopEvents();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(PerfMonitorBenchmarks, PerfMonitor);
BENCHMARK_REGISTER_F(PerfMonitorBenchmarks, PerfMonitorWithConstruction);
BENCHMARK_REGISTER_F(PerfMonitorBenchmarks, BigPerfMonitor);
BENCHMARK_REGISTER_F(PerfMonitorBenchmarks, BigPerfMonitorWithConstruction);
}  // namespace terrier
