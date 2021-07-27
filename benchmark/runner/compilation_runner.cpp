#include <fstream>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "common/dedicated_thread_registry.h"
#include "common/scoped_timer.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"
#include "storage/write_ahead_log/log_manager.h"

#define LOG_TEST_LOG_FILE_NAME "benchmark.txt"

namespace noisepage::runner {

class CompilationRunner : public benchmark::Fixture {};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CompilationRunner, Compilation)(benchmark::State &state) {
  noisepage::LoggersUtil::Initialize();
  execution::ExecutionUtil::InitTPL("./bytecode_handlers_ir.bc");

  auto *const metrics_manager = new metrics::MetricsManager();
  metrics_manager->EnableMetric(metrics::MetricsComponent::COMPILATION);
  metrics_manager->RegisterThread();

  const std::string &path = "../sample_tpl/tpl_tests.txt";
  std::ifstream tpl_tests(path);

  std::string input_line;
  size_t identifier = 0;
  while (std::getline(tpl_tests, input_line)) {
    if (input_line.find(".tpl") != std::string::npos && input_line[0] != '#') {
      // We have found a valid test
      std::string tpl = input_line.substr(0, input_line.find(","));
      std::string target = "../sample_tpl/" + tpl;

      std::ifstream input(target);
      std::string contents((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));
      EXECUTION_LOG_INFO("Running compilation on {}", target);

      execution::exec::ExecutionSettings exec_settings;
      auto exec_query = execution::compiler::ExecutableQuery(contents, nullptr, false, 16, exec_settings,
                                                             transaction::timestamp_t(0));
      for (const auto &fragment : exec_query.GetFragments()) {
        fragment->GetModule()->CompileToMachineCode(execution::query_id_t(identifier));
      }

      identifier++;
    }
  }

  metrics_manager->Aggregate();
  metrics_manager->ToOutput(nullptr);
  metrics_manager->UnregisterThread();
  delete metrics_manager;
  noisepage::LoggersUtil::ShutDown();
}

BENCHMARK_REGISTER_F(CompilationRunner, Compilation)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);

}  // namespace noisepage::runner
