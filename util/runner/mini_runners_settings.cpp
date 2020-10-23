#include <cstdlib>
#include <cstring>

#include "loggers/settings_logger.h"
#include "runner/mini_runners_settings.h"

namespace terrier::runner {

struct Arg {
  const char *match_;
  bool found_;
  const char *value_;
  int int_value_;
};

void MiniRunnersSettings::InitializeFromArguments(int argc, char **argv) {
  Arg port_info{"--port=", false};
  Arg filter_info{"--benchmark_filter=", false, "*"};
  Arg skip_large_rows_runs_info{"--skip_large_rows_runs=", false};
  Arg warm_num_info{"--warm_num=", false};
  Arg rerun_info{"--rerun=", false};
  Arg updel_info{"--updel_limit=", false};
  Arg warm_limit_info{"--warm_limit=", false};
  Arg gen_test_data{"--gen_test=", false};
  Arg create_index_small_data{"--create_index_small_limit=", false};
  Arg create_index_car_data{"--create_index_large_car_num=", false};
  Arg run_limit{"--mini_runner_rows_limit=", false};
  Arg *args[] = {
      &port_info,       &filter_info,   &skip_large_rows_runs_info, &warm_num_info,         &rerun_info, &updel_info,
      &warm_limit_info, &gen_test_data, &create_index_small_data,   &create_index_car_data, &run_limit};

  for (int i = 0; i < argc; i++) {
    for (auto *arg : args) {
      if (strstr(argv[i], arg->match_) != nullptr) {
        arg->found_ = true;
        arg->value_ = strstr(argv[i], "=") + 1;
        arg->int_value_ = atoi(arg->value_);
      }
    }
  }

  if (gen_test_data.found_) generate_test_data_ = true;
  if (filter_info.found_) target_runner_specified_ = true;
  if (port_info.found_) port_ = port_info.int_value_;
  if (skip_large_rows_runs_info.found_) skip_large_rows_runs_ = true;
  if (warm_num_info.found_) warmup_iterations_num_ = warm_num_info.int_value_;
  if (rerun_info.found_) rerun_iterations_ = rerun_info.int_value_;
  if (updel_info.found_) updel_limit_ = updel_info.int_value_;
  if (warm_limit_info.found_) warmup_rows_limit_ = warm_limit_info.int_value_;
  if (create_index_small_data.found_) create_index_small_limit_ = create_index_small_data.int_value_;
  if (create_index_car_data.found_) create_index_large_cardinality_num_ = create_index_car_data.int_value_;
  if (run_limit.found_) data_rows_limit_ = run_limit.int_value_;

  terrier::LoggersUtil::Initialize();
  SETTINGS_LOG_INFO("Starting mini-runners with this parameter set:");
  SETTINGS_LOG_INFO("Port ({}): {}", port_info.match_, port_);
  SETTINGS_LOG_INFO("Skip Large Rows ({}): {}", skip_large_rows_runs_info.match_, skip_large_rows_runs_);
  SETTINGS_LOG_INFO("Warmup Iterations ({}): {}", warm_num_info.match_, warmup_iterations_num_);
  SETTINGS_LOG_INFO("Rerun Iterations ({}): {}", rerun_info.match_, rerun_iterations_);
  SETTINGS_LOG_INFO("Update/Delete Index Limit ({}): {}", updel_info.match_, updel_limit_);
  SETTINGS_LOG_INFO("Create Index Small Build Limit ({}): {}", create_index_small_data.match_,
                    create_index_small_limit_);
  SETTINGS_LOG_INFO("Create Index Large Cardinality Number Vary ({}): {}", create_index_car_data.match_,
                    create_index_large_cardinality_num_);
  SETTINGS_LOG_INFO("Warmup Rows Limit ({}): {}", warm_limit_info.match_, warmup_rows_limit_);
  SETTINGS_LOG_INFO("Mini Runner Rows Limit ({}): {}", run_limit.match_, data_rows_limit_);
  SETTINGS_LOG_INFO("Filter ({}): {}", filter_info.match_, filter_info.value_);
  SETTINGS_LOG_INFO("Generate Test Data ({}): {}", gen_test_data.match_, gen_test_data.found_);
}

};  // namespace terrier::runner
