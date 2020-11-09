#include "brain/pilot/pilot.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "brain/forecast/workload_forecast.h"
#include "common/action_context.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/exec_defs.h"
#include "main/db_main.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_manager.h"

namespace noisepage::brain {

Pilot::Pilot(common::ManagedPointer<DBMain> db_main, uint64_t forecast_interval)
    : db_main_(db_main), forecast_interval_(forecast_interval) {
  forecastor_ = nullptr;
}

void Pilot::PerformPlanning() {
  LoadQueryTrace();
  LoadQueryText();
  forecastor_ =
      std::make_unique<WorkloadForecast>(query_timestamp_to_id_, num_executions_, query_id_to_text_, query_text_to_id_,
                                         query_id_to_params_, query_id_to_dboid_, forecast_interval_);

  db_main_->GetMetricsThread()->PauseMetrics();
  ExecuteForecast();
  db_main_->GetMetricsThread()->ResumeMetrics();
}

void Pilot::ExecuteForecast() {
  NOISEPAGE_ASSERT(forecastor_ != nullptr, "Need forecastor initialized.");

  for (const auto &file : metrics::PipelineMetricRawData::FILES) unlink(std::string(file).c_str());

  auto settings_manager = db_main_->GetSettingsManager();
  bool oldval = settings_manager->GetBool(settings::Param::pipeline_metrics_enable);
  bool oldcounter = settings_manager->GetBool(settings::Param::counters_enable);
  uint64_t oldintv = settings_manager->GetInt64(settings::Param::pipeline_metrics_interval);

  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  if (!oldval) {
    settings_manager->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  if (!oldcounter) {
    settings_manager->SetBool(settings::Param::counters_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager->SetInt(settings::Param::pipeline_metrics_interval, 0, common::ManagedPointer(action_context),
                            EmptySetterCallback);

  forecastor_->ExecuteSegments(db_main_);

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  if (!oldval) {
    settings_manager->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  if (!oldcounter) {
    settings_manager->SetBool(settings::Param::counters_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager->SetInt(settings::Param::pipeline_metrics_interval, oldintv, common::ManagedPointer(action_context),
                            EmptySetterCallback);
}

void Pilot::LoadQueryTrace() {
  uint8_t num_cols = 5;
  std::ifstream my_file("./query_trace.csv");
  // Make sure the file is open
  if (!my_file.is_open()) throw std::runtime_error("Could not open file");

  // Helper vars
  std::string line, val_string, type_string;
  if (!my_file.good()) throw std::runtime_error("File stream is not good");

  // ignore header
  std::getline(my_file, line);

  // Read data, line by line
  execution::query_id_t query_id;
  size_t pos, pos2, colnum, curr_size;
  bool null_detected = false;
  std::vector<std::string> val_vec(num_cols, "");

  while (std::getline(my_file, line)) {
    colnum = 0;
    val_vec.assign(num_cols, "");
    // std::cout << line << "\n" << std::flush;
    while ((pos = line.find(',')) != std::string::npos && colnum < num_cols) {
      if (pos > 0) {
        val_vec[colnum] = line.substr(0, pos);
      }
      line.erase(0, pos + 2);
      colnum++;
    }

    if (val_vec[0].empty()) {
      // query_id not found
      continue;
    }

    query_id = static_cast<execution::query_id_t>(std::stoi(val_vec[0]));
    val_string = val_vec[2];
    type_string = val_vec[3];

    std::vector<parser::ConstantValueExpression> param_vec;
    while ((pos = val_string.find(';')) != std::string::npos && (pos2 = type_string.find(';')) != std::string::npos) {
      if (pos > 0) {
        auto cve = parser::ConstantValueExpression::FromString(val_string.substr(0, pos),
                                                               std::stoi(type_string.substr(0, pos2)));

        param_vec.push_back(cve);
      } else {
        // std::cout << "null value detected in query params recorded\n" << std::flush;
        null_detected = true;
        break;
      }
      val_string.erase(0, pos + 1);
      type_string.erase(0, pos2 + 1);
    }

    if (!null_detected) {
      if ((curr_size = query_id_to_params_[query_id].size()) < num_sample_) {
        query_id_to_params_[query_id].push_back(param_vec);
        query_id_to_dboid_[query_id] = std::stoi(val_vec[4]);
        query_timestamp_to_id_[std::stoull(val_vec[1])] = std::make_pair(query_id, curr_size);
        num_executions_[query_id].push_back(1);
      } else {
        num_executions_[query_id][rand() % num_sample_]++;
      }
    }
  }
  // Close file
  my_file.close();
}

void Pilot::LoadQueryText() {
  // Parse qid and query text, assuming they are the first two columns, with query text wrapped in quotations marks

  // Create an input filestream
  std::ifstream my_file("./query_text.csv");
  // Make sure the file is open
  if (!my_file.is_open()) throw std::runtime_error("Could not open file");

  // Helper vars
  std::string line;
  if (!my_file.good()) throw std::runtime_error("File stream is not good");

  // ignore header
  std::getline(my_file, line);

  // Read data, line by line
  execution::query_id_t query_id;
  size_t pos;

  while (std::getline(my_file, line)) {
    // std::cout << line << "\n" << std::flush;
    pos = line.find("\"");
    if (pos == std::string::npos || pos < 3) {
      // no quotation mark found or no query_id found
      continue;
    }
    query_id = static_cast<execution::query_id_t>(std::stoi(line.substr(0, pos - 2)));
    line.erase(0, pos + 1);
    pos = line.find("\"");

    if (pos == std::string::npos) continue;

    query_id_to_text_[query_id] = line.substr(0, pos);
    query_text_to_id_[line.substr(0, pos)] = query_id;
  }
  // Close file
  my_file.close();
}

}  // namespace noisepage::brain
