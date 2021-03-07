#include "self_driving/planning/pilot.h"

#include <memory>
#include <utility>

#include "common/action_context.h"
#include "execution/exec_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "optimizer/statistics/stats_storage.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/planning/mcts/monte_carlo_tree_search.h"
#include "self_driving/planning/pilot_util.h"
#include "settings/settings_manager.h"

namespace noisepage::selfdriving {

Pilot::Pilot(std::string model_save_path, std::string forecast_model_save_path,
             common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager, uint64_t workload_forecast_interval)
    : model_save_path_(std::move(model_save_path)),
      forecast_model_save_path_(std::move(forecast_model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      workload_forecast_interval_(workload_forecast_interval) {
  forecast_ = nullptr;
  while (!model_server_manager_->ModelServerStarted()) {
  }
}

void Pilot::PerformForecasterTrain() {
  std::vector<std::string> models{"LSTM"};
  std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};
  modelserver::ModelServerFuture<std::string> future;
  model_server_manager_->TrainForecastModel(models, input_path, forecast_model_save_path_, workload_forecast_interval_,
                                            common::ManagedPointer(&future));
  future.Wait();
}

void Pilot::PerformPlanning() {
  forecast_ = std::make_unique<WorkloadForecast>(workload_forecast_interval_);

  metrics_thread_->PauseMetrics();
  std::vector<std::pair<const std::string, catalog::db_oid_t>> best_action_seq;
  Pilot::ActionSearch(&best_action_seq);
  metrics_thread_->ResumeMetrics();
}

void Pilot::ActionSearch(std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq) {
  auto num_segs = forecast_->GetNumberOfSegments();
  auto end_segment_index = std::min(action_planning_horizon_ - 1, num_segs - 1);

  auto mcst =
      pilot::MonteCarloTreeSearch(common::ManagedPointer(this), common::ManagedPointer(forecast_), end_segment_index);
  mcst.BestAction(simulation_number_, best_action_seq);
  for (uint64_t i = 0; i < best_action_seq->size(); i++) {
    SELFDRIVING_LOG_INFO(fmt::format("Action Selected: Time Interval: {}; Action Command: {} Applied to Database {}", i,
                                     best_action_seq->at(i).first,
                                     static_cast<uint32_t>(best_action_seq->at(i).second)));
  }
  PilotUtil::ApplyAction(common::ManagedPointer(this), best_action_seq->begin()->first,
                         best_action_seq->begin()->second);
}

void Pilot::ExecuteForecast(std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                     std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction,
                            uint64_t start_segment_index, uint64_t end_segment_index) {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
  // first we make sure the pipeline metrics flag as well as the counters is enabled. Also set the sample rate to be 0
  // so that every query execution is being recorded

  // record previous parameters to be restored at the end of this function
  const bool old_metrics_enable = settings_manager_->GetBool(settings::Param::pipeline_metrics_enable);
  const bool old_counters_enable = settings_manager_->GetBool(settings::Param::counters_enable);
  const auto old_sample_rate = settings_manager_->GetInt64(settings::Param::pipeline_metrics_sample_rate);

  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  if (!old_metrics_enable) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  if (!old_counters_enable) {
    settings_manager_->SetBool(settings::Param::counters_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, 100, common::ManagedPointer(action_context),
                            EmptySetterCallback);

  std::vector<execution::query_id_t> pipeline_qids;
  // Collect pipeline metrics of forecasted queries within the interval of segments
  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_), start_segment_index,
                                                          end_segment_index, &pipeline_qids);
  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::InferenceWithFeatures(model_save_path_, model_server_manager_, pipeline_qids, pipeline_data,
                                   pipeline_to_prediction);

  // restore the old parameters
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  if (!old_metrics_enable) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  if (!old_counters_enable) {
    settings_manager_->SetBool(settings::Param::counters_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, old_sample_rate,
                            common::ManagedPointer(action_context), EmptySetterCallback);
}

}  // namespace noisepage::selfdriving
