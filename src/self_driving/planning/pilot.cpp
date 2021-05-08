#include "self_driving/planning/pilot.h"

#include <cstdio>
#include <memory>
#include <utility>

#include "common/action_context.h"
#include "common/error/error_code.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/vm/vm_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/planning/mcts/monte_carlo_tree_search.h"
#include "self_driving/planning/pilot_util.h"
#include "settings/settings_manager.h"
#include "task/task_manager.h"
#include "transaction/transaction_manager.h"
#include "util/forecast_recording_util.h"
#include "util/query_exec_util.h"

namespace noisepage::selfdriving {

Pilot::Pilot(std::string ou_model_save_path, std::string interference_model_save_path,
             std::string forecast_model_save_path, common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager,
             std::unique_ptr<util::QueryExecUtil> query_exec_util,
             common::ManagedPointer<task::TaskManager> task_manager, uint64_t workload_forecast_interval,
             uint64_t sequence_length, uint64_t horizon_length)
    : ou_model_save_path_(std::move(ou_model_save_path)),
      interference_model_save_path_(std::move(interference_model_save_path)),
      forecast_model_save_path_(std::move(forecast_model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      query_exec_util_(std::move(query_exec_util)),
      task_manager_(task_manager),
      workload_forecast_interval_(workload_forecast_interval),
      sequence_length_(sequence_length),
      horizon_length_(horizon_length) {
  forecast_ = nullptr;
  while (!model_server_manager_->ModelServerStarted()) {
  }
}

std::pair<uint64_t, uint64_t> Pilot::ComputeTimestampDataRange(uint64_t now, bool train) {
  // Evaluation length is sequence length + 2 horizons
  uint64_t eval_length = sequence_length_ + 2 * horizon_length_;

  if (train) {
    // Pull a range of the order of 5x (assuming classic 80% train/20% test split)
    eval_length *= 5;
  }

  // Sequence length and horizon length are in workload_forecast_interval_ time units
  uint64_t eval_time = eval_length * workload_forecast_interval_;

  return std::make_pair(now - eval_time, now - 1);
}

void Pilot::PerformForecasterTrain() {
  uint64_t timestamp = metrics::MetricsUtil::Now();
  std::vector<std::string> models{"LSTM"};
  modelserver::ModelServerFuture<std::string> future;

  auto metrics_output = metrics_thread_->GetMetricsManager()->GetMetricOutput(metrics::MetricsComponent::QUERY_TRACE);
  bool metrics_in_db =
      metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_AND_DB;
  {
    bool success = false;
    std::unordered_map<int64_t, std::vector<double>> segment_information;
    if (metrics_in_db && task_manager_) {
      // Only get the data corresponding to the closest horizon range
      // TODO(wz2): Do we want to get all the information from the beginning
      segment_information = GetSegmentInformation(ComputeTimestampDataRange(timestamp, true), &success);
    }

    if (segment_information.empty() || !success) {
      // If the segment information is empty, use the file instead on disk
      std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};
      model_server_manager_->TrainForecastModel(models, input_path, forecast_model_save_path_,
                                                workload_forecast_interval_, sequence_length_, horizon_length_,
                                                common::ManagedPointer(&future));
    } else {
      model_server_manager_->TrainForecastModel(models, &segment_information, forecast_model_save_path_,
                                                workload_forecast_interval_, sequence_length_, horizon_length_,
                                                common::ManagedPointer(&future));
    }
  }

  auto future_result = future.WaitFor(FUTURE_TIMEOUT);
  if (!future_result.has_value()) {
    throw PILOT_EXCEPTION("Future timed out.", common::ErrorCode::ERRCODE_IO_ERROR);
  }
}

std::pair<WorkloadMetadata, bool> Pilot::RetrieveWorkloadMetadata(
    std::pair<uint64_t, uint64_t> bounds,
    const std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> &out_metadata,
    const std::unordered_map<execution::query_id_t, std::vector<std::string>> &out_params) {
  // Initialize the workload metadata
  WorkloadMetadata metadata;

  // Lambda function to convert a JSON-serialized param string to a vector of type ids
  auto types_conv = [](const std::string &param_types) {
    std::vector<type::TypeId> types;
    auto json_decomp = nlohmann::json::parse(param_types);
    for (auto &elem : json_decomp) {
      types.push_back(type::TypeUtil::TypeIdFromString(elem));
    }
    return types;
  };

  // Lambda function to convert a JSON-serialized constants to a vector of cexpressions
  auto cves_conv = [](const WorkloadMetadata &metadata, execution::query_id_t qid, const std::string &cve) {
    std::vector<parser::ConstantValueExpression> cves;
    const std::vector<type::TypeId> &types = metadata.query_id_to_param_types_.find(qid)->second;
    auto json_decomp = nlohmann::json::parse(cve);
    for (size_t i = 0; i < json_decomp.size(); i++) {
      cves.emplace_back(parser::ConstantValueExpression::FromString(json_decomp[i], types[i]));
    }
    return cves;
  };

  for (auto &info : out_metadata) {
    metadata.query_id_to_dboid_[info.first] = info.second.db_oid_.UnderlyingValue();
    metadata.query_id_to_text_[info.first] = info.second.text_.substr(1, info.second.text_.size() - 2);
    metadata.query_id_to_param_types_[info.first] = types_conv(info.second.param_type_);
  }

  for (auto &info : out_params) {
    for (auto &cve : info.second) {
      metadata.query_id_to_params_[info.first].emplace_back(cves_conv(metadata, info.first, cve));
    }
  }

  bool result = true;
  {
    common::Future<task::DummyResult> sync;

    // Metadata query
    auto to_row_fn = [&metadata, types_conv](const std::vector<execution::sql::Val *> &values) {
      auto db_oid = static_cast<execution::sql::Integer *>(values[0])->val_;
      auto qid = execution::query_id_t(static_cast<execution::sql::Integer *>(values[1])->val_);

      // Only insert new if not convered already
      if (metadata.query_id_to_dboid_.find(qid) == metadata.query_id_to_dboid_.end()) {
        metadata.query_id_to_dboid_[qid] = db_oid;

        auto *text_val = static_cast<execution::sql::StringVal *>(values[2]);
        // We do this since the string has been quoted by the metric
        metadata.query_id_to_text_[qid] =
            std::string(text_val->StringView().data() + 1, text_val->StringView().size() - 2);

        auto *param_types = static_cast<execution::sql::StringVal *>(values[3]);
        metadata.query_id_to_param_types_[qid] = types_conv(std::string(param_types->StringView()));
      }
    };

    // This loads the entire query text history from the internal tables. It might be possible to
    // do on-demand fetching or windowed fetching at a futrure time. We do this because a interval
    // can execute a prepared query without a corresponding text recording (if the query was
    // already prepared during a prior interval).
    task_manager_->AddTask(std::make_unique<task::TaskDML>(
        catalog::INVALID_DATABASE_OID, "SELECT * FROM noisepage_forecast_texts",
        std::make_unique<optimizer::TrivialCostModel>(), false, to_row_fn, common::ManagedPointer(&sync)));

    auto future_result = sync.WaitFor(FUTURE_TIMEOUT);
    if (!future_result.has_value()) {
      throw PILOT_EXCEPTION("Future timed out.", common::ErrorCode::ERRCODE_IO_ERROR);
    }
    result &= future_result->second;
  }

  {
    common::Future<task::DummyResult> sync;
    auto to_row_fn = [&metadata, cves_conv](const std::vector<execution::sql::Val *> &values) {
      auto qid = execution::query_id_t(static_cast<execution::sql::Integer *>(values[1])->val_);
      auto *param_val = static_cast<execution::sql::StringVal *>(values[2]);
      {
        // Read the parameters. In the worse case, we will have double the parameters, but that is
        // okay since every parameter will be duplicated. This can happen since the parameters
        // could already be visible by the time this select query runs.
        metadata.query_id_to_params_[qid].emplace_back(cves_conv(metadata, qid, std::string(param_val->StringView())));
      }
    };

    auto query = fmt::format("SELECT * FROM noisepage_forecast_parameters WHERE ts >= {} AND ts <= {}", bounds.first,
                             bounds.second);
    task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query,
                                                           std::make_unique<optimizer::TrivialCostModel>(), false,
                                                           to_row_fn, common::ManagedPointer(&sync)));

    auto future_result = sync.WaitFor(FUTURE_TIMEOUT);
    if (!future_result.has_value()) {
      throw PILOT_EXCEPTION("Future timed out.", common::ErrorCode::ERRCODE_IO_ERROR);
    }
    result &= future_result->second;
  }

  return std::make_pair(std::move(metadata), result);
}

std::unordered_map<int64_t, std::vector<double>> Pilot::GetSegmentInformation(std::pair<uint64_t, uint64_t> bounds,
                                                                              bool *success) {
  NOISEPAGE_ASSERT(task_manager_, "GetSegmentInformation() requires task manager");
  uint64_t low_timestamp = bounds.first;
  uint64_t segment_number = 0;
  std::unordered_map<int64_t, std::vector<double>> segments;

  uint64_t interval = workload_forecast_interval_;
  auto to_row_fn = [&segments, &segment_number, &low_timestamp,
                    interval](const std::vector<execution::sql::Val *> &values) {
    // We need to do some postprocessing here on the rows because we want
    // to fully capture empty intervals (i.e., an interval of time where
    // no query at all has been executed).
    auto ts = static_cast<execution::sql::Integer *>(values[0])->val_;
    auto qid = static_cast<execution::sql::Integer *>(values[1])->val_;
    auto seen = static_cast<execution::sql::Real *>(values[2])->val_;

    // Compute the correct segment the data belongs to
    uint64_t segment_idx = (ts - low_timestamp) / interval;
    segment_number = std::max(segment_number, segment_idx);
    segments[qid].resize(segment_number + 1);
    segments[qid][segment_number] = seen;
  };

  // This will give us the history of seen frequencies.
  auto query = fmt::format("SELECT * FROM noisepage_forecast_frequencies WHERE ts >= {} AND ts <= {} ORDER BY ts",
                           bounds.first, bounds.second);

  common::Future<task::DummyResult> sync;
  task_manager_->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query,
                                                         std::make_unique<optimizer::TrivialCostModel>(), false,
                                                         to_row_fn, common::ManagedPointer(&sync)));

  auto future_result = sync.WaitFor(FUTURE_TIMEOUT);
  if (!future_result.has_value()) {
    throw PILOT_EXCEPTION("Future timed out.", common::ErrorCode::ERRCODE_IO_ERROR);
  }
  *success = future_result->second;

  NOISEPAGE_ASSERT(segment_number <= (((bounds.second - bounds.first) / interval) + 1),
                   "Incorrect data retrieved from internal tables");

  // Pad each segment to the number of segments needed for forecast model.
  segment_number = ((bounds.second - bounds.first) / interval) + 1;
  for (auto &seg : segments) {
    seg.second.resize(segment_number);
  }

  return segments;
}

void Pilot::RecordWorkloadForecastPrediction(uint64_t timestamp,
                                             const selfdriving::WorkloadForecastPrediction &prediction,
                                             const WorkloadMetadata &metadata) {
  if (task_manager_ == nullptr) {
    return;
  }

  util::ForecastRecordingUtil::RecordForecastClusters(timestamp, metadata, prediction, task_manager_);
  util::ForecastRecordingUtil::RecordForecastQueryFrequencies(timestamp, metadata, prediction, task_manager_);
}

void Pilot::LoadWorkloadForecast(WorkloadForecastInitMode mode) {
  // Metrics thread is suspended at this point
  bool infer_from_internal = mode == WorkloadForecastInitMode::INTERNAL_TABLES_WITH_INFERENCE;
  bool infer_from_disk = mode == WorkloadForecastInitMode::DISK_WITH_INFERENCE;
  metrics_thread_->GetMetricsManager()->Aggregate();
  metrics_thread_->GetMetricsManager()->ToOutput(task_manager_);

  // Get the current timestamp
  uint64_t timestamp = metrics::MetricsUtil::Now();

  std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> out_metadata;
  std::unordered_map<execution::query_id_t, std::vector<std::string>> out_params;
  if (infer_from_internal) {
    auto raw = reinterpret_cast<metrics::QueryTraceMetricRawData *>(
        metrics_thread_->GetMetricsManager()
            ->AggregatedMetrics()
            .at(static_cast<uint8_t>(metrics::MetricsComponent::QUERY_TRACE))
            .get());
    if (raw != nullptr) {
      // Perform a flush to database. This will also get any temporary data.
      // This is also used to flush all parameter information at a forecast interval.
      raw->WriteToDB(task_manager_, true, timestamp, &out_metadata, &out_params);

      // We don't have to worry about flushing the tasks submitted by WriteToDB.
      // The query metadata and parameters that would have been flushed out
      // have already been captured by out_metadata and out_params.
      //
      // Under the assumption that the task manager is not backlogged by tasks
      // submitted by QueryTraceMetricRawData to write frequency information,
      // the frequency information we pull from the tables should also be
      // reasonably up to date.
    }
  }

  if (infer_from_internal) {
    bool success = false;
    std::vector<std::string> models{"LSTM"};
    std::unordered_map<int64_t, std::vector<double>> segment_information;
    std::pair<selfdriving::WorkloadForecastPrediction, bool> result;
    if (task_manager_) {
      // Only pull the segment information if inference from internal tables
      segment_information = GetSegmentInformation(ComputeTimestampDataRange(timestamp, false), &success);
    }

    if (!success || segment_information.empty()) {
      SELFDRIVING_LOG_WARN("Trying to perform inference from internal tables that are empty");
      return;
    }

    result = model_server_manager_->InferForecastModel(&segment_information, forecast_model_save_path_, models, nullptr,
                                                       workload_forecast_interval_, sequence_length_, horizon_length_);
    if (!result.second) {
      SELFDRIVING_LOG_ERROR("Forecast model inference failed");
      return;
    }

    // Retrieve query information from internal tables
    auto metadata_result =
        RetrieveWorkloadMetadata(ComputeTimestampDataRange(timestamp, false), out_metadata, out_params);
    if (!metadata_result.second) {
      SELFDRIVING_LOG_ERROR("Failed to read from internal trace metadata tables");
      metrics_thread_->ResumeMetrics();
      return;
    }

    // Record forecast into internal tables
    RecordWorkloadForecastPrediction(timestamp, result.first, metadata_result.first);

    // Construct workload forecast
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(result.first, std::move(metadata_result.first));
  } else if (infer_from_disk) {
    std::vector<std::string> models{"LSTM"};
    std::pair<selfdriving::WorkloadForecastPrediction, bool> result;

    // Pull the information from disk if segment information
    // If the segment information is empty, use the file instead on disk
    std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};

    result = model_server_manager_->InferForecastModel(input_path, forecast_model_save_path_, models, nullptr,
                                                       workload_forecast_interval_, sequence_length_, horizon_length_);

    // Since we reading from an on-disk file, these results are not
    // loaded into internal tables (otherwise, we'd have to load the
    // contents of query_trace.csv and query_text.csv into tables too).
    if (!result.second) {
      SELFDRIVING_LOG_ERROR("Forecast model inference failed");
      return;
    }

    // Construct the WorkloadForecast froM a mix of on-disk and inference information
    auto sample = settings_manager_->GetInt(settings::Param::forecast_sample_limit);
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(result.first, workload_forecast_interval_, sample);
  } else {
    NOISEPAGE_ASSERT(mode == WorkloadForecastInitMode::DISK_ONLY, "Expected the mode to be directly from disk");

    // Load the WorkloadForecast directly from disk without using the model
    auto sample = settings_manager_->GetInt(settings::Param::forecast_sample_limit);
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(workload_forecast_interval_, sample);
  }
}

void Pilot::PerformPlanning() {
  // Suspend the metrics thread while we are handling the data (snapshot).
  metrics_thread_->PauseMetrics();

  // Populate the workload forecast
  auto metrics_output = metrics_thread_->GetMetricsManager()->GetMetricOutput(metrics::MetricsComponent::QUERY_TRACE);
  bool metrics_in_db =
      metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_AND_DB;
  LoadWorkloadForecast(metrics_in_db ? WorkloadForecastInitMode::INTERNAL_TABLES_WITH_INFERENCE
                                     : WorkloadForecastInitMode::DISK_WITH_INFERENCE);
  if (forecast_ == nullptr) {
    SELFDRIVING_LOG_ERROR("Unable to initialize the WorkloadForecast information");
    metrics_thread_->ResumeMetrics();
    return;
  }

  // Perform planning
  std::vector<std::pair<const std::string, catalog::db_oid_t>> best_action_seq;
  Pilot::ActionSearch(&best_action_seq);

  metrics_thread_->ResumeMetrics();
}

void Pilot::ActionSearch(std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq) {
  auto num_segs = forecast_->GetNumberOfSegments();
  auto end_segment_index = std::min(action_planning_horizon_ - 1, num_segs - 1);

  auto mcst =
      pilot::MonteCarloTreeSearch(common::ManagedPointer(this), common::ManagedPointer(forecast_), end_segment_index);
  mcst.BestAction(simulation_number_, best_action_seq,
                  settings_manager_->GetInt64(settings::Param::pilot_memory_constraint));
  for (uint64_t i = 0; i < best_action_seq->size(); i++) {
    SELFDRIVING_LOG_INFO(fmt::format("Action Selected: Time Interval: {}; Action Command: {} Applied to Database {}", i,
                                     best_action_seq->at(i).first,
                                     static_cast<uint32_t>(best_action_seq->at(i).second)));
  }
  PilotUtil::ApplyAction(common::ManagedPointer(this), best_action_seq->begin()->first,
                         best_action_seq->begin()->second, false);
}

void Pilot::ExecuteForecast(uint64_t start_segment_index, uint64_t end_segment_index,
                            std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
                            std::map<uint32_t, uint64_t> *segment_to_offset,
                            std::vector<std::vector<double>> *interference_result_matrix) {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
  // first we make sure the pipeline metrics flag as well as the counters is enabled. Also set the sample rate to be 0
  // so that every query execution is being recorded

  std::vector<execution::query_id_t> pipeline_qids;
  // Collect pipeline metrics of forecasted queries within the interval of segments
  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_), start_segment_index,
                                                          end_segment_index, &pipeline_qids, !WHAT_IF);

  // pipeline_to_prediction maps each pipeline to a vector of ou inference results for all ous of this pipeline
  // (where each entry corresponds to a different query param)
  // Each element of the outermost vector is a vector of ou prediction (each being a double vector) for one set of
  // parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction;

  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::OUModelInference(ou_model_save_path_, model_server_manager_, pipeline_qids, pipeline_data->pipeline_data_,
                              &pipeline_to_prediction);

  PilotUtil::InterferenceModelInference(interference_model_save_path_, model_server_manager_, pipeline_to_prediction,
                                        common::ManagedPointer(forecast_), start_segment_index, end_segment_index,
                                        query_info, segment_to_offset, interference_result_matrix);
}

}  // namespace noisepage::selfdriving
