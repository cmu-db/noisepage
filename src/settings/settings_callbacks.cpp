#include "settings/settings_callbacks.h"

#include <memory>

#include "loggers/loggers_util.h"
#include "main/db_main.h"

namespace noisepage::settings {

void Callbacks::NoOp(void *old_value, void *new_value, DBMain *const db_main,
                     common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::BufferSegmentPoolSizeLimit(void *const old_value, void *const new_value, DBMain *const db_main,
                                           common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_size = *static_cast<int *>(new_value);
  bool success = db_main->GetBufferSegmentPool()->SetSizeLimit(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

void Callbacks::BufferSegmentPoolReuseLimit(void *const old_value, void *const new_value, DBMain *const db_main,
                                            common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_reuse = *static_cast<int *>(new_value);
  db_main->GetBufferSegmentPool()->SetReuseLimit(new_reuse);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::BlockStoreSizeLimit(void *const old_value, void *const new_value, DBMain *const db_main,
                                    common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int64_t new_size = *static_cast<int64_t *>(new_value);
  bool success = db_main->GetStorageLayer()->GetBlockStore()->SetSizeLimit(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

void Callbacks::BlockStoreReuseLimit(void *const old_value, void *const new_value, DBMain *const db_main,
                                     common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int64_t new_reuse = *static_cast<int64_t *>(new_value);
  db_main->GetStorageLayer()->GetBlockStore()->SetReuseLimit(new_reuse);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::WalNumBuffers(void *const old_value, void *const new_value, DBMain *const db_main,
                              common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_size = *static_cast<int *>(new_value);
  bool success = db_main->GetLogManager()->SetNumBuffers(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

void Callbacks::WalSerializationInterval(void *const old_value, void *const new_value, DBMain *const db_main,
                                         common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_interval = *static_cast<int *>(new_value);
  db_main->GetLogManager()->SetSerializationInterval(new_interval);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsLogging(void *const old_value, void *const new_value, DBMain *const db_main,
                               common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::LOGGING);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsTransaction(void *const old_value, void *const new_value, DBMain *const db_main,
                                   common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::TRANSACTION);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsGC(void *const old_value, void *const new_value, DBMain *const db_main,
                          common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::GARBAGECOLLECTION);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::GARBAGECOLLECTION);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsExecution(void *const old_value, void *const new_value, DBMain *const db_main,
                                 common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::EXECUTION);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsPipeline(void *const old_value, void *const new_value, DBMain *const db_main,
                                common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsPipelineSampleRate(void *old_value, void *new_value, DBMain *db_main,
                                          common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int interval = *static_cast<int *>(new_value);
  db_main->GetMetricsManager()->SetMetricSampleRate(metrics::MetricsComponent::EXECUTION_PIPELINE,
                                                    static_cast<uint8_t>(interval));
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsLoggingSampleRate(void *old_value, void *new_value, DBMain *db_main,
                                         common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int interval = *static_cast<int *>(new_value);
  db_main->GetMetricsManager()->SetMetricSampleRate(metrics::MetricsComponent::LOGGING, static_cast<uint8_t>(interval));
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsBindCommand(void *const old_value, void *const new_value, DBMain *const db_main,
                                   common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::BIND_COMMAND);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::BIND_COMMAND);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsExecuteCommand(void *const old_value, void *const new_value, DBMain *const db_main,
                                      common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTE_COMMAND);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::EXECUTE_COMMAND);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsQueryTrace(void *const old_value, void *const new_value, DBMain *const db_main,
                                  common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::QUERY_TRACE);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::QUERY_TRACE);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsQueryTraceOutput(void *const old_value, void *const new_value, DBMain *const db_main,
                                        common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  auto metrics_output = metrics::MetricsUtil::FromMetricsOutputString(*static_cast<std::string_view *>(new_value));
  if (metrics_output == std::nullopt) {
    action_context->SetState(common::ActionState::FAILURE);
    return;
  }
  db_main->GetMetricsManager()->SetMetricOutput(metrics::MetricsComponent::QUERY_TRACE, *metrics_output);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::CompiledQueryExecution(void *const old_value, void *const new_value, DBMain *const db_main,
                                       common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool is_compiled = *static_cast<bool *>(new_value);
  db_main->GetTrafficCop()->SetExecutionMode(is_compiled);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::ForecastSampleLimit(void *old_value, void *new_value, DBMain *db_main,
                                    common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int forecast_sample_limit = *static_cast<int *>(new_value);
  metrics::QueryTraceMetricRawData::query_param_sample = static_cast<uint64_t>(forecast_sample_limit);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::TaskPoolSize(void *old_value, void *new_value, DBMain *db_main,
                             common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int task_size = *static_cast<int *>(new_value);
  db_main->GetTaskManager()->SetTaskPoolSize(task_size);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::PilotEnablePlanning(void *const old_value, void *const new_value, DBMain *const db_main,
                                    common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetPilotThread()->EnablePilot();
  else
    db_main->GetPilotThread()->DisablePilot();
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::TrainForecastModel(void *old_value, void *new_value, DBMain *db_main,
                                   common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  db_main->GetPilot()->PerformForecasterTrain();
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::TrainInterferenceModel(void *old_value, void *new_value, DBMain *db_main,
                                       common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);

  auto settings = db_main->GetSettingsManager();
  auto ms_manager = db_main->GetModelServerManager();

  auto ou_model_save = std::string(settings->GetValue(settings::Param::ou_model_save_path).Peek<std::string_view>());
  auto interference_model_save =
      std::string(settings->GetValue(settings::Param::interference_model_save_path).Peek<std::string_view>());
  auto input_path =
      std::string(settings->GetValue(settings::Param::interference_model_input_path).Peek<std::string_view>());
  auto sample_rate = settings->GetValue(settings::Param::interference_model_pipeline_sample_rate).Peek<int>();

  std::vector<std::string> methods;
  {
    auto method_str =
        std::string(settings->GetValue(settings::Param::interference_model_train_methods).Peek<std::string_view>());
    std::stringstream ss(method_str);
    while (ss.good()) {
      std::string token;
      std::getline(ss, token, ',');
      methods.emplace_back(token);
    }
  }

  modelserver::ModelServerFuture<std::string> future;
  ms_manager->TrainInterferenceModel(methods, input_path, interference_model_save, ou_model_save, sample_rate,
                                     common::ManagedPointer<modelserver::ModelServerFuture<std::string>>(&future));

  auto timeout = settings->GetValue(settings::Param::interference_model_train_timeout).Peek<int>();
  auto result = future.WaitFor(std::chrono::milliseconds(timeout));
  if (!result.has_value()) {
    SETTINGS_LOG_ERROR("Callbacks::TrainInterferenceModel timed out {} milliseconds", timeout);
    action_context->SetState(common::ActionState::FAILURE);
  } else if (!result.value().second) {
    SETTINGS_LOG_ERROR("Callbacks::TrainInterferenceModel failed with error {}", future.FailMessage());
    action_context->SetState(common::ActionState::FAILURE);
  } else {
    action_context->SetState(common::ActionState::SUCCESS);
  }
}

void Callbacks::TrainOUModel(void *old_value, void *new_value, DBMain *db_main,
                             common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);

  auto settings = db_main->GetSettingsManager();
  auto ms_manager = db_main->GetModelServerManager();

  auto ou_model_save = std::string(settings->GetValue(settings::Param::ou_model_save_path).Peek<std::string_view>());
  auto input_path = std::string(settings->GetValue(settings::Param::ou_model_input_path).Peek<std::string_view>());

  std::vector<std::string> methods;
  {
    auto method_str = std::string(settings->GetValue(settings::Param::ou_model_train_methods).Peek<std::string_view>());
    std::stringstream ss(method_str);
    while (ss.good()) {
      std::string token;
      std::getline(ss, token, ',');
      methods.emplace_back(token);
    }
  }

  modelserver::ModelServerFuture<std::string> future;
  ms_manager->TrainModel(modelserver::ModelType::Type::OperatingUnit, methods, &input_path, ou_model_save, nullptr,
                         common::ManagedPointer<modelserver::ModelServerFuture<std::string>>(&future));

  auto timeout = settings->GetValue(settings::Param::ou_model_train_timeout).Peek<int>();
  auto result = future.WaitFor(std::chrono::milliseconds(timeout));
  if (!result.has_value()) {
    SETTINGS_LOG_ERROR("Callbacks::TrainOUModel timed out {} milliseconds", timeout);
    action_context->SetState(common::ActionState::FAILURE);
  } else if (!result.value().second) {
    SETTINGS_LOG_ERROR("Callbacks::TrainOUModel failed with error {}", future.FailMessage());
    action_context->SetState(common::ActionState::FAILURE);
  } else {
    action_context->SetState(common::ActionState::SUCCESS);
  }
}

#define SETTINGS_GENERATE_LOGGER_CALLBACK(component)                                                     \
  void Callbacks::LogLevelSet##component(void *old_value, void *new_value, DBMain *db_main,              \
                                         common::ManagedPointer<common::ActionContext> action_context) { \
    action_context->SetState(common::ActionState::IN_PROGRESS);                                          \
    auto level = *static_cast<std::string_view *>(new_value);                                            \
    auto level_val = LoggersUtil::GetLevel(level);                                                       \
    auto logger = LoggersUtil::GetLogger(#component);                                                    \
                                                                                                         \
    if (!level_val.has_value() || logger == nullptr) {                                                   \
      action_context->SetState(common::ActionState::FAILURE);                                            \
      return;                                                                                            \
    }                                                                                                    \
                                                                                                         \
    logger->set_level(*level_val);                                                                       \
    action_context->SetState(common::ActionState::SUCCESS);                                              \
  }

SETTINGS_GENERATE_LOGGER_CALLBACK(binder)
SETTINGS_GENERATE_LOGGER_CALLBACK(catalog)
SETTINGS_GENERATE_LOGGER_CALLBACK(common)
SETTINGS_GENERATE_LOGGER_CALLBACK(execution)
SETTINGS_GENERATE_LOGGER_CALLBACK(index)
SETTINGS_GENERATE_LOGGER_CALLBACK(messenger)
SETTINGS_GENERATE_LOGGER_CALLBACK(metrics)
SETTINGS_GENERATE_LOGGER_CALLBACK(modelserver)
SETTINGS_GENERATE_LOGGER_CALLBACK(network)
SETTINGS_GENERATE_LOGGER_CALLBACK(optimizer)
SETTINGS_GENERATE_LOGGER_CALLBACK(parser)
SETTINGS_GENERATE_LOGGER_CALLBACK(replication)
SETTINGS_GENERATE_LOGGER_CALLBACK(selfdriving)
SETTINGS_GENERATE_LOGGER_CALLBACK(settings)
SETTINGS_GENERATE_LOGGER_CALLBACK(storage)
SETTINGS_GENERATE_LOGGER_CALLBACK(transaction)

#undef SETTINGS_GENERATE_LOGGER_CALLBACK

}  // namespace noisepage::settings
