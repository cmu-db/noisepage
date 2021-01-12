#include "settings/settings_callbacks.h"

#include <memory>

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

void Callbacks::MetricsPipelineSamplingInterval(void *old_value, void *new_value, DBMain *db_main,
                                                common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int interval = *static_cast<int *>(new_value);
  db_main->GetMetricsManager()->SetMetricSampleInterval(metrics::MetricsComponent::EXECUTION_PIPELINE, interval);
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

}  // namespace noisepage::settings
