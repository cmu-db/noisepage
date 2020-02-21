#include "settings/settings_callbacks.h"

#include <memory>

#include "main/db_main.h"

namespace terrier::settings {

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

void Callbacks::NumLogManagerBuffers(void *const old_value, void *const new_value, DBMain *const db_main,
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
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING, 0);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::LOGGING);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsTransaction(void *const old_value, void *const new_value, DBMain *const db_main,
                                   common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION, 0);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::TRANSACTION);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsPipeline(void *const old_value, void *const new_value, DBMain *const db_main,
                                common::ManagedPointer<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);
  else
    db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
  action_context->SetState(common::ActionState::SUCCESS);
}

}  // namespace terrier::settings
