#include "settings/settings_callbacks.h"
#include <memory>
#include "main/db_main.h"

namespace terrier::settings {

void Callbacks::NoOp(void *old_value, void *new_value, DBMain *const db_main,
                     const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::BufferSegmentPoolSizeLimit(void *const old_value, void *const new_value, DBMain *const db_main,
                                           const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_size = *static_cast<int *>(new_value);
  bool success = db_main->buffer_segment_pool_->SetSizeLimit(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

void Callbacks::BufferSegmentPoolReuseLimit(void *const old_value, void *const new_value, DBMain *const db_main,
                                            const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_reuse = *static_cast<int *>(new_value);
  db_main->buffer_segment_pool_->SetReuseLimit(new_reuse);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::WorkerPoolThreads(void *const old_value, void *const new_value, DBMain *const db_main,
                                  const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int num_threads = *static_cast<int *>(new_value);
  db_main->thread_pool_->SetNumWorkers(num_threads);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::NumLogManagerBuffers(void *const old_value, void *const new_value, DBMain *const db_main,
                                     const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_size = *static_cast<int *>(new_value);
  bool success = db_main->log_manager_->SetNumBuffers(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

void Callbacks::MetricsTransaction(void *const old_value, void *const new_value, DBMain *const db_main,
                                   const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->metrics_manager_->EnableMetric(metrics::MetricsComponent::TRANSACTION);
  else
    db_main->metrics_manager_->DisableMetric(metrics::MetricsComponent::TRANSACTION);
  action_context->SetState(common::ActionState::SUCCESS);
}

void Callbacks::MetricsLogging(void *const old_value, void *const new_value, DBMain *const db_main,
                               const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  bool new_status = *static_cast<bool *>(new_value);
  if (new_status)
    db_main->metrics_manager_->EnableMetric(metrics::MetricsComponent::LOGGING);
  else
    db_main->metrics_manager_->DisableMetric(metrics::MetricsComponent::LOGGING);
  action_context->SetState(common::ActionState::SUCCESS);
}

}  // namespace terrier::settings
