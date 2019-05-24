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
  bool success = db_main->txn_manager_->SetBufferPoolSizeLimit(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

}  // namespace terrier::settings
