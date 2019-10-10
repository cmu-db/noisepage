#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "main/db_main.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "type/transient_value_factory.h"

#define __SETTING_GFLAGS_DECLARE__     // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DECLARE__      // NOLINT

namespace terrier::settings {

using ValueFactory = type::TransientValueFactory;
using ValuePeeker = type::TransientValuePeeker;
using ActionContext = common::ActionContext;
using ActionState = common::ActionState;

SettingsManager::SettingsManager(DBMain *db) : db_(db) { ValidateParams(); }

void SettingsManager::ValidateParams() {
  // This will expand to invoke settings_manager::DefineSetting on
  // all of the settings defined in settings.h.
  // Example:
  //   ValidateSetting(Param::port, type::TransientValueFactory::GetInteger(1024),
  //                  type::TransientValueFactory::GetInteger(65535));

#define __SETTING_VALIDATE__           // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_VALIDATE__            // NOLINT
}

void SettingsManager::ValidateSetting(Param param, const type::TransientValue &min_value,
                                      const type::TransientValue &max_value) {
  const ParamInfo &info = db_->param_map_.find(param)->second;
  if (!ValidateValue(info.value_, min_value, max_value)) {
    SETTINGS_LOG_ERROR(
        "Value given for \"{}"
        "\" is not in its min-max bounds",
        info.name_);
    throw SETTINGS_EXCEPTION("Invalid setting value");
  }
}

int32_t SettingsManager::GetInt(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return ValuePeeker::PeekInteger(GetValue(param));
}

double SettingsManager::GetDouble(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return ValuePeeker::PeekDecimal(GetValue(param));
}

bool SettingsManager::GetBool(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return ValuePeeker::PeekBoolean(GetValue(param));
}

std::string SettingsManager::GetString(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return std::string(ValuePeeker::PeekVarChar(GetValue(param)));
}

void SettingsManager::SetInt(Param param, int32_t value, const std::shared_ptr<ActionContext> &action_context,
                             setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = db_->param_map_.find(param)->second;
  auto min_value = static_cast<const int>(param_info.min_value_);
  auto max_value = static_cast<const int>(param_info.max_value_);

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  if (!(value >= min_value && value <= max_value)) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    int old_value = ValuePeeker::PeekInteger(GetValue(param));
    if (!SetValue(param, ValueFactory::GetInteger(value))) {
      action_context->SetState(ActionState::FAILURE);
    } else {
      ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
      if (action_state == ActionState::FAILURE) {
        bool result = SetValue(param, ValueFactory::GetInteger(old_value));
        if (!result) {
          SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
          throw SETTINGS_EXCEPTION("Failed to reset parameter");
        }
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetDouble(Param param, double value, const std::shared_ptr<ActionContext> &action_context,
                                setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = db_->param_map_.find(param)->second;
  auto min_value = static_cast<const int>(param_info.min_value_);
  auto max_value = static_cast<const int>(param_info.max_value_);

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  if (!(value >= min_value && value <= max_value)) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    double old_value = ValuePeeker::PeekDecimal(GetValue(param));
    if (!SetValue(param, ValueFactory::GetDecimal(value))) {
      action_context->SetState(ActionState::FAILURE);
    } else {
      ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
      if (action_state == ActionState::FAILURE) {
        bool result = SetValue(param, ValueFactory::GetDecimal(old_value));
        if (!result) {
          SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
          throw SETTINGS_EXCEPTION("Failed to reset parameter");
        }
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetBool(Param param, bool value, const std::shared_ptr<ActionContext> &action_context,
                              setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = db_->param_map_.find(param)->second;

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  bool old_value = ValuePeeker::PeekBoolean(GetValue(param));
  if (!SetValue(param, ValueFactory::GetBoolean(value))) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
    if (action_state == ActionState::FAILURE) {
      bool result = SetValue(param, ValueFactory::GetBoolean(old_value));
      if (!result) {
        SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
        throw SETTINGS_EXCEPTION("Failed to reset parameter");
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetString(Param param, const std::string_view &value,
                                const std::shared_ptr<ActionContext> &action_context,
                                setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = db_->param_map_.find(param)->second;

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  std::string_view old_value = ValuePeeker::PeekVarChar(GetValue(param));
  if (!SetValue(param, ValueFactory::GetVarChar(value))) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    std::string_view new_value(value);
    ActionState action_state = InvokeCallback(param, &old_value, &new_value, action_context);
    if (action_state == ActionState::FAILURE) {
      bool result = SetValue(param, ValueFactory::GetVarChar(old_value));
      if (!result) {
        SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
        throw SETTINGS_EXCEPTION("Failed to reset parameter");
      }
    }
  }
  setter_callback(action_context);
}

type::TransientValue &SettingsManager::GetValue(Param param) {
  auto &param_info = db_->param_map_.find(param)->second;
  return param_info.value_;
}

bool SettingsManager::SetValue(Param param, type::TransientValue value) {
  auto &param_info = db_->param_map_.find(param)->second;

  if (!param_info.is_mutable_) return false;

  param_info.value_ = std::move(value);
  return true;
}

bool SettingsManager::ValidateValue(const type::TransientValue &value, const type::TransientValue &min_value,
                                    const type::TransientValue &max_value) {
  switch (value.Type()) {
    case type::TypeId::INTEGER:
      return ValuePeeker::PeekInteger(value) >= ValuePeeker::PeekInteger(min_value) &&
             ValuePeeker::PeekInteger(value) <= ValuePeeker::PeekInteger(max_value);
    case type::TypeId ::DECIMAL:
      return ValuePeeker::PeekDecimal(value) >= ValuePeeker::PeekDecimal(min_value) &&
             ValuePeeker::PeekDecimal(value) <= ValuePeeker::PeekDecimal(max_value);
    default:
      return true;
  }
}

common::ActionState SettingsManager::InvokeCallback(Param param, void *old_value, void *new_value,
                                                    const std::shared_ptr<common::ActionContext> &action_context) {
  callback_fn callback = db_->param_map_.find(param)->second.callback_;
  (callback)(old_value, new_value, db_, action_context);
  ActionState action_state = action_context->GetState();
  TERRIER_ASSERT(action_state == ActionState::FAILURE || action_state == ActionState::SUCCESS,
                 "action context should have state of either SUCCESS or FAILURE on completion.");
  return action_state;
}

void SettingsManager::ConstructParamMap(                                                      // NOLINT
    std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> &param_map) {  // NOLINT
  /*
   * Populate gflag values to param map.
   * This will expand to a list of code like:
   * param_map.emplace(
   *     terrier::settings::Param::port,
   *     terrier::settings::ParamInfo(port, terrier::type::TransientValueFactory::GetInteger(FLAGS_port),
   *                                  "Terrier port (default: 15721)",
   *                                  terrier::type::TransientValueFactory::GetInteger(15721), is_mutable));
   */

#define __SETTING_POPULATE__           // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_POPULATE__            // NOLINT
}

}  // namespace terrier::settings
