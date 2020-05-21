#include "settings/settings_manager.h"

#include <gflags/gflags.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "main/db_main.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_callbacks.h"

#define __SETTING_GFLAGS_DECLARE__     // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DECLARE__      // NOLINT

namespace terrier::settings {

using ActionContext = common::ActionContext;
using ActionState = common::ActionState;

SettingsManager::SettingsManager(const common::ManagedPointer<DBMain> db_main,
                                 std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : db_main_(db_main), param_map_(std::move(param_map)) {
  ValidateParams();
}

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

void SettingsManager::ValidateSetting(Param param, const parser::ConstantValueExpression &min_value,
                                      const parser::ConstantValueExpression &max_value) {
  const ParamInfo &info = param_map_.find(param)->second;
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
  return GetValue(param).GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
}

int64_t SettingsManager::GetInt64(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return GetValue(param).GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
}

double SettingsManager::GetDouble(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return GetValue(param).GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
}

bool SettingsManager::GetBool(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return GetValue(param).GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
}

std::string SettingsManager::GetString(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  return std::string(GetValue(param).GetValue().CastManagedPointerTo<execution::sql::StringVal>()->StringView());
}

void SettingsManager::SetInt(Param param, int32_t value, common::ManagedPointer<ActionContext> action_context,
                             setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = param_map_.find(param)->second;
  const auto min_value = static_cast<int32_t>(param_info.min_value_);
  const auto max_value = static_cast<int32_t>(param_info.max_value_);

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  if (!(value >= min_value && value <= max_value)) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    int32_t old_value = GetValue(param).GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
    if (!SetValue(param, {type::TypeId::INTEGER, std::make_unique<execution::sql::Integer>(value)})) {
      action_context->SetState(ActionState::FAILURE);
    } else {
      ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
      if (action_state == ActionState::FAILURE) {
        const bool result =
            SetValue(param, {type::TypeId::INTEGER, std::make_unique<execution::sql::Integer>(old_value)});
        if (!result) {
          SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
          throw SETTINGS_EXCEPTION("Failed to reset parameter");
        }
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetInt64(Param param, int64_t value, common::ManagedPointer<ActionContext> action_context,
                               setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = param_map_.find(param)->second;
  const auto min_value = static_cast<const int64_t>(param_info.min_value_);
  const auto max_value = static_cast<const int64_t>(param_info.max_value_);

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  if (!(value >= min_value && value <= max_value)) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    int64_t old_value = GetValue(param).GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
    if (!SetValue(param, {type::TypeId::BIGINT, std::make_unique<execution::sql::Integer>(value)})) {
      action_context->SetState(ActionState::FAILURE);
    } else {
      ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
      if (action_state == ActionState::FAILURE) {
        const bool result =
            SetValue(param, {type::TypeId::BIGINT, std::make_unique<execution::sql::Integer>(old_value)});
        if (!result) {
          SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
          throw SETTINGS_EXCEPTION("Failed to reset parameter");
        }
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetDouble(Param param, double value, common::ManagedPointer<ActionContext> action_context,
                                setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = param_map_.find(param)->second;
  const auto min_value = static_cast<const int>(param_info.min_value_);
  const auto max_value = static_cast<const int>(param_info.max_value_);

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  if (!(value >= min_value && value <= max_value)) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    double old_value = GetValue(param).GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
    if (!SetValue(param, {type::TypeId::DECIMAL, std::make_unique<execution::sql::Real>(value)})) {
      action_context->SetState(ActionState::FAILURE);
    } else {
      ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
      if (action_state == ActionState::FAILURE) {
        const bool result = SetValue(param, {type::TypeId::DECIMAL, std::make_unique<execution::sql::Real>(old_value)});
        if (!result) {
          SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
          throw SETTINGS_EXCEPTION("Failed to reset parameter");
        }
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetBool(Param param, bool value, common::ManagedPointer<ActionContext> action_context,
                              setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = param_map_.find(param)->second;

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  bool old_value = GetValue(param).GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
  if (!SetValue(param, {type::TypeId::BOOLEAN, std::make_unique<execution::sql::BoolVal>(value)})) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);
    if (action_state == ActionState::FAILURE) {
      const bool result =
          SetValue(param, {type::TypeId::BOOLEAN, std::make_unique<execution::sql::BoolVal>(old_value)});
      if (!result) {
        SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
        throw SETTINGS_EXCEPTION("Failed to reset parameter");
      }
    }
  }
  setter_callback(action_context);
}

void SettingsManager::SetString(Param param, const std::string_view &value,
                                common::ManagedPointer<ActionContext> action_context,
                                setter_callback_fn setter_callback) {
  // The ActionContext state must be set to INITIATED to prevent
  // somebody from reusing it for multiple invocations
  if (action_context->GetState() != ActionState::INITIATED) {
    SETTINGS_LOG_ERROR("ActionContext state is not set to INITIATED");
    throw SETTINGS_EXCEPTION("Invalid ActionContext state");
  }

  const auto &param_info = param_map_.find(param)->second;

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  auto old_cve = std::unique_ptr<parser::ConstantValueExpression>{
      reinterpret_cast<parser::ConstantValueExpression *>(GetValue(param).Copy().release())};

  auto string_val = execution::sql::ValueUtil::CreateStringVal(value);

  if (!SetValue(param, {type::TypeId::VARCHAR, std::move(string_val.first), std::move(string_val.second)})) {
    action_context->SetState(ActionState::FAILURE);
  } else {
    std::string_view new_value(value);
    auto old_value = old_cve->GetValue().CastManagedPointerTo<execution::sql::StringVal>()->StringView();
    ActionState action_state = InvokeCallback(param, &old_value, &new_value, action_context);
    if (action_state == ActionState::FAILURE) {
      const bool result = SetValue(param, *old_cve);
      if (!result) {
        SETTINGS_LOG_ERROR("Failed to revert parameter \"{}\"", param_info.name_);
        throw SETTINGS_EXCEPTION("Failed to reset parameter");
      }
    }
  }
  setter_callback(action_context);
}

parser::ConstantValueExpression &SettingsManager::GetValue(Param param) {
  auto &param_info = param_map_.find(param)->second;
  return param_info.value_;
}

bool SettingsManager::SetValue(Param param, parser::ConstantValueExpression value) {
  auto &param_info = param_map_.find(param)->second;

  if (!param_info.is_mutable_) return false;

  param_info.value_ = std::move(value);
  return true;
}

bool SettingsManager::ValidateValue(const parser::ConstantValueExpression &value,
                                    const parser::ConstantValueExpression &min_value,
                                    const parser::ConstantValueExpression &max_value) {
  switch (value.GetReturnValueType()) {
    case type::TypeId::INTEGER:
      return value.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_ >=
                 min_value.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_ &&
             value.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_ <=
                 max_value.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
    case type::TypeId ::DECIMAL:
      return value.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_ >=
                 min_value.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_ &&
             value.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_ <=
                 max_value.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
    default:
      return true;
  }
}

common::ActionState SettingsManager::InvokeCallback(Param param, void *old_value, void *new_value,
                                                    common::ManagedPointer<common::ActionContext> action_context) {
  callback_fn callback = param_map_.find(param)->second.callback_;
  (callback)(old_value, new_value, db_main_.Get(), action_context);
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
