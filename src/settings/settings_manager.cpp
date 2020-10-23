#include "settings/settings_manager.h"

#include <algorithm>

#include "binder/binder_util.h"
#include "common/macros.h"
#include "execution/sql/value_util.h"
#include "gflags/gflags.h"
#include "main/db_main.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_callbacks.h"

#define __SETTING_GFLAGS_DECLARE__   // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DECLARE__    // NOLINT

namespace noisepage::settings {

using ActionContext = common::ActionContext;
using ActionState = common::ActionState;

namespace {

/** Check the action context state for setting, throwing an exception if it is invalid. */
void CheckActionContextStateForSet(common::ManagedPointer<common::ActionContext> action_context) {
  // The ActionContext state must be INITIATED to prevent somebody from reusing it for multiple invocations.
  if (action_context->GetState() != ActionState::INITIATED) {
    // TODO(WAN): I don't know what the right ErrorCode is here.
    throw SETTINGS_EXCEPTION("Invalid ActionContext state, should be ActionState::INITIATED.",
                             common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

}  // namespace

SettingsManager::SettingsManager(const common::ManagedPointer<DBMain> db_main,
                                 std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : db_main_(db_main), param_map_(std::move(param_map)) {
  ValidateParams();
  // TODO(WAN): Because settings::Param is an enum, the settings manager currently does not expose a simple way
  //  of going from the parameter's name to the parameter's corresponding enum value. The param_name_map_ is a hack.
  for (const auto &item : param_map_) {
    const settings::Param &param = item.first;
    const settings::ParamInfo &param_info = item.second;
    param_name_map_[param_info.name_] = param;
  }
}

void SettingsManager::ValidateParams() {
  // This will expand to invoke settings_manager::DefineSetting on
  // all of the settings defined in settings.h.
  // Example:
  //   ValidateSetting(Param::port, parser::ConstantValueExpression(type::TypeID::INTEGER,
  //   execution::sql::Integer(1024)), parser::ConstantValueExpression(type::TypeID::INTEGER,
  //   execution::sql::Integer(65535)));

#define __SETTING_VALIDATE__         // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_VALIDATE__          // NOLINT
}

void SettingsManager::ValidateSetting(Param param, const parser::ConstantValueExpression &min_value,
                                      const parser::ConstantValueExpression &max_value) {
  const ParamInfo &info = param_map_.find(param)->second;
  if (!ValidateValue(info.value_, min_value, max_value)) {
    throw SETTINGS_EXCEPTION(
        fmt::format("{} is outside the valid range for parameter \"{}\" ({} .. {})", info.value_.ToString(), info.name_,
                    min_value.ToString(), max_value.ToString()),
        common::ErrorCode::ERRCODE_INVALID_PARAMETER_VALUE);
  }
}

#define DEFINE_SETTINGS_MANAGER_GET(Name, CppType)         \
  CppType SettingsManager::Get##Name(Param param) {        \
    common::SharedLatch::ScopedSharedLatch guard(&latch_); \
    return GetValue(param).Peek<CppType>();                \
  }

DEFINE_SETTINGS_MANAGER_GET(Bool, bool)
DEFINE_SETTINGS_MANAGER_GET(Int, int32_t)
DEFINE_SETTINGS_MANAGER_GET(Int64, int64_t)
DEFINE_SETTINGS_MANAGER_GET(Double, double)

#undef DEFINE_SETTINGS_MANAGER_GET

std::string SettingsManager::GetString(Param param) {
  common::SharedLatch::ScopedSharedLatch guard(&latch_);
  const auto &value = GetValue(param);
  return std::string(value.Peek<std::string_view>());
}

#define DEFINE_SETTINGS_MANAGER_SET(Name, CppType, FrontendType, ExecutionType, ShouldCheckMinMaxBounds)            \
  void SettingsManager::Set##Name(Param param, CppType value, common::ManagedPointer<ActionContext> action_context, \
                                  setter_callback_fn setter_callback) {                                             \
    CheckActionContextStateForSet(action_context);                                                                  \
                                                                                                                    \
    const auto &param_info = param_map_.find(param)->second;                                                        \
    const auto min_value = static_cast<CppType>(param_info.min_value_);                                             \
    const auto max_value = static_cast<CppType>(param_info.max_value_);                                             \
                                                                                                                    \
    /* Check new value is within bounds. */                                                                         \
    common::SharedLatch::ScopedExclusiveLatch guard(&latch_);                                                       \
    if ((ShouldCheckMinMaxBounds) && !(value >= min_value && value <= max_value)) {                                 \
      action_context->SetState(ActionState::FAILURE);                                                               \
      setter_callback(action_context);                                                                              \
      throw SETTINGS_EXCEPTION(fmt::format("{} is outside the valid range for parameter \"{}\" ({} .. {})", value,  \
                                           param_info.name_, min_value, max_value),                                 \
                               common::ErrorCode::ERRCODE_INVALID_PARAMETER_VALUE);                                 \
    }                                                                                                               \
                                                                                                                    \
    /* Change the settings value. */                                                                                \
    auto old_value = GetValue(param).Peek<CppType>();                                                               \
    if (!SetValue(param, {FrontendType, ExecutionType(value)})) {                                                   \
      action_context->SetState(ActionState::FAILURE);                                                               \
      setter_callback(action_context);                                                                              \
      /* TODO(WAN): I don't know what the right ErrorCode is here. */                                               \
      throw SETTINGS_EXCEPTION(                                                                                     \
          fmt::format("Unable to set \"{}\" from \"{}\" to \"{}\".", param_info.name_, old_value, value),           \
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);                                                               \
    }                                                                                                               \
                                                                                                                    \
    /* Invoke the action callback. */                                                                               \
    ActionState action_state = InvokeCallback(param, &old_value, &value, action_context);                           \
    if (action_state == ActionState::FAILURE) {                                                                     \
      const bool revert_success = SetValue(param, {FrontendType, ExecutionType(old_value)});                        \
      setter_callback(action_context);                                                                              \
      /* TODO(WAN): I don't know what the right ErrorCode is here. */                                               \
      throw SETTINGS_EXCEPTION(                                                                                     \
          fmt::format("Callback failed after setting \"{}\" from \"{}\" to \"{}\", revert {}.", param_info.name_,   \
                      old_value, value, revert_success ? "succeeded" : "failed"),                                   \
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);                                                               \
    }                                                                                                               \
                                                                                                                    \
    setter_callback(action_context);                                                                                \
  }

DEFINE_SETTINGS_MANAGER_SET(Bool, bool, type::TypeId::BOOLEAN, execution::sql::BoolVal, false);
DEFINE_SETTINGS_MANAGER_SET(Int, int32_t, type::TypeId::INTEGER, execution::sql::Integer, true);
DEFINE_SETTINGS_MANAGER_SET(Int64, int64_t, type::TypeId::BIGINT, execution::sql::Integer, true);
DEFINE_SETTINGS_MANAGER_SET(Double, double, type::TypeId::DECIMAL, execution::sql::Real, true);

void SettingsManager::SetString(Param param, const std::string_view &value,
                                common::ManagedPointer<ActionContext> action_context,
                                setter_callback_fn setter_callback) {
  CheckActionContextStateForSet(action_context);

  const auto &param_info = param_map_.find(param)->second;

  common::SharedLatch::ScopedExclusiveLatch guard(&latch_);
  auto old_cve = std::unique_ptr<parser::ConstantValueExpression>{
      reinterpret_cast<parser::ConstantValueExpression *>(GetValue(param).Copy().release())};

  auto string_val = execution::sql::ValueUtil::CreateStringVal(value);

  if (!SetValue(param, {type::TypeId::VARCHAR, string_val.first, std::move(string_val.second)})) {
    action_context->SetState(ActionState::FAILURE);
    setter_callback(action_context);
    /* TODO(WAN): I don't know what the right ErrorCode is here. */
    throw SETTINGS_EXCEPTION(fmt::format("Unable to set \"{}\" from \"{}\" to \"{}\".", param_info.name_,
                                         old_cve->GetStringVal().StringView(), string_val.first.StringView()),
                             common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }

  std::string_view new_value(value);
  auto old_value = old_cve->Peek<std::string_view>();
  ActionState action_state = InvokeCallback(param, &old_value, &new_value, action_context);
  if (action_state == ActionState::FAILURE) {
    const bool revert_success = SetValue(param, *old_cve);
    setter_callback(action_context);
    /* TODO(WAN): I don't know what the right ErrorCode is here. */
    throw SETTINGS_EXCEPTION(fmt::format("Callback failed after setting \"{}\" from \"{}\" to \"{}\", revert {}.",
                                         param_info.name_, old_value, value, revert_success ? "succeeded" : "failed"),
                             common::ErrorCode::ERRCODE_INTERNAL_ERROR);
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
      return value.Peek<int32_t>() >= min_value.Peek<int32_t>() && value.Peek<int32_t>() <= max_value.Peek<int32_t>();
    case type::TypeId::BIGINT:
      return value.Peek<int64_t>() >= min_value.Peek<int64_t>() && value.Peek<int64_t>() <= max_value.Peek<int32_t>();
    case type::TypeId ::DECIMAL:
      return value.Peek<double>() >= min_value.Peek<double>() && value.Peek<double>() <= max_value.Peek<double>();
    default:
      return true;
  }
}

common::ActionState SettingsManager::InvokeCallback(Param param, void *old_value, void *new_value,
                                                    common::ManagedPointer<common::ActionContext> action_context) {
  callback_fn callback = param_map_.find(param)->second.callback_;
  (callback)(old_value, new_value, db_main_.Get(), action_context);
  ActionState action_state = action_context->GetState();
  NOISEPAGE_ASSERT(action_state == ActionState::FAILURE || action_state == ActionState::SUCCESS,
                   "action context should have state of either SUCCESS or FAILURE on completion.");
  return action_state;
}

// clang-format off
void SettingsManager::ConstructParamMap(                                                      // NOLINT
    std::unordered_map<noisepage::settings::Param, noisepage::settings::ParamInfo> &param_map) {  // NOLINT
  /*
   * Populate gflag values to param map.
   * This will expand to a list of code like:
   * param_map.emplace(
   *     noisepage::settings::Param::port,
   *     noisepage::settings::ParamInfo(port, parser::ConstantValueExpression(type::TypeID::INTEGER,
   *     execution::sql::Integer(FLAGS_port)), "Terrier port (default: 15721)",
   *     parser::ConstantValueExpression(type::TypeID::INTEGER, execution::sql::Integer(15721)),
   *     is_mutable));
   */

#define __SETTING_POPULATE__           // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_POPULATE__            // NOLINT
}  // clang-format on

Param SettingsManager::GetParam(const std::string &name) const {
  // Search for the parameter.
  auto search = param_name_map_.find(name);
  if (search == param_name_map_.end()) {
    throw SETTINGS_EXCEPTION(fmt::format("unrecognized configuration parameter \"{}\"", name),
                             common::ErrorCode::ERRCODE_UNDEFINED_OBJECT);
  }
  Param param = search->second;
  return param;
}

const settings::ParamInfo &SettingsManager::GetParamInfo(const Param &param) const {
  // Search for the parameter's information.
  auto search = param_map_.find(param);
  NOISEPAGE_ASSERT(search != param_map_.end(), "Mismatch between param_name_map_ and param_map_ keys.");
  const ParamInfo &info = search->second;
  return info;
}

const parser::ConstantValueExpression &SettingsManager::GetDefault(const std::string &name) {
  return GetParamInfo(GetParam(name)).default_value_;
}

void SettingsManager::SetParameter(const std::string &name,
                                   const std::vector<common::ManagedPointer<parser::AbstractExpression>> &values) {
  // Search for the parameter to set.
  const Param param = GetParam(name);
  const ParamInfo &info = GetParamInfo(param);
  NOISEPAGE_ASSERT(info.name_ == name, "Inconsistent setting name.");
  const type::TypeId param_type = info.value_.GetReturnValueType();

  // Get the value to be set.
  NOISEPAGE_ASSERT(values.size() == 1, "The SettingsManager currently assumes that each setting only has one value.");
  NOISEPAGE_ASSERT(
      std::all_of(values.begin(), values.end(),
                  [](const auto &expr) { return expr->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT; }),
      "Values should be constant value expressions.");
  const auto &value = values[0].CastManagedPointerTo<parser::ConstantValueExpression>();

  // Check types. If the type of the value does not match the parameter's type, type casting / promotion is attempted.
  if (value->GetReturnValueType() != param_type) {
    try {
      // The binder has exceptional control flow. If this call succeeds, then the type promotion succeeded.
      binder::BinderUtil::CheckAndTryPromoteType(value, param_type);
    } catch (BinderException &e) {
      // The promotion failed and the type remains mismatched, so a SettingsException is thrown.
      throw SETTINGS_EXCEPTION(fmt::format("invalid value for parameter \"{}\": \"{}\"", info.name_, value->ToString()),
                               common::ErrorCode::ERRCODE_INVALID_PARAMETER_VALUE);
    }
  }

  // TODO(WAN): hook up real action contexts and settings callbacks.
  common::ActionContext action_context{common::action_id_t{0}};

  // Set the parameter.
  switch (param_type) {
    case type::TypeId::BOOLEAN:
      SetBool(param, value->GetBoolVal().val_, common::ManagedPointer(&action_context),
              SettingsManager::EmptySetterCallback);
      break;
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
      SetInt(param, value->GetInteger().val_, common::ManagedPointer(&action_context),
             SettingsManager::EmptySetterCallback);
      break;
    case type::TypeId::BIGINT:
      SetInt64(param, value->GetInteger().val_, common::ManagedPointer(&action_context),
               SettingsManager::EmptySetterCallback);
      break;
    case type::TypeId::DECIMAL:
      SetDouble(param, value->GetReal().val_, common::ManagedPointer(&action_context),
                SettingsManager::EmptySetterCallback);
      break;
    case type::TypeId::VARCHAR:
      SetString(param, value->GetStringVal().StringView(), common::ManagedPointer(&action_context),
                SettingsManager::EmptySetterCallback);
      break;
    default:
      UNREACHABLE("Unsupported parameter type.");
  }
}

}  // namespace noisepage::settings
