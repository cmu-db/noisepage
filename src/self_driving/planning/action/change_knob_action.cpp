#include "self_driving/planning/action/change_knob_action.h"

#include "common/error/error_code.h"
#include "self_driving/planning/mcts/action_state.h"
#include "settings/settings_manager.h"

namespace noisepage::selfdriving::pilot {

template <class T>
ChangeKnobAction<T>::ChangeKnobAction(settings::Param param, std::string param_name, T change_value,
                                      common::ManagedPointer<settings::SettingsManager> settings_manager)
    // ChangeKnobAction should not need a database oid so put the invalid oid here
    : AbstractAction(ActionType::CHANGE_KNOB, catalog::INVALID_DATABASE_OID),
      param_(param),
      param_name_(std::move(param_name)),
      change_value_(change_value),
      settings_manager_(settings_manager) {
  auto param_info = settings_manager_->GetParamInfo(param_);
  param_min_value_ = param_info.min_value_;
  param_max_value_ = param_info.max_value_;
}

template <class T>
const std::string &ChangeKnobAction<T>::GetSQLCommand() {
  sql_command_ = "set " + param_name_ + " = ";
  // Set the new value accordingly based on the param type
  if constexpr (std::is_same<T, bool>::value) {  // NOLINT
    T original_value = settings_manager_->GetBool(param_);
    T new_value = original_value ^ change_value_;
    sql_command_ += new_value ? "'true';" : "'false';";
  } else if constexpr (std::is_same<T, int32_t>::value) {  // NOLINT
    T original_value = settings_manager_->GetInt(param_);
    T new_value = original_value + change_value_;
    sql_command_ += std::to_string(new_value) + ";";
  } else if constexpr (std::is_same<T, int64_t>::value) {  // NOLINT
    T original_value = settings_manager_->GetInt64(param_);
    T new_value = original_value + change_value_;
    sql_command_ += std::to_string(new_value) + ";";
  } else {
    throw PILOT_EXCEPTION(fmt::format("Unexpected knob parameter type \"{}\"", param_name_),
                          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  return sql_command_;
}

template <class T>
void ChangeKnobAction<T>::ModifyActionState(ActionState *action_state) {
  // Set the new value accordingly based on the param type
  T new_value;
  if constexpr (std::is_same<T, bool>::value) {  // NOLINT
    T original_value = settings_manager_->GetBool(param_);
    new_value = original_value ^ change_value_;
  } else if constexpr (std::is_same<T, int32_t>::value) {  // NOLINT
    T original_value = settings_manager_->GetInt(param_);
    new_value = original_value + change_value_;
  } else if constexpr (std::is_same<T, int64_t>::value) {  // NOLINT
    T original_value = settings_manager_->GetInt64(param_);
    new_value = original_value + change_value_;
  } else {
    throw PILOT_EXCEPTION(fmt::format("Unexpected knob parameter type \"{}\"", param_name_),
                          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  // TODO(lin): we implicitly cast everything to in64_t for now. Need to extend to other types, e.g., string
  action_state->UpdateKnobValue(param_name_, new_value);
}

template <class T>
bool ChangeKnobAction<T>::IsValid() {
  if constexpr (std::is_same<T, bool>::value) {  // NOLINT
    return true;
  } else if constexpr (std::is_same<T, int32_t>::value) {  // NOLINT
    T original_value = settings_manager_->GetInt(param_);
    T new_value = original_value + change_value_;
    return new_value <= param_max_value_ && new_value >= param_min_value_;
  } else if constexpr (std::is_same<T, int64_t>::value) {  // NOLINT
    T original_value = settings_manager_->GetInt64(param_);
    T new_value = original_value + change_value_;
    return new_value <= param_max_value_ && new_value >= param_min_value_;
  } else {
    throw PILOT_EXCEPTION(fmt::format("Unexpected knob parameter type \"{}\"", param_name_),
                          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

template class ChangeKnobAction<bool>;
template class ChangeKnobAction<int32_t>;
template class ChangeKnobAction<int64_t>;

}  // namespace noisepage::selfdriving::pilot
