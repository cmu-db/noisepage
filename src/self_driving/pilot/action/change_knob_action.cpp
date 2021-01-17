#include "self_driving/pilot/action/change_knob_action.h"

#include "common/error/error_code.h"
#include "settings/settings_manager.h"

namespace noisepage::selfdriving::pilot {

template <class T>
const std::string &ChangeKnobAction<T>::GetSQLCommand() {
  sql_command_ = "set " + param_name_ + " ";
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
    throw PILOT_EXCEPTION(fmt::format("Unexpected knob parameter type\"{}\"", param_name_),
                          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  return sql_command_;
}

template class ChangeKnobAction<bool>;
template class ChangeKnobAction<int32_t>;
template class ChangeKnobAction<int64_t>;

}  // namespace noisepage::selfdriving::pilot
