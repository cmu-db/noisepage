#pragma once

#include <utility>
#include "self_driving/pilot/action/abstract_action.h"
#include "settings/settings_param.h"

namespace noisepage::settings {
class SettingsManager;
}

namespace noisepage::selfdriving::pilot {

/**
 * Represent a change knob self-driving action
 */
template <class T>
class ChangeKnobAction : public AbstractAction {
 public:
  /**
   * Construct ChangeKnobAction
   * @param index_name The name of the index
   * @param table_name The table to create index on
   * @param columns The columns to build index on
   */
  ChangeKnobAction(settings::Param param, std::string param_name, T change_value,
                   common::ManagedPointer<settings::SettingsManager> settings_manager)
      : AbstractAction(ActionType::CHANGE_KNOB),
        param_(param),
        param_name_(std::move(param_name)),
        change_value_(change_value),
        settings_manager_(settings_manager) {}

  /**
   * Get the SQL command to apply the action
   * @return Action SQL command; throws exception if the param type is unsupported
   */
  const std::string &GetSQLCommand() override;

 private:
  settings::Param param_;
  std::string param_name_;
  T change_value_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
};

}  // namespace noisepage::selfdriving::pilot
