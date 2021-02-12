#pragma once

#include <string>
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
   * @param param Which knob param
   * @param param_name Name of the param
   * @param change_value The value to change that knob with
   * @param settings_manager SettingsManager (used to find our the current knob value before applying the change_value)
   */
  ChangeKnobAction(settings::Param param, std::string param_name, T change_value,
                   common::ManagedPointer<settings::SettingsManager> settings_manager);

  const std::string &GetSQLCommand() override;

  bool IsValid() override;

 private:
  settings::Param param_;
  std::string param_name_;
  T change_value_;
  T param_min_value_;
  T param_max_value_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
};

}  // namespace noisepage::selfdriving::pilot
