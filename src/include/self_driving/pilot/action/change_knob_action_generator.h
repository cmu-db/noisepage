#pragma once

#include <memory>
#include <vector>

#include "self_driving/pilot/action/action_defs.h"

namespace noisepage {

namespace settings {
class SettingsManager;
}

namespace selfdriving::pilot {

class AbstractAction;

/**
 * Generate change knob candidate actions
 */
class ChangeKnobActionGenerator {
  /**
   * Generate change knob self-driving actions
   * @param action_map Maps action id to the pointer of the generated action.
   * @param candidate_actions To hold the ids of the candidate actions
   */
  static void GenerateChangeKnobActions(common::ManagedPointer<settings::SettingsManager> settings_manager,
                                        std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                        std::vector<action_id_t> *candidate_actions);

 private:
  template <class T>
  static void GenerateActionForType(common::ManagedPointer<settings::SettingsManager> settings_manager,
                                    std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                    std::vector<action_id_t> *candidate_actions);
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
