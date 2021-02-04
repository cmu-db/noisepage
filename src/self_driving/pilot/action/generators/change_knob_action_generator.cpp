#include "self_driving/pilot/action/generators/change_knob_action_generator.h"

#include "common/error/error_code.h"
#include "self_driving/pilot/action/change_knob_action.h"
#include "self_driving/pilot/action/change_knob_value_config.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"

namespace noisepage::selfdriving::pilot {

void ChangeKnobActionGenerator::GenerateActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                                                common::ManagedPointer<settings::SettingsManager> settings_manager,
                                                std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                                std::vector<action_id_t> *candidate_actions) {
  NOISEPAGE_ASSERT(settings_manager != nullptr, "No SettingsManager provided!");
  GenerateActionForType<bool>(settings_manager, action_map, candidate_actions);
  GenerateActionForType<int32_t>(settings_manager, action_map, candidate_actions);
  GenerateActionForType<int64_t>(settings_manager, action_map, candidate_actions);
}

template <class T>
void ChangeKnobActionGenerator::GenerateActionForType(
    common::ManagedPointer<settings::SettingsManager> settings_manager,
    std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map, std::vector<action_id_t> *candidate_actions) {
  common::ManagedPointer<std::map<settings::Param, std::vector<std::pair<T, T>>>> knob_change_value_map = nullptr;
  if constexpr (std::is_same<T, bool>::value) {  // NOLINT
    knob_change_value_map = ChangeKnobValueConfig::GetBoolChangeValueMap();
  } else if constexpr (std::is_same<T, int32_t>::value) {  // NOLINT
    knob_change_value_map = ChangeKnobValueConfig::GetIntChangeValueMap();
  } else if constexpr (std::is_same<T, int64_t>::value) {  // NOLINT
    knob_change_value_map = ChangeKnobValueConfig::GetInt64ChangeValueMap();
  } else {
    throw PILOT_EXCEPTION(fmt::format("Unexpected change knob action type \"{}\"", typeid(T).name()),
                          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  for (auto &it : *knob_change_value_map) {
    settings::Param param = it.first;
    auto value_pairs = it.second;
    for (auto &value_pair : value_pairs) {
      auto first_value = value_pair.first;
      auto second_value = value_pair.second;

      auto param_name = settings_manager->GetParamInfo(param).name_;

      // Generate a pair of reverse actions
      auto first_action = std::make_unique<ChangeKnobAction<T>>(param, param_name, first_value, settings_manager);
      action_id_t first_action_id = first_action->GetActionID();
      action_map->emplace(first_action_id, std::move(first_action));
      candidate_actions->emplace_back(first_action_id);
      action_id_t second_action_id;
      if (first_value != second_value) {
        auto second_action = std::make_unique<ChangeKnobAction<T>>(param, param_name, second_value, settings_manager);
        second_action_id = second_action->GetActionID();
        action_map->emplace(second_action_id, std::move(second_action));
        candidate_actions->emplace_back(second_action_id);
        action_map->at(second_action_id)->AddReverseAction(first_action_id);
      } else {
        // Do not generate the self-reverse second action
        second_action_id = first_action_id;
      }

      // Populate the reverse actions
      action_map->at(first_action_id)->AddReverseAction(second_action_id);

      // Note: change knob actions do not have any enabled/invalidated actions since they're based on deltas and
      // always valid (unless exceeds the knob setting limit, which is handled by `ChangeKnobAction::IsValid()`).
    }
  }
}

}  // namespace noisepage::selfdriving::pilot
