#include "gtest/gtest.h"
#include "main/db_main.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/action_defs.h"
#include "self_driving/pilot/action/change_knob_value_config.h"
#include "self_driving/pilot/action/generators/change_knob_action_generator.h"
#include "test_util/test_harness.h"

namespace noisepage::selfdriving::pilot::test {

class GenerateChangeKnobAction : public TerrierTest {
  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);
    db_main_ = DBMain::Builder().SetUseSettingsManager(true).SetSettingsParameterMap(std::move(param_map)).Build();
  }

 protected:
  std::unique_ptr<DBMain> db_main_;
};

// NOLINTNEXTLINE
TEST_F(GenerateChangeKnobAction, GenerateAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  auto settings_manager = db_main_->GetSettingsManager();
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  ChangeKnobActionGenerator().GenerateActions(plans, settings_manager, &action_map, &candidate_actions);

  // Each bool knob only has on action since the action is self-reverse
  auto bool_change_value_map = ChangeKnobValueConfig::GetBoolChangeValueMap();
  auto int_change_value_map = ChangeKnobValueConfig::GetIntChangeValueMap();
  auto int64_change_value_map = ChangeKnobValueConfig::GetInt64ChangeValueMap();
  size_t num_actions = bool_change_value_map->size();
  for (auto &it : *int_change_value_map) num_actions += it.second.size() * 2;
  for (auto &it : *int64_change_value_map) num_actions += it.second.size() * 2;

  EXPECT_EQ(action_map.size(), num_actions);
  EXPECT_EQ(candidate_actions.size(), num_actions);

  size_t num_valid_actions = 0;
  std::unordered_set<std::string> commands;
  commands.reserve(action_map.size());
  for (auto &it : action_map) {
    commands.emplace(it.second->GetSQLCommand());
    num_valid_actions += it.second->IsValid() ? 1 : 0;
  }

  size_t expected_num_valid_actions = 0;

  std::unordered_set<std::string> expected_commands;
  expected_commands.reserve(action_map.size());
  for (auto &it : *bool_change_value_map) {
    auto param = it.first;
    bool original_value = settings_manager->GetBool(param);
    std::string command = "set " + settings_manager->GetParamInfo(param).name_ + " = ";
    command += original_value ? "'false';" : "'true';";
    expected_commands.emplace(command);
    // Change bool actions should always be valid
    expected_num_valid_actions++;
  }

  for (auto &it : *int_change_value_map) {
    auto param = it.first;
    int32_t original_value = settings_manager->GetInt(param);

    for (auto &value_pair : it.second) {
      auto param_info = settings_manager->GetParamInfo(param);
      std::string command = "set " + param_info.name_ + " = ";
      int new_value = original_value + value_pair.first;
      expected_num_valid_actions += new_value <= param_info.max_value_ && new_value >= param_info.min_value_ ? 1 : 0;
      command += std::to_string(new_value) + ";";
      expected_commands.emplace(command);
      command = "set " + param_info.name_ + " = ";
      new_value = original_value + value_pair.second;
      expected_num_valid_actions += new_value <= param_info.max_value_ && new_value >= param_info.min_value_ ? 1 : 0;
      command += std::to_string(new_value) + ";";
      expected_commands.emplace(command);
    }
  }

  for (auto &it : *int64_change_value_map) {
    auto param = it.first;
    int64_t original_value = settings_manager->GetInt64(param);

    for (auto &value_pair : it.second) {
      auto param_info = settings_manager->GetParamInfo(param);
      std::string command = "set " + param_info.name_ + " = ";
      int64_t new_value = original_value + value_pair.first;
      expected_num_valid_actions += new_value <= param_info.max_value_ && new_value >= param_info.min_value_ ? 1 : 0;
      command += std::to_string(new_value) + ";";
      expected_commands.emplace(command);
      command = "set " + param_info.name_ + " = ";
      new_value = original_value + value_pair.second;
      expected_num_valid_actions += new_value <= param_info.max_value_ && new_value >= param_info.min_value_ ? 1 : 0;
      command += std::to_string(new_value) + ";";
      expected_commands.emplace(command);
    }
  }

  EXPECT_EQ(commands, expected_commands);
  EXPECT_EQ(expected_num_valid_actions, expected_num_valid_actions);
}

}  // namespace noisepage::selfdriving::pilot::test
