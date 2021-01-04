#include "gtest/gtest.h"
#include "main/db_main.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/action_defs.h"
#include "self_driving/pilot/action/change_knob_action_generator.h"
#include "self_driving/pilot/action/change_knob_value_config.h"
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
TEST_F(GenerateChangeKnobAction, GenerateChangeKnobAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  auto settings_manager = db_main_->GetSettingsManager();
  ChangeKnobActionGenerator::GenerateChangeKnobActions(settings_manager, &action_map, &candidate_actions);

  // Each bool knob only has on action since the action is self-reverse
  size_t num_actions = ChangeKnobValueConfig::GetBoolChangeValueMap()->size();
  for (auto &it : *ChangeKnobValueConfig::GetInt64ChangeValueMap()) num_actions += it.second.size() * 2;

  EXPECT_EQ(action_map.size(), num_actions);
  EXPECT_EQ(candidate_actions.size(), num_actions);
}

}  // namespace noisepage::selfdriving::pilot::test
