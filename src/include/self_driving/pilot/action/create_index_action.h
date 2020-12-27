#pragma once

#include "self_driving/pilot/action/abstract_action.h"

namespace noisepage {

namespace planner {
class CreateIndexPlanNode;
}  // namespace planner

namespace selfdriving::pilot {

/**
 * Represent a create index self-driving action
 */
class CreateIndexAction : public AbstractAction {
 private:
  std::unique_ptr<planner::CreateIndexPlanNode> plan_node_;

  uint32_t num_threads_;

  action_id_t drop_index_action_id_;
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
