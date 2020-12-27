#pragma once

#include <memory>
#include <vector>

#include "self_driving/pilot/action/action_defs.h"

namespace noisepage {

namespace planner {
class AbstractPlanNode;
}  // namespace planner

namespace selfdriving::pilot {

class AbstractAction;

/**
 * Generate create/drop index candidate actions
 */
class IndexActionGenerator {
  /**
   * Generate index create/drop actions for the given workload (query plans)
   * @param plans The set of query plans to generate index actions for
   * @return A pair: the first element is a vector to hold the pointer for the generated actions. The second
   * element is a vector that contains the ids of the candidate actions
   */
  std::pair<std::vector<std::unique_ptr<AbstractAction>>, std::vector<action_id_t>> GenerateIndexActions(
      const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans);

 private:
  void FindMissingIndex(const planner::AbstractPlanNode *plan);

  std::vector<std::unique_ptr<AbstractAction>> actions_;
  std::vector<action_id_t> candidate_action_ids_;
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
