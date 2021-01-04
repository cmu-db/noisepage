#pragma once

#include <map>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "self_driving/pilot/action/action_defs.h"

namespace noisepage {

namespace planner {
class AbstractPlanNode;
}  // namespace planner

namespace parser {
class AbstractExpression;
class ColumnValueExpression;
}  // namespace parser

namespace selfdriving::pilot {

class AbstractAction;

/**
 * Generate create/drop index candidate actions
 */
class IndexActionGenerator {
 public:
  /**
   * Generate index create/drop actions for the given workload (query plans)
   * @param plans The set of query plans to generate index actions for
   * @param action_map Maps action id to the pointer of the generated action.
   * @param candidate_actions To hold the ids of the candidate actions
   */
  static void GenerateIndexActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                                   std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                   std::vector<action_id_t> *candidate_actions);

 private:
  static void FindMissingIndex(const planner::AbstractPlanNode *plan,
                               std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                               std::vector<action_id_t> *candidate_actions);

  static bool GenerateIndexableColumns(
      catalog::table_oid_t table_oid, common::ManagedPointer<parser::AbstractExpression> expr,
      std::vector<common::ManagedPointer<parser::ColumnValueExpression>> *equality_columns,
      std::vector<common::ManagedPointer<parser::ColumnValueExpression>> *inequality_columns);
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
