#pragma once

#include <map>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "self_driving/pilot/action/action_defs.h"
#include "self_driving/pilot/action/generators/abstract_action_generator.h"

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
class IndexActionGenerator : AbstractActionGenerator {
 public:
  void GenerateActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                       common::ManagedPointer<settings::SettingsManager> settings_manager,
                       std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                       std::vector<action_id_t> *candidate_actions) override;

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
