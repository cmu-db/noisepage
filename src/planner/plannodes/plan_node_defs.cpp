#include "planner/plannodes/plan_node_defs.h"

#include "execution/util/execution_common.h"

namespace noisepage::planner {

std::string PlanNodeTypeToString(PlanNodeType type) {
  switch (type) {
    case PlanNodeType::INVALID:
      return "Invalid";
    case PlanNodeType::SEQSCAN:
      return "SequentialScan";
    case PlanNodeType::INDEXSCAN:
      return "IndexScan";
    case PlanNodeType::CSVSCAN:
      return "CsvScan";
    case PlanNodeType::NESTLOOP:
      return "NestedLoop";
    case PlanNodeType::HASHJOIN:
      return "HashJoin";
    case PlanNodeType::INDEXNLJOIN:
      return "IndexNestedLoopJoin";
    case PlanNodeType::UPDATE:
      return "Update";
    case PlanNodeType::INSERT:
      return "Insert";
    case PlanNodeType::DELETE:
      return "Delete";
    case PlanNodeType::CREATE_DATABASE:
      return "CreateDatabase";
    case PlanNodeType::CREATE_NAMESPACE:
      return "CreateNamespace";
    case PlanNodeType::CREATE_TABLE:
      return "CreateTable";
    case PlanNodeType::CREATE_INDEX:
      return "CreateIndex";
    case PlanNodeType::CREATE_FUNC:
      return "CreateFunction";
    case PlanNodeType::CREATE_TRIGGER:
      return "CreateTrigger";
    case PlanNodeType::CREATE_VIEW:
      return "CreateView";
    case PlanNodeType::DROP_DATABASE:
      return "DropDatabase";
    case PlanNodeType::DROP_NAMESPACE:
      return "DropNamespace";
    case PlanNodeType::DROP_TABLE:
      return "DropTable";
    case PlanNodeType::DROP_INDEX:
      return "DropIndex";
    case PlanNodeType::DROP_TRIGGER:
      return "DropTrigger";
    case PlanNodeType::DROP_VIEW:
      return "DropView";
    case PlanNodeType::ANALYZE:
      return "Analyze";
    case PlanNodeType::AGGREGATE:
      return "Aggregate";
    case PlanNodeType::ORDERBY:
      return "OrderBy";
    case PlanNodeType::PROJECTION:
      return "Projection";
    case PlanNodeType::LIMIT:
      return "Limit";
    case PlanNodeType::DISTINCT:
      return "Distinct";
    case PlanNodeType::HASH:
      return "Hash";
    case PlanNodeType::SETOP:
      return "SetOperation";
    case PlanNodeType::EXPORT_EXTERNAL_FILE:
      return "ExportExternalFile";
    case PlanNodeType::RESULT:
      return "Result";
    case PlanNodeType::MOCK:
      return "Mock";
  }
  UNREACHABLE("Impossible to reach. All plan node types handled.");
}

std::string JoinTypeToString(LogicalJoinType type) {
  switch (type) {
    case LogicalJoinType::INVALID:
      return "Invalid";
    case LogicalJoinType::LEFT:
      return "Left";
    case LogicalJoinType::RIGHT:
      return "Right";
    case LogicalJoinType::INNER:
      return "Inner";
    case LogicalJoinType::OUTER:
      return "Outer";
    case LogicalJoinType::SEMI:
      return "Semi";
    case LogicalJoinType::ANTI:
      return "Anti";
    case LogicalJoinType::LEFT_SEMI:
      return "LeftSemi";
    case LogicalJoinType::RIGHT_SEMI:
      return "RightSemi";
    case LogicalJoinType::RIGHT_ANTI:
      return "RightAnti";
  }
  UNREACHABLE("Impossible to reach. All join types handled.");
}

}  // namespace noisepage::planner
