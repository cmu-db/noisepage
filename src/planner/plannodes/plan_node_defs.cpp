#include "planner/plannodes/plan_node_defs.h"

namespace terrier::planner {

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

}  // namespace terrier::planner
