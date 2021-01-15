#include "self_driving/modeling/operating_unit_util.h"

#include <cstdint>

#include "execution/util/execution_common.h"
#include "self_driving/modeling/operating_unit_defs.h"

namespace noisepage::selfdriving {

bool OperatingUnitUtil::IsOperatingUnitTypeBlocking(ExecutionOperatingUnitType feature) {
  switch (feature) {
    case ExecutionOperatingUnitType::HASHJOIN_BUILD:
    case ExecutionOperatingUnitType::SORT_BUILD:
    case ExecutionOperatingUnitType::SORT_TOPK_BUILD:
    case ExecutionOperatingUnitType::AGGREGATE_BUILD:
    case ExecutionOperatingUnitType::CREATE_INDEX:
      return true;
    default:
      return false;
  }
}

ExecutionOperatingUnitType OperatingUnitUtil::GetNonParallelType(ExecutionOperatingUnitType feature) {
  switch (feature) {
    case ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN:
      return ExecutionOperatingUnitType::HASHJOIN_BUILD;
    case ExecutionOperatingUnitType::PARALLEL_MERGE_AGGBUILD:
      return ExecutionOperatingUnitType::AGGREGATE_BUILD;
    case ExecutionOperatingUnitType::PARALLEL_SORT_STEP:
    case ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP:
      return ExecutionOperatingUnitType::SORT_BUILD;
    case ExecutionOperatingUnitType::PARALLEL_SORT_TOPK_STEP:
    case ExecutionOperatingUnitType::PARALLEL_SORT_TOPK_MERGE_STEP:
      return ExecutionOperatingUnitType::SORT_TOPK_BUILD;
    case ExecutionOperatingUnitType::CREATE_INDEX_MAIN:
      return ExecutionOperatingUnitType::CREATE_INDEX;
    default:
      return ExecutionOperatingUnitType::INVALID;
  }
}

std::string OperatingUnitUtil::ExecutionOperatingUnitTypeToString(ExecutionOperatingUnitType f) {
  // NOTE: Before adding any extra case to this switch statement,
  // please ensure that the output type is actually supported
  // by the mini-runner infrastructure.
  switch (f) {
    case ExecutionOperatingUnitType::AGGREGATE_BUILD:
      return "AGG_BUILD";
    case ExecutionOperatingUnitType::AGGREGATE_ITERATE:
      return "AGG_ITERATE";
    case ExecutionOperatingUnitType::HASHJOIN_BUILD:
      return "HASHJOIN_BUILD";
    case ExecutionOperatingUnitType::HASHJOIN_PROBE:
      return "HASHJOIN_PROBE";
    case ExecutionOperatingUnitType::SORT_BUILD:
      return "SORT_BUILD";
    case ExecutionOperatingUnitType::SORT_TOPK_BUILD:
      return "SORT_TOPK_BUILD";
    case ExecutionOperatingUnitType::SORT_ITERATE:
      return "SORT_ITERATE";
    case ExecutionOperatingUnitType::SEQ_SCAN:
      return "SEQ_SCAN";
    case ExecutionOperatingUnitType::IDX_SCAN:
      return "IDX_SCAN";
    case ExecutionOperatingUnitType::INSERT:
      return "INSERT";
    case ExecutionOperatingUnitType::UPDATE:
      return "UPDATE";
    case ExecutionOperatingUnitType::DELETE:
      return "DELETE";
    case ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS:
      return "OP_INTEGER_PLUS_OR_MINUS";
    case ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY:
      return "OP_INTEGER_MULTIPLY";
    case ExecutionOperatingUnitType::OP_INTEGER_DIVIDE:
      return "OP_INTEGER_DIVIDE";
    case ExecutionOperatingUnitType::OP_INTEGER_COMPARE:
      return "OP_INTEGER_COMPARE";
    case ExecutionOperatingUnitType::OP_REAL_PLUS_OR_MINUS:
      return "OP_REAL_PLUS_OR_MINUS";
    case ExecutionOperatingUnitType::OP_REAL_MULTIPLY:
      return "OP_REAL_MULTIPLY";
    case ExecutionOperatingUnitType::OP_REAL_DIVIDE:
      return "OP_REAL_DIVIDE";
    case ExecutionOperatingUnitType::OP_REAL_COMPARE:
      return "OP_REAL_COMPARE";
    case ExecutionOperatingUnitType::OP_BOOL_COMPARE:
      return "OP_BOOL_COMPARE";
    case ExecutionOperatingUnitType::OP_VARCHAR_COMPARE:
      return "OP_VARCHAR_COMPARE";
    case ExecutionOperatingUnitType::OUTPUT:
      return "OUTPUT";
    case ExecutionOperatingUnitType::LIMIT:
      return "LIMIT";
    case ExecutionOperatingUnitType::INDEX_INSERT:
      return "INDEX_INSERT";
    case ExecutionOperatingUnitType::INDEX_DELETE:
      return "INDEX_DELETE";
    case ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN:
      return "PARALLEL_MERGE_HASHJOIN";
    case ExecutionOperatingUnitType::PARALLEL_MERGE_AGGBUILD:
      return "PARALLEL_MERGE_AGGBUILD";
    case ExecutionOperatingUnitType::PARALLEL_SORT_STEP:
      return "PARALLEL_SORT_STEP";
    case ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP:
      return "PARALLEL_SORT_MERGE_STEP";
    case ExecutionOperatingUnitType::PARALLEL_SORT_TOPK_STEP:
      return "PARALLEL_SORT_TOPK_STEP";
    case ExecutionOperatingUnitType::PARALLEL_SORT_TOPK_MERGE_STEP:
      return "PARALLEL_SORT_TOPK_MERGE_STEP";
    case ExecutionOperatingUnitType::CREATE_INDEX:
      return "CREATE_INDEX";
    case ExecutionOperatingUnitType::CREATE_INDEX_MAIN:
      return "CREATE_INDEX_MAIN";
    default:
      UNREACHABLE("Undefined ExecutionOperatingUnitType encountered");
      break;
  }
}

}  // namespace noisepage::selfdriving
