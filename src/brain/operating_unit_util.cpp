#include <cstdint>

#include "brain/brain_defs.h"
#include "brain/operating_unit_util.h"

namespace noisepage::brain {

bool OperatingUnitUtil::IsOperatingUnitTypeBlocking(ExecutionOperatingUnitType feature) {
  switch (feature) {
    case ExecutionOperatingUnitType::HASHJOIN_BUILD:
    case ExecutionOperatingUnitType::SORT_BUILD:
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
    case ExecutionOperatingUnitType::CREATE_INDEX_MAIN:
      return ExecutionOperatingUnitType::CREATE_INDEX;
    default:
      return ExecutionOperatingUnitType::INVALID;
  }
}

}  // namespace noisepage::brain
