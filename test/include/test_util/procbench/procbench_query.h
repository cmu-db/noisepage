#pragma once

#include <memory>
#include <tuple>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/executable_query.h"

namespace noisepage::procbench {

/** ProcbenchQuery defines queries for SQL Procbench benchmarks. */
class ProcbenchQuery {
 public:
  // Static functions to generate executable queries for ProcBench benchmark. Query plans are hard coded.
  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  MakeExecutableQ6(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                   const execution::exec::ExecutionSettings &exec_settings);
};
}  // namespace noisepage::procbench
