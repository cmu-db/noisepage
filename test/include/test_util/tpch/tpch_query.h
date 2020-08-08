#include <memory>
#include <tuple>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/executable_query.h"

namespace terrier::tpch {
/// Static functions to generate executable queries for TPCH benchmark. Query plans are hard coded.
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                 const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ4(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                 const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ5(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                 const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ6(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                 const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ7(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                 const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ11(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                  const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ16(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                  const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ18(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                  const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
MakeExecutableQ19(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                  const execution::exec::ExecutionSettings &exec_settings);

}  // namespace terrier::tpch
