#include "execution/compiler/executable_query.h"
#include "catalog/catalog_accessor.h"

namespace terrier::tpch {
/// Static functions to generate executable queries for TPCH benchmark. Query plans are hard coded.
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ1(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ4(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ5(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ6(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ7(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ11(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ16(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ18(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>> MakeExecutableQ19(std::unique_ptr<catalog::CatalogAccessor> &accessor, const execution::exec::ExecutionSettings &exec_settings);

}
