#include <memory>
#include <tuple>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/executable_query.h"

namespace noisepage::ssb {
class SSBQuery {
 public:
  /// Static functions to generate executable queries for SSB benchmark. Query plans are hard coded.
  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ1Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ1Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ1Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ2Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ2Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ2Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3Part4(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ4Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ4Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ4Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                           const execution::exec::ExecutionSettings &exec_settings);
};
}  // namespace noisepage::ssb
