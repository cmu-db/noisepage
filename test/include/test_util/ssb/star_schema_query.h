#include <memory>
#include <tuple>

#include "catalog/catalog_accessor.h"
#include "execution/compiler/executable_query.h"

namespace terrier::ssb {
class SSBQuery {
 public:
  /// Static functions to generate executable queries for SSB benchmark. Query plans are hard coded.
  // TODO(Wuwen): modify q16 after LIKE fix and q19 after VARCHAR fix
  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ1_1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ1_2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ1_3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ2_1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ2_2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ2_3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3_1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3_2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3_3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ3_4(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ4_1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ4_2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);

  static std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
  SSBMakeExecutableQ4_3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                        const execution::exec::ExecutionSettings &exec_settings);
};
}  // namespace terrier::ssb
