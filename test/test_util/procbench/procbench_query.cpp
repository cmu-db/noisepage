#include "test_util/procbench/procbench_query.h"

#include "catalog/catalog_accessor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/expression_maker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/sql/sql_def.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace noisepage::procbench {

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
ProcbenchQuery::MakeExecutableQ6(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                 const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  const auto web_sales_history_oid = accessor->GetTableOid("web_sales_history");
  const auto &web_sales_history_schema = accessor->GetSchema(web_sales_history_oid);

  // Scan the table
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  execution::compiler::test::OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto ws_sold_date =
        expr_maker.CVE(web_sales_history_oid, web_sales_history_schema.GetColumn("ws_sold_date_sk").Oid(),
                       execution::sql::SqlTypeId::Integer);
    std::vector<catalog::col_oid_t> col_oids = {web_sales_history_schema.GetColumn("ws_sold_date_sk").Oid()};

    // Make the output schema
    seq_scan_out.AddOutput("ws_sold_date", ws_sold_date);
    auto schema = seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(nullptr)
                   .SetTableOid(web_sales_history_oid)
                   .SetColumnOids(std::move(col_oids))
                   .Build();
  }
  auto query = execution::compiler::CompilationContext::Compile(*seq_scan, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(seq_scan));
}

}  // namespace noisepage::procbench
