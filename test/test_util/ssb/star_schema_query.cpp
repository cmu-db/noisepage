#include "test_util/ssb/star_schema_query.h"

#include "execution/compiler/compilation_context.h"

#include "catalog/catalog_accessor.h"
#include "execution/compiler/expression_maker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/sql/sql_def.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "loggers/execution_logger.h"

namespace terrier::ssb {
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ1Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                              const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
// Date
//auto d_table_oid = accessor->GetTableOid("ssbm.date");
//  const auto &d_schema = accessor->GetSchema(d_table_oid);
// LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);


// Scan date
//std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
//  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
//{
//auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
//auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
//  std::vector<catalog::col_oid_t> d_oids = {
//      d_schema.GetColumn("d_datekey").Oid(), d_schema.GetColumn("d_year").Oid()
//};
//// Make the predicate: d_year=1993
//auto _1993 = expr_maker.Constant(1993);
//auto predicate = expr_maker.ComparisonEq(d_year, _1993);
//// Make output schema.
//d_seq_scan_out.AddOutput("d_datekey", d_datekey);
//// Build plan node.
//d_seq_scan = planner::SeqScanPlanNode::Builder{}
//    .SetOutputSchema(d_seq_scan_out.MakeSchema())
//    .SetScanPredicate(predicate)
//    .SetTableOid(d_table_oid)
//                 .SetColumnOids(std::move(d_oids))
//    .Build();
//}

// Scan lineorder.
std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{0 , &expr_maker};
{

auto lo_orderdate =
    expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
auto lo_extendedprice =
    expr_maker.CVE(lo_schema.GetColumn("lo_extendedprice").Oid(), type::TypeId::INTEGER);
auto lo_discount =
    expr_maker.CVE(lo_schema.GetColumn("lo_discount").Oid(), type::TypeId::INTEGER);
auto lo_quantity =
   expr_maker.CVE(lo_schema.GetColumn("lo_quantity").Oid(), type::TypeId::INTEGER);
  EXECUTION_LOG_INFO("lo_orderdate: {}, lo_extendedprice: {}, lo_discount: {}.", uint32_t(lo_schema.GetColumn("lo_orderdate").Oid()), uint32_t(lo_schema.GetColumn("lo_extendedprice").Oid()), uint32_t(lo_schema.GetColumn("lo_discount").Oid()));

  std::vector<catalog::col_oid_t> lo_col_oids = {
      lo_schema.GetColumn("lo_quantity").Oid(), lo_schema.GetColumn("lo_discount").Oid(),lo_schema.GetColumn("lo_extendedprice").Oid(),  lo_schema.GetColumn("lo_orderdate").Oid()
};
// Make predicate: lo_discount between 1 and 3 AND lo_quantity < 25
//  auto low_predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonGt(lo_discount, expr_maker.Constant(1)), expr_maker.ComparisonEq(lo_discount, expr_maker.Constant(1)));
 // auto high_predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonLt(lo_discount, expr_maker.Constant(3)), expr_maker.ComparisonEq(lo_discount, expr_maker.Constant(3)));
  auto predicate = expr_maker.ComparisonLt(lo_discount, expr_maker.Constant(3));
//auto predicate = expr_maker.ConjunctionAnd(
//    low_predicate,
//    expr_maker.ComparisonLt(lo_quantity, expr_maker.Constant(25)));
// Make output schema.
lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    lo_seq_scan_out.AddOutput("lo_quantity", lo_quantity);
    auto schema = lo_seq_scan_out.MakeSchema();
// Build plan node.
planner::SeqScanPlanNode::Builder builder;
lo_seq_scan = builder.SetOutputSchema(std::move(schema))
    .SetScanPredicate(predicate)
    .SetTableOid(lo_table_oid)
                  .SetColumnOids(std::move(lo_col_oids))
    .Build();
}
//
//// date <-> lineorder join.
//std::unique_ptr<planner::AbstractPlanNode> hash_join;
//  execution::compiler::test::OutputSchemaHelper hash_join_out{0, &expr_maker};
//{
//// Left columns.
//auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
//// Right columns.
//auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
//auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
//auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
//// Output Schema
//hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
//hash_join_out.AddOutput("lo_discount", lo_discount);
//// Build
//planner::HashJoinPlanNode::Builder builder;
//hash_join = builder.AddChild(std::move(d_seq_scan))
//    .AddChild(std::move(lo_seq_scan))
//    .SetOutputSchema(hash_join_out.MakeSchema())
//    .AddLeftHashKey(d_datekey)
//    .AddRightHashKey(lo_orderdate)
//    .SetJoinType(planner::LogicalJoinType::INNER)
//    .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
//    .Build();
//}
//
//// Make the aggregate
//std::unique_ptr<planner::AbstractPlanNode> agg;
//  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
//{
//// Read previous layer's output
//auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
//auto lo_discount = hash_join_out.GetOutput("lo_discount");
//auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
//// Add them to the helper.
//agg_out.AddAggTerm("revenue", revenue);
//// Make the output schema.
//agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
//// Build plan node.
//planner::AggregatePlanNode::Builder builder;
//agg = builder.SetOutputSchema(agg_out.MakeSchema())
//    .AddAggregateTerm(revenue)
//    .AddChild(std::move(hash_join))
//    .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
//    .SetHavingClausePredicate(nullptr)
//    .Build();
//}

// Compile plan!
  auto query = execution::compiler::CompilationContext::Compile(*lo_seq_scan, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(lo_seq_scan));
}



}  // namespace terrier::ssb
