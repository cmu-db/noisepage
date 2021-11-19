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

// std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
// ProcbenchQuery::MakeExecutableQ1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
//                             const execution::exec::ExecutionSettings &exec_settings) {
//   execution::compiler::test::ExpressionMaker expr_maker;
//   auto table_oid = accessor->GetTableOid("lineitem");
//   const auto &l_schema = accessor->GetSchema(table_oid);
//   // Scan the table
//   std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
//   execution::compiler::test::OutputSchemaHelper l_seq_scan_out{0, &expr_maker};
//   {
//     // Read all needed columns
//     auto l_returnflag = expr_maker.CVE(l_schema.GetColumn("l_returnflag").Oid(), execution::sql::SqlTypeId::Varchar);
//     auto l_linestatus = expr_maker.CVE(l_schema.GetColumn("l_linestatus").Oid(), execution::sql::SqlTypeId::Varchar);
//     auto l_extendedprice =
//         expr_maker.CVE(l_schema.GetColumn("l_extendedprice").Oid(), execution::sql::SqlTypeId::Double);
//     auto l_discount = expr_maker.CVE(l_schema.GetColumn("l_discount").Oid(), execution::sql::SqlTypeId::Double);
//     auto l_tax = expr_maker.CVE(l_schema.GetColumn("l_tax").Oid(), execution::sql::SqlTypeId::Double);
//     auto l_quantity = expr_maker.CVE(l_schema.GetColumn("l_quantity").Oid(), execution::sql::SqlTypeId::Double);
//     auto l_shipdate = expr_maker.CVE(l_schema.GetColumn("l_shipdate").Oid(), execution::sql::SqlTypeId::Date);
//     std::vector<catalog::col_oid_t> col_oids = {
//         l_schema.GetColumn("l_returnflag").Oid(),    l_schema.GetColumn("l_linestatus").Oid(),
//         l_schema.GetColumn("l_extendedprice").Oid(), l_schema.GetColumn("l_discount").Oid(),
//         l_schema.GetColumn("l_tax").Oid(),           l_schema.GetColumn("l_quantity").Oid(),
//         l_schema.GetColumn("l_shipdate").Oid()};
//     // Make the output schema
//     l_seq_scan_out.AddOutput("l_returnflag", l_returnflag);
//     l_seq_scan_out.AddOutput("l_linestatus", l_linestatus);
//     l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
//     l_seq_scan_out.AddOutput("l_discount", l_discount);
//     l_seq_scan_out.AddOutput("l_tax", l_tax);
//     l_seq_scan_out.AddOutput("l_quantity", l_quantity);
//     auto schema = l_seq_scan_out.MakeSchema();
//     // Make the predicate
//     l_seq_scan_out.AddOutput("l_shipdate", l_shipdate);
//     auto date_const = expr_maker.Constant(1998, 9, 2);
//     auto predicate = expr_maker.ComparisonLt(l_shipdate, date_const);
//     // Build
//     planner::SeqScanPlanNode::Builder builder;
//     l_seq_scan = builder.SetOutputSchema(std::move(schema))
//                      .SetScanPredicate(predicate)
//                      .SetTableOid(table_oid)
//                      .SetColumnOids(std::move(col_oids))
//                      .Build();
//   }
//   // Make the aggregate
//   std::unique_ptr<planner::AbstractPlanNode> agg;
//   execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
//   {
//     // Read previous layer's output
//     auto l_returnflag = l_seq_scan_out.GetOutput("l_returnflag");
//     auto l_linestatus = l_seq_scan_out.GetOutput("l_linestatus");
//     auto l_quantity = l_seq_scan_out.GetOutput("l_quantity");
//     auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
//     auto l_discount = l_seq_scan_out.GetOutput("l_discount");
//     auto l_tax = l_seq_scan_out.GetOutput("l_tax");
//     // Make the aggregate expressions
//     auto sum_qty = expr_maker.AggSum(l_quantity);
//     auto sum_base_price = expr_maker.AggSum(l_extendedprice);
//     auto one_const = expr_maker.Constant(1.0f);
//     auto disc_price = expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount));
//     auto sum_disc_price = expr_maker.AggSum(disc_price);
//     auto charge = expr_maker.OpMul(disc_price, expr_maker.OpSum(one_const, l_tax));
//     auto sum_charge = expr_maker.AggSum(charge);
//     auto avg_qty = expr_maker.AggAvg(l_quantity);
//     auto avg_price = expr_maker.AggAvg(l_extendedprice);
//     auto avg_disc = expr_maker.AggAvg(l_discount);
//     auto count_order = expr_maker.AggCount(expr_maker.Constant(1));  // Works as Count(*)
//     // Add them to the helper.
//     agg_out.AddGroupByTerm("l_returnflag", l_returnflag);
//     agg_out.AddGroupByTerm("l_linestatus", l_linestatus);
//     agg_out.AddAggTerm("sum_qty", sum_qty);
//     agg_out.AddAggTerm("sum_base_price", sum_base_price);
//     agg_out.AddAggTerm("sum_disc_price", sum_disc_price);
//     agg_out.AddAggTerm("sum_charge", sum_charge);
//     agg_out.AddAggTerm("avg_qty", avg_qty);
//     agg_out.AddAggTerm("avg_price", avg_price);
//     agg_out.AddAggTerm("avg_disc", avg_disc);
//     agg_out.AddAggTerm("count_order", count_order);
//     // Make the output schema
//     agg_out.AddOutput("l_returnflag", agg_out.GetGroupByTermForOutput("l_returnflag"));
//     agg_out.AddOutput("l_linestatus", agg_out.GetGroupByTermForOutput("l_linestatus"));
//     agg_out.AddOutput("sum_qty", agg_out.GetAggTermForOutput("sum_qty"));
//     agg_out.AddOutput("sum_base_price", agg_out.GetAggTermForOutput("sum_base_price"));
//     agg_out.AddOutput("sum_disc_price", agg_out.GetAggTermForOutput("sum_disc_price"));
//     agg_out.AddOutput("sum_charge", agg_out.GetAggTermForOutput("sum_charge"));
//     agg_out.AddOutput("avg_qty", agg_out.GetAggTermForOutput("avg_qty"));
//     agg_out.AddOutput("avg_price", agg_out.GetAggTermForOutput("avg_price"));
//     agg_out.AddOutput("avg_disc", agg_out.GetAggTermForOutput("avg_disc"));
//     agg_out.AddOutput("count_order", agg_out.GetAggTermForOutput("count_order"));
//     auto schema = agg_out.MakeSchema();
//     // Build
//     planner::AggregatePlanNode::Builder builder;
//     agg = builder.SetOutputSchema(std::move(schema))
//               .AddGroupByTerm(l_returnflag)
//               .AddGroupByTerm(l_linestatus)
//               .AddAggregateTerm(sum_qty)
//               .AddAggregateTerm(sum_base_price)
//               .AddAggregateTerm(sum_disc_price)
//               .AddAggregateTerm(sum_charge)
//               .AddAggregateTerm(avg_qty)
//               .AddAggregateTerm(avg_price)
//               .AddAggregateTerm(avg_disc)
//               .AddAggregateTerm(count_order)
//               .AddChild(std::move(l_seq_scan))
//               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
//               .SetHavingClausePredicate(nullptr)
//               .Build();
//   }

//   // Order By
//   std::unique_ptr<planner::AbstractPlanNode> order_by;
//   execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
//   {
//     // Output Colums col1, col2, col1 + col2
//     auto l_returnflag = agg_out.GetOutput("l_returnflag");
//     auto l_linestatus = agg_out.GetOutput("l_linestatus");
//     auto sum_qty = agg_out.GetOutput("sum_qty");
//     auto sum_base_price = agg_out.GetOutput("sum_base_price");
//     auto sum_disc_price = agg_out.GetOutput("sum_disc_price");
//     auto sum_charge = agg_out.GetOutput("sum_charge");
//     auto avg_qty = agg_out.GetOutput("avg_qty");
//     auto avg_price = agg_out.GetOutput("avg_price");
//     auto avg_disc = agg_out.GetOutput("avg_disc");
//     auto count_order = agg_out.GetOutput("count_order");
//     order_by_out.AddOutput("l_returnflag", l_returnflag);
//     order_by_out.AddOutput("l_linestatus", l_linestatus);
//     order_by_out.AddOutput("sum_qty", sum_qty);
//     order_by_out.AddOutput("sum_base_price", sum_base_price);
//     order_by_out.AddOutput("sum_disc_price", sum_disc_price);
//     order_by_out.AddOutput("sum_charge", sum_charge);
//     order_by_out.AddOutput("avg_qty", avg_qty);
//     order_by_out.AddOutput("avg_price", avg_price);
//     order_by_out.AddOutput("avg_disc", avg_disc);
//     order_by_out.AddOutput("count_order", count_order);
//     auto schema = order_by_out.MakeSchema();
//     // Order By Clause
//     planner::SortKey clause1{l_returnflag, optimizer::OrderByOrderingType::ASC};
//     planner::SortKey clause2{l_linestatus, optimizer::OrderByOrderingType::ASC};
//     // Build
//     planner::OrderByPlanNode::Builder builder;
//     order_by = builder.SetOutputSchema(std::move(schema))
//                    .AddChild(std::move(agg))
//                    .AddSortKey(clause1.first, clause1.second)
//                    .AddSortKey(clause2.first, clause2.second)
//                    .Build();
//   }
//   auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
//   return std::make_tuple(std::move(query), std::move(order_by));
// }

}  // namespace noisepage::procbench
