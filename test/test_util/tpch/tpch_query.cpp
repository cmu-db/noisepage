#include "test_util/tpch/tpch_query.h"

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

namespace noisepage::tpch {

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                            const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(table_oid);
  // Scan the table
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto l_returnflag = expr_maker.CVE(l_schema.GetColumn("l_returnflag").Oid(), type::TypeId::VARCHAR);
    auto l_linestatus = expr_maker.CVE(l_schema.GetColumn("l_linestatus").Oid(), type::TypeId::VARCHAR);
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumn("l_extendedprice").Oid(), type::TypeId::DECIMAL);
    auto l_discount = expr_maker.CVE(l_schema.GetColumn("l_discount").Oid(), type::TypeId::DECIMAL);
    auto l_tax = expr_maker.CVE(l_schema.GetColumn("l_tax").Oid(), type::TypeId::DECIMAL);
    auto l_quantity = expr_maker.CVE(l_schema.GetColumn("l_quantity").Oid(), type::TypeId::DECIMAL);
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumn("l_shipdate").Oid(), type::TypeId::DATE);
    std::vector<catalog::col_oid_t> col_oids = {
        l_schema.GetColumn("l_returnflag").Oid(),    l_schema.GetColumn("l_linestatus").Oid(),
        l_schema.GetColumn("l_extendedprice").Oid(), l_schema.GetColumn("l_discount").Oid(),
        l_schema.GetColumn("l_tax").Oid(),           l_schema.GetColumn("l_quantity").Oid(),
        l_schema.GetColumn("l_shipdate").Oid()};
    // Make the output schema
    l_seq_scan_out.AddOutput("l_returnflag", l_returnflag);
    l_seq_scan_out.AddOutput("l_linestatus", l_linestatus);
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_tax", l_tax);
    l_seq_scan_out.AddOutput("l_quantity", l_quantity);
    auto schema = l_seq_scan_out.MakeSchema();
    // Make the predicate
    l_seq_scan_out.AddOutput("l_shipdate", l_shipdate);
    auto date_const = expr_maker.Constant(1998, 9, 2);
    auto predicate = expr_maker.ComparisonLt(l_shipdate, date_const);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(table_oid)
                     .SetColumnOids(std::move(col_oids))
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto l_returnflag = l_seq_scan_out.GetOutput("l_returnflag");
    auto l_linestatus = l_seq_scan_out.GetOutput("l_linestatus");
    auto l_quantity = l_seq_scan_out.GetOutput("l_quantity");
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_tax = l_seq_scan_out.GetOutput("l_tax");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    auto sum_base_price = expr_maker.AggSum(l_extendedprice);
    auto one_const = expr_maker.Constant(1.0f);
    auto disc_price = expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount));
    auto sum_disc_price = expr_maker.AggSum(disc_price);
    auto charge = expr_maker.OpMul(disc_price, expr_maker.OpSum(one_const, l_tax));
    auto sum_charge = expr_maker.AggSum(charge);
    auto avg_qty = expr_maker.AggAvg(l_quantity);
    auto avg_price = expr_maker.AggAvg(l_extendedprice);
    auto avg_disc = expr_maker.AggAvg(l_discount);
    auto count_order = expr_maker.AggCount(expr_maker.Constant(1));  // Works as Count(*)
    // Add them to the helper.
    agg_out.AddGroupByTerm("l_returnflag", l_returnflag);
    agg_out.AddGroupByTerm("l_linestatus", l_linestatus);
    agg_out.AddAggTerm("sum_qty", sum_qty);
    agg_out.AddAggTerm("sum_base_price", sum_base_price);
    agg_out.AddAggTerm("sum_disc_price", sum_disc_price);
    agg_out.AddAggTerm("sum_charge", sum_charge);
    agg_out.AddAggTerm("avg_qty", avg_qty);
    agg_out.AddAggTerm("avg_price", avg_price);
    agg_out.AddAggTerm("avg_disc", avg_disc);
    agg_out.AddAggTerm("count_order", count_order);
    // Make the output schema
    agg_out.AddOutput("l_returnflag", agg_out.GetGroupByTermForOutput("l_returnflag"));
    agg_out.AddOutput("l_linestatus", agg_out.GetGroupByTermForOutput("l_linestatus"));
    agg_out.AddOutput("sum_qty", agg_out.GetAggTermForOutput("sum_qty"));
    agg_out.AddOutput("sum_base_price", agg_out.GetAggTermForOutput("sum_base_price"));
    agg_out.AddOutput("sum_disc_price", agg_out.GetAggTermForOutput("sum_disc_price"));
    agg_out.AddOutput("sum_charge", agg_out.GetAggTermForOutput("sum_charge"));
    agg_out.AddOutput("avg_qty", agg_out.GetAggTermForOutput("avg_qty"));
    agg_out.AddOutput("avg_price", agg_out.GetAggTermForOutput("avg_price"));
    agg_out.AddOutput("avg_disc", agg_out.GetAggTermForOutput("avg_disc"));
    agg_out.AddOutput("count_order", agg_out.GetAggTermForOutput("count_order"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(l_returnflag)
              .AddGroupByTerm(l_linestatus)
              .AddAggregateTerm(sum_qty)
              .AddAggregateTerm(sum_base_price)
              .AddAggregateTerm(sum_disc_price)
              .AddAggregateTerm(sum_charge)
              .AddAggregateTerm(avg_qty)
              .AddAggregateTerm(avg_price)
              .AddAggregateTerm(avg_disc)
              .AddAggregateTerm(count_order)
              .AddChild(std::move(l_seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Output Colums col1, col2, col1 + col2
    auto l_returnflag = agg_out.GetOutput("l_returnflag");
    auto l_linestatus = agg_out.GetOutput("l_linestatus");
    auto sum_qty = agg_out.GetOutput("sum_qty");
    auto sum_base_price = agg_out.GetOutput("sum_base_price");
    auto sum_disc_price = agg_out.GetOutput("sum_disc_price");
    auto sum_charge = agg_out.GetOutput("sum_charge");
    auto avg_qty = agg_out.GetOutput("avg_qty");
    auto avg_price = agg_out.GetOutput("avg_price");
    auto avg_disc = agg_out.GetOutput("avg_disc");
    auto count_order = agg_out.GetOutput("count_order");
    order_by_out.AddOutput("l_returnflag", l_returnflag);
    order_by_out.AddOutput("l_linestatus", l_linestatus);
    order_by_out.AddOutput("sum_qty", sum_qty);
    order_by_out.AddOutput("sum_base_price", sum_base_price);
    order_by_out.AddOutput("sum_disc_price", sum_disc_price);
    order_by_out.AddOutput("sum_charge", sum_charge);
    order_by_out.AddOutput("avg_qty", avg_qty);
    order_by_out.AddOutput("avg_price", avg_price);
    order_by_out.AddOutput("avg_disc", avg_disc);
    order_by_out.AddOutput("count_order", count_order);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{l_returnflag, optimizer::OrderByOrderingType::ASC};
    planner::SortKey clause2{l_linestatus, optimizer::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .Build();
  }
  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ4(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                            const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Orders.
  auto o_table_oid = accessor->GetTableOid("orders");
  const auto &o_schema = accessor->GetSchema(o_table_oid);
  // Lineitem.
  auto l_table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(l_table_oid);

  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  execution::compiler::test::OutputSchemaHelper o_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumn("o_orderkey").Oid(), type::TypeId::INTEGER);
    auto o_orderpriority = expr_maker.CVE(o_schema.GetColumn("o_orderpriority").Oid(), type::TypeId::VARCHAR);
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumn("o_orderdate").Oid(), type::TypeId::DATE);
    std::vector<catalog::col_oid_t> col_oids = {o_schema.GetColumn("o_orderkey").Oid(),
                                                o_schema.GetColumn("o_orderpriority").Oid(),
                                                o_schema.GetColumn("o_orderdate").Oid()};
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_orderpriority", o_orderpriority);
    auto schema = o_seq_scan_out.MakeSchema();
    // Make predicate
    auto lo_date = expr_maker.Constant(1993, 7, 1);
    auto hi_date = expr_maker.Constant(1993, 10, 1);
    auto lo_comp = expr_maker.ComparisonGe(o_orderdate, lo_date);
    auto hi_comp = expr_maker.ComparisonLt(o_orderdate, hi_date);
    auto predicate = expr_maker.ConjunctionAnd(lo_comp, hi_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(o_table_oid)
                     .SetColumnOids(std::move(col_oids))
                     .Build();
  }
  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumn("l_orderkey").Oid(), type::TypeId::INTEGER);
    auto l_commitdate = expr_maker.CVE(l_schema.GetColumn("l_commitdate").Oid(), type::TypeId::DATE);
    auto l_receiptdate = expr_maker.CVE(l_schema.GetColumn("l_receiptdate").Oid(), type::TypeId::DATE);
    std::vector<catalog::col_oid_t> col_oids = {l_schema.GetColumn("l_orderkey").Oid(),
                                                l_schema.GetColumn("l_commitdate").Oid(),
                                                l_schema.GetColumn("l_receiptdate").Oid()};
    // Make the output schema
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    auto schema = l_seq_scan_out.MakeSchema();
    auto predicate = expr_maker.ComparisonLt(l_commitdate, l_receiptdate);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table_oid)
                     .SetColumnOids(std::move(col_oids))
                     .Build();
  }
  // Semi Join
  std::unique_ptr<planner::AbstractPlanNode> semi_join;
  execution::compiler::test::OutputSchemaHelper semi_join_out{0, &expr_maker};
  {
    // Read all needed columns
    // Left
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_orderpriority = o_seq_scan_out.GetOutput("o_orderpriority");
    // Right
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    // Make output schema
    semi_join_out.AddOutput("o_orderpriority", o_orderpriority);
    auto schema = semi_join_out.MakeSchema();
    auto predicate = expr_maker.ComparisonEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    semi_join = builder.SetOutputSchema(std::move(schema))
                    .SetJoinPredicate(predicate)
                    .AddChild(std::move(o_seq_scan))
                    .AddChild(std::move(l_seq_scan))
                    .AddLeftHashKey(o_orderkey)
                    .AddRightHashKey(l_orderkey)
                    .SetJoinType(planner::LogicalJoinType::LEFT_SEMI)
                    .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto o_orderpriority = semi_join_out.GetOutput("o_orderpriority");
    // Make the aggregate expressions
    auto one_const = expr_maker.Constant(1);
    auto order_count = expr_maker.AggCount(one_const);
    // Add them to the helper.
    agg_out.AddGroupByTerm("o_orderpriority", o_orderpriority);
    agg_out.AddAggTerm("order_count", order_count);
    // Make the output schema
    agg_out.AddOutput("o_orderpriority", agg_out.GetGroupByTermForOutput("o_orderpriority"));
    agg_out.AddOutput("order_count", agg_out.GetAggTermForOutput("order_count"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(o_orderpriority)
              .AddAggregateTerm(order_count)
              .AddChild(std::move(semi_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    auto o_orderpriority = agg_out.GetOutput("o_orderpriority");
    auto order_count = agg_out.GetOutput("order_count");
    order_by_out.AddOutput("o_orderpriority", o_orderpriority);
    order_by_out.AddOutput("order_count", order_count);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause{o_orderpriority, optimizer::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause.first, clause.second)
                   .Build();
  }

  // Compile plan

  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ5(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                            const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Region.
  auto r_table_oid = accessor->GetTableOid("region");
  const auto &r_schema = accessor->GetSchema(r_table_oid);
  // Nation.
  auto n_table_oid = accessor->GetTableOid("nation");
  const auto &n_schema = accessor->GetSchema(n_table_oid);
  // Customer.
  auto c_table_oid = accessor->GetTableOid("customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Orders.
  auto o_table_oid = accessor->GetTableOid("orders");
  const auto &o_schema = accessor->GetSchema(o_table_oid);
  // Lineitem.
  auto l_table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(l_table_oid);
  // Supplier.
  auto s_table_oid = accessor->GetTableOid("supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // Scan region
  std::unique_ptr<planner::AbstractPlanNode> r_seq_scan;
  execution::compiler::test::OutputSchemaHelper r_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto r_name = expr_maker.CVE(r_schema.GetColumn("r_name").Oid(), type::TypeId::VARCHAR);
    auto r_regionkey = expr_maker.CVE(r_schema.GetColumn("r_regionkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> r_col_oids = {r_schema.GetColumn("r_name").Oid(),
                                                  r_schema.GetColumn("r_regionkey").Oid()};
    // Make the output schema
    r_seq_scan_out.AddOutput("r_regionkey", r_regionkey);
    auto schema = r_seq_scan_out.MakeSchema();
    // Make the predicate
    auto asia = expr_maker.Constant("ASIA");
    auto predicate = expr_maker.ComparisonEq(r_name, asia);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    r_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(r_table_oid)
                     .SetColumnOids(std::move(r_col_oids))
                     .Build();
  }
  // Scan nation
  std::unique_ptr<planner::AbstractPlanNode> n_seq_scan;
  execution::compiler::test::OutputSchemaHelper n_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto n_name = expr_maker.CVE(n_schema.GetColumn("n_name").Oid(), type::TypeId::VARCHAR);
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumn("n_nationkey").Oid(), type::TypeId::INTEGER);
    auto n_regionkey = expr_maker.CVE(n_schema.GetColumn("n_regionkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> n_col_oids = {n_schema.GetColumn("n_name").Oid(),
                                                  n_schema.GetColumn("n_nationkey").Oid(),
                                                  n_schema.GetColumn("n_regionkey").Oid()};
    // Make the output schema
    n_seq_scan_out.AddOutput("n_name", n_name);
    n_seq_scan_out.AddOutput("n_nationkey", n_nationkey);
    n_seq_scan_out.AddOutput("n_regionkey", n_regionkey);
    auto schema = n_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(n_table_oid)
                     .SetColumnOids(std::move(n_col_oids))
                     .Build();
  }
  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_nationkey = expr_maker.CVE(c_schema.GetColumn("c_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> c_col_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                                  c_schema.GetColumn("c_nationkey").Oid()};
    // Make the output schema
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nationkey", c_nationkey);
    auto schema = c_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    c_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_col_oids))
                     .Build();
  }
  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  execution::compiler::test::OutputSchemaHelper o_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumn("o_orderkey").Oid(), type::TypeId::INTEGER);
    auto o_custkey = expr_maker.CVE(o_schema.GetColumn("o_custkey").Oid(), type::TypeId::INTEGER);
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumn("o_orderdate").Oid(), type::TypeId::DATE);
    std::vector<catalog::col_oid_t> o_col_oids = {o_schema.GetColumn("o_orderkey").Oid(),
                                                  o_schema.GetColumn("o_custkey").Oid(),
                                                  o_schema.GetColumn("o_orderdate").Oid()};
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    auto schema = o_seq_scan_out.MakeSchema();
    // Make predicate
    auto lo_date = expr_maker.Constant(1994, 1, 1);
    auto hi_date = expr_maker.Constant(1995, 1, 1);
    auto lo_comp = expr_maker.ComparisonGe(o_orderdate, lo_date);
    auto hi_comp = expr_maker.ComparisonLe(o_orderdate, hi_date);
    auto predicate = expr_maker.ConjunctionAnd(lo_comp, hi_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(o_table_oid)
                     .SetColumnOids(std::move(o_col_oids))
                     .Build();
  }
  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumn("l_extendedprice").Oid(), type::TypeId::DECIMAL);
    auto l_discount = expr_maker.CVE(l_schema.GetColumn("l_discount").Oid(), type::TypeId::DECIMAL);
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumn("l_orderkey").Oid(), type::TypeId::INTEGER);
    auto l_suppkey = expr_maker.CVE(l_schema.GetColumn("l_suppkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> l_col_oids = {
        l_schema.GetColumn("l_extendedprice").Oid(), l_schema.GetColumn("l_discount").Oid(),
        l_schema.GetColumn("l_orderkey").Oid(), l_schema.GetColumn("l_suppkey").Oid()};
    // Make the output schema
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_suppkey", l_suppkey);
    auto schema = l_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(l_table_oid)
                     .SetColumnOids(std::move(l_col_oids))
                     .Build();
  }
  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumn("s_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> s_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                  s_schema.GetColumn("s_nationkey").Oid()};
    // Make the output schema
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_col_oids))
                     .Build();
  }
  // Make first hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{0, &expr_maker};
  {
    // Left columns
    auto r_regionkey = r_seq_scan_out.GetOutput("r_regionkey");
    // Right columns
    auto n_name = n_seq_scan_out.GetOutput("n_name");
    auto n_nationkey = n_seq_scan_out.GetOutput("n_nationkey");
    auto n_regionkey = n_seq_scan_out.GetOutput("n_regionkey");
    // Output Schema
    hash_join_out1.AddOutput("n_nationkey", n_nationkey);
    hash_join_out1.AddOutput("n_name", n_name);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(r_regionkey, n_regionkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(r_seq_scan))
                     .AddChild(std::move(n_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(r_regionkey)
                     .AddRightHashKey(n_regionkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns
    auto n_nationkey = hash_join_out1.GetOutput("n_nationkey");
    auto n_name = hash_join_out1.GetOutput("n_name");
    // Right columns
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nationkey = c_seq_scan_out.GetOutput("c_nationkey");
    // Output Schema
    hash_join_out2.AddOutput("n_nationkey", n_nationkey);
    hash_join_out2.AddOutput("n_name", n_name);
    hash_join_out2.AddOutput("c_custkey", c_custkey);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(n_nationkey, c_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(hash_join1))
                     .AddChild(std::move(c_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(n_nationkey)
                     .AddRightHashKey(c_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make third hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns
    auto n_nationkey = hash_join_out2.GetOutput("n_nationkey");
    auto n_name = hash_join_out2.GetOutput("n_name");
    auto c_custkey = hash_join_out2.GetOutput("c_custkey");
    // Right columns
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    // Output Schema
    hash_join_out3.AddOutput("n_nationkey", n_nationkey);
    hash_join_out3.AddOutput("n_name", n_name);
    hash_join_out3.AddOutput("o_orderkey", o_orderkey);
    auto schema = hash_join_out3.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(c_custkey, o_custkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make fourth hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  execution::compiler::test::OutputSchemaHelper hash_join_out4{1, &expr_maker};
  {
    // Left columns
    auto n_nationkey = hash_join_out3.GetOutput("n_nationkey");
    auto n_name = hash_join_out3.GetOutput("n_name");
    auto o_orderkey = hash_join_out3.GetOutput("o_orderkey");
    // Right columns
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_suppkey = l_seq_scan_out.GetOutput("l_suppkey");
    // Output Schema
    hash_join_out4.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out4.AddOutput("l_discount", l_discount);
    hash_join_out4.AddOutput("l_suppkey", l_suppkey);
    hash_join_out4.AddOutput("n_nationkey", n_nationkey);
    hash_join_out4.AddOutput("n_name", n_name);
    auto schema = hash_join_out4.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make fifth hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join5;
  execution::compiler::test::OutputSchemaHelper hash_join_out5{0, &expr_maker};
  {
    // Left columns
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out.GetOutput("s_nationkey");
    // Right columns
    auto n_name = hash_join_out4.GetOutput("n_name");
    auto n_nationkey = hash_join_out4.GetOutput("n_nationkey");
    auto l_extendedprice = hash_join_out4.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out4.GetOutput("l_discount");
    auto l_suppkey = hash_join_out4.GetOutput("l_suppkey");
    // Output Schema
    hash_join_out5.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out5.AddOutput("l_discount", l_discount);
    hash_join_out5.AddOutput("n_name", n_name);
    auto schema = hash_join_out5.MakeSchema();
    // Predicate
    auto comp1 = expr_maker.ComparisonEq(n_nationkey, s_nationkey);
    auto comp2 = expr_maker.ComparisonEq(l_suppkey, s_suppkey);
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join5 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join4))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(s_nationkey)
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(n_nationkey)
                     .AddRightHashKey(l_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto l_extendedprice = hash_join_out5.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out5.GetOutput("l_discount");
    auto n_name = hash_join_out5.GetOutput("n_name");
    // Make the aggregate expressions
    auto one_const = expr_maker.Constant(1.0f);
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount)));
    // Add them to the helper.
    agg_out.AddGroupByTerm("n_name", n_name);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("n_name", agg_out.GetGroupByTermForOutput("n_name"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(n_name)
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join5))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Output Colums col1, col2, col1 + col2
    auto n_name = agg_out.GetOutput("n_name");
    auto revenue = agg_out.GetOutput("revenue");
    order_by_out.AddOutput("n_name", n_name);
    order_by_out.AddOutput("revenue", revenue);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause{revenue, optimizer::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause.first, clause.second)
                   .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ6(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                            const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Lineitem.
  auto l_table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(l_table_oid);
  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumn("l_extendedprice").Oid(), type::TypeId::DECIMAL);
    auto l_discount = expr_maker.CVE(l_schema.GetColumn("l_discount").Oid(), type::TypeId::DECIMAL);
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumn("l_shipdate").Oid(), type::TypeId::DATE);
    auto l_quantity = expr_maker.CVE(l_schema.GetColumn("l_quantity").Oid(), type::TypeId::DECIMAL);
    std::vector<catalog::col_oid_t> l_col_oids = {
        l_schema.GetColumn("l_extendedprice").Oid(), l_schema.GetColumn("l_discount").Oid(),
        l_schema.GetColumn("l_shipdate").Oid(), l_schema.GetColumn("l_quantity").Oid()};
    // Make the output schema
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    auto schema = l_seq_scan_out.MakeSchema();
    // Predicate
    auto lo_date = expr_maker.Constant(1994, 1, 1);
    auto hi_date = expr_maker.Constant(1995, 1, 1);
    auto lo_discount = expr_maker.Constant(0.05);
    auto hi_discount = expr_maker.Constant(0.07);
    auto lo_qty = expr_maker.Constant(24.0);
    auto lo_date_comp = expr_maker.ComparisonGe(l_shipdate, lo_date);
    auto hi_date_comp = expr_maker.ComparisonLt(l_shipdate, hi_date);
    auto lo_discount_comp = expr_maker.ComparisonGe(l_discount, lo_discount);
    auto hi_discount_comp = expr_maker.ComparisonLe(l_discount, hi_discount);
    auto lo_qty_comp = expr_maker.ComparisonLt(l_quantity, lo_qty);
    auto date_comp = expr_maker.ConjunctionAnd(lo_date_comp, hi_date_comp);
    auto discount_comp = expr_maker.ConjunctionAnd(lo_discount_comp, hi_discount_comp);
    auto predicate = expr_maker.ConjunctionAnd(date_comp, expr_maker.ConjunctionAnd(discount_comp, lo_qty_comp));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table_oid)
                     .SetColumnOids(std::move(l_col_oids))
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    // Make the aggregate expressions
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(l_extendedprice, l_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(revenue)
              .AddChild(std::move(l_seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*agg, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(agg));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ7(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                            const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Nation.
  auto n_table_oid = accessor->GetTableOid("nation");
  const auto &n_schema = accessor->GetSchema(n_table_oid);
  // Customer.
  auto c_table_oid = accessor->GetTableOid("customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Orders.
  auto o_table_oid = accessor->GetTableOid("orders");
  const auto &o_schema = accessor->GetSchema(o_table_oid);
  // Lineitem.
  auto l_table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(l_table_oid);
  // Supplier.
  auto s_table_oid = accessor->GetTableOid("supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // Scan nation1
  std::unique_ptr<planner::AbstractPlanNode> n1_seq_scan;
  execution::compiler::test::OutputSchemaHelper n1_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto n1_name = expr_maker.CVE(n_schema.GetColumn("n_name").Oid(), type::TypeId::VARCHAR);
    auto n1_nationkey = expr_maker.CVE(n_schema.GetColumn("n_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> n1_col_oids = {n_schema.GetColumn("n_name").Oid(),
                                                   n_schema.GetColumn("n_nationkey").Oid()};
    // Make the output schema
    n1_seq_scan_out.AddOutput("n1_name", n1_name);
    n1_seq_scan_out.AddOutput("n1_nationkey", n1_nationkey);
    // Predicate
    auto schema = n1_seq_scan_out.MakeSchema();
    auto france_comp = expr_maker.ComparisonEq(n1_name, expr_maker.Constant("FRANCE"));
    auto germany_comp = expr_maker.ComparisonEq(n1_name, expr_maker.Constant("GERMANY"));
    auto predicate = expr_maker.ConjunctionOr(france_comp, germany_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n1_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table_oid)
                      .SetColumnOids(std::move(n1_col_oids))
                      .Build();
  }

  // Scan nation2
  std::unique_ptr<planner::AbstractPlanNode> n2_seq_scan;
  execution::compiler::test::OutputSchemaHelper n2_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto n2_name = expr_maker.CVE(n_schema.GetColumn("n_name").Oid(), type::TypeId::VARCHAR);
    auto n2_nationkey = expr_maker.CVE(n_schema.GetColumn("n_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> n2_col_oids = {n_schema.GetColumn("n_name").Oid(),
                                                   n_schema.GetColumn("n_nationkey").Oid()};
    // Make the output schema
    n2_seq_scan_out.AddOutput("n2_name", n2_name);
    n2_seq_scan_out.AddOutput("n2_nationkey", n2_nationkey);
    auto schema = n2_seq_scan_out.MakeSchema();
    // Predicate
    auto france_comp = expr_maker.ComparisonEq(n2_name, expr_maker.Constant("FRANCE"));
    auto germany_comp = expr_maker.ComparisonEq(n2_name, expr_maker.Constant("GERMANY"));
    auto predicate = expr_maker.ConjunctionOr(france_comp, germany_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n2_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table_oid)
                      .SetColumnOids(std::move(n2_col_oids))
                      .Build();
  }

  // BNL
  std::unique_ptr<planner::AbstractPlanNode> bnl;
  execution::compiler::test::OutputSchemaHelper bnl_out{0, &expr_maker};
  {
    // Left
    auto n1_name = n1_seq_scan_out.GetOutput("n1_name");
    auto n1_nationkey = n1_seq_scan_out.GetOutput("n1_nationkey");
    // Right
    auto n2_name = n2_seq_scan_out.GetOutput("n2_name");
    auto n2_nationkey = n2_seq_scan_out.GetOutput("n2_nationkey");
    // Make the output schema
    bnl_out.AddOutput("n1_name", n1_name);
    bnl_out.AddOutput("n2_name", n2_name);
    bnl_out.AddOutput("n1_nationkey", n1_nationkey);
    bnl_out.AddOutput("n2_nationkey", n2_nationkey);
    auto schema = bnl_out.MakeSchema();
    // Predicate
    auto france_comp1 = expr_maker.ComparisonEq(n1_name, expr_maker.Constant("FRANCE"));
    auto france_comp2 = expr_maker.ComparisonEq(n2_name, expr_maker.Constant("FRANCE"));
    auto germany_comp1 = expr_maker.ComparisonEq(n1_name, expr_maker.Constant("GERMANY"));
    auto germany_comp2 = expr_maker.ComparisonEq(n2_name, expr_maker.Constant("GERMANY"));
    auto eq1 = expr_maker.ConjunctionAnd(france_comp1, germany_comp2);
    auto eq2 = expr_maker.ConjunctionAnd(france_comp2, germany_comp1);
    auto predicate = expr_maker.ConjunctionOr(eq1, eq2);
    // Build
    planner::NestedLoopJoinPlanNode::Builder builder;
    bnl = builder.AddChild(std::move(n1_seq_scan))
              .AddChild(std::move(n2_seq_scan))
              .SetOutputSchema(std::move(schema))
              .SetJoinType(planner::LogicalJoinType::INNER)
              .SetJoinPredicate(predicate)
              .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_nationkey = expr_maker.CVE(c_schema.GetColumn("c_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> c_col_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                                  c_schema.GetColumn("c_nationkey").Oid()};
    // Make the output schema
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nationkey", c_nationkey);
    auto schema = c_seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    c_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_col_oids))
                     .Build();
  }

  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{0, &expr_maker};
  {
    // Left columns
    auto n1_name = bnl_out.GetOutput("n1_name");
    auto n2_name = bnl_out.GetOutput("n2_name");
    auto n1_nationkey = bnl_out.GetOutput("n1_nationkey");
    auto n2_nationkey = bnl_out.GetOutput("n2_nationkey");
    // Right columns
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nationkey = c_seq_scan_out.GetOutput("c_nationkey");
    // Output Schema
    hash_join_out1.AddOutput("n1_name", n1_name);
    hash_join_out1.AddOutput("n2_name", n2_name);
    hash_join_out1.AddOutput("n1_nationkey", n1_nationkey);
    hash_join_out1.AddOutput("c_custkey", c_custkey);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(n2_nationkey, c_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(bnl))
                     .AddChild(std::move(c_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(n2_nationkey)
                     .AddRightHashKey(c_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  execution::compiler::test::OutputSchemaHelper o_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumn("o_orderkey").Oid(), type::TypeId::INTEGER);
    auto o_custkey = expr_maker.CVE(o_schema.GetColumn("o_custkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> o_col_oids = {o_schema.GetColumn("o_orderkey").Oid(),
                                                  o_schema.GetColumn("o_custkey").Oid()};
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    auto schema = o_seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(o_table_oid)
                     .SetColumnOids(std::move(o_col_oids))
                     .Build();
  }

  // Make second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns
    auto n1_name = hash_join_out1.GetOutput("n1_name");
    auto n2_name = hash_join_out1.GetOutput("n2_name");
    auto n1_nationkey = hash_join_out1.GetOutput("n1_nationkey");
    auto c_custkey = hash_join_out1.GetOutput("c_custkey");
    // Right columns
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    // Output Schema
    hash_join_out2.AddOutput("n1_name", n1_name);
    hash_join_out2.AddOutput("n2_name", n2_name);
    hash_join_out2.AddOutput("n1_nationkey", n1_nationkey);
    hash_join_out2.AddOutput("o_orderkey", o_orderkey);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(c_custkey, o_custkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(hash_join1))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Scan lineitem
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumn("l_extendedprice").Oid(), type::TypeId::DECIMAL);
    auto l_discount = expr_maker.CVE(l_schema.GetColumn("l_discount").Oid(), type::TypeId::DECIMAL);
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumn("l_orderkey").Oid(), type::TypeId::INTEGER);
    auto l_suppkey = expr_maker.CVE(l_schema.GetColumn("l_suppkey").Oid(), type::TypeId::INTEGER);
    auto l_shipdate = expr_maker.CVE(l_schema.GetColumn("l_shipdate").Oid(), type::TypeId::DATE);
    std::vector<catalog::col_oid_t> l_col_oids = {
        l_schema.GetColumn("l_extendedprice").Oid(), l_schema.GetColumn("l_discount").Oid(),
        l_schema.GetColumn("l_orderkey").Oid(), l_schema.GetColumn("l_suppkey").Oid(),
        l_schema.GetColumn("l_shipdate").Oid()};
    // Make the output schema
    auto const_one = expr_maker.Constant(1.0f);
    auto volume = expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(const_one, l_discount));
    l_seq_scan_out.AddOutput("volume", volume);
    l_seq_scan_out.AddOutput("l_orderkey", l_orderkey);
    l_seq_scan_out.AddOutput("l_suppkey", l_suppkey);

    auto date_type = expr_maker.Constant(static_cast<int32_t>(execution::sql::DatePartType::YEAR));
    auto extract_year = expr_maker.Function("data_part", {l_shipdate, date_type}, type::TypeId::INTEGER,
                                            noisepage::catalog::postgres::DATE_PART_PRO_OID);

    l_seq_scan_out.AddOutput("l_year", extract_year);
    auto schema = l_seq_scan_out.MakeSchema();
    // Predicate
    auto lo_date = expr_maker.Constant(1995, 1, 1);
    auto hi_date = expr_maker.Constant(1996, 12, 31);
    auto lo_date_comp = expr_maker.ComparisonGe(l_shipdate, lo_date);
    auto hi_date_comp = expr_maker.ComparisonLt(l_shipdate, hi_date);
    auto predicate = expr_maker.ConjunctionAnd(lo_date_comp, hi_date_comp);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table_oid)
                     .SetColumnOids(std::move(l_col_oids))
                     .Build();
  }
  // make the third hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{1, &expr_maker};
  {
    // Left columns
    auto n1_name = hash_join_out2.GetOutput("n1_name");
    auto n2_name = hash_join_out2.GetOutput("n2_name");
    auto n1_nationkey = hash_join_out2.GetOutput("n1_nationkey");
    auto o_orderkey = hash_join_out2.GetOutput("o_orderkey");

    // Right columns
    auto volume = l_seq_scan_out.GetOutput("volume");
    auto l_orderkey = l_seq_scan_out.GetOutput("l_orderkey");
    auto l_suppkey = l_seq_scan_out.GetOutput("l_suppkey");
    auto l_year = l_seq_scan_out.GetOutput("l_year");
    // Output Schema
    hash_join_out3.AddOutput("n1_name", n1_name);
    hash_join_out3.AddOutput("n2_name", n2_name);
    hash_join_out3.AddOutput("n1_nationkey", n1_nationkey);
    hash_join_out3.AddOutput("volume", volume);
    hash_join_out3.AddOutput("l_suppkey", l_suppkey);
    hash_join_out3.AddOutput("l_year", l_year);
    auto schema = hash_join_out3.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumn("s_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> s_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                  s_schema.GetColumn("s_nationkey").Oid()};
    // Make the output schema
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_col_oids))
                     .Build();
  }

  // Make fourth hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  execution::compiler::test::OutputSchemaHelper hash_join_out4{0, &expr_maker};
  {
    // Left columns
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out.GetOutput("s_nationkey");
    // Right columns
    auto n1_name = hash_join_out3.GetOutput("n1_name");
    auto n2_name = hash_join_out3.GetOutput("n2_name");
    auto n1_nationkey = hash_join_out3.GetOutput("n1_nationkey");
    auto volume = hash_join_out3.GetOutput("volume");
    auto l_suppkey = hash_join_out3.GetOutput("l_suppkey");
    auto l_year = hash_join_out3.GetOutput("l_year");

    // Output Schema
    hash_join_out4.AddOutput("supp_nation", n1_name);
    hash_join_out4.AddOutput("cust_nation", n2_name);
    hash_join_out4.AddOutput("volume", volume);
    hash_join_out4.AddOutput("l_year", l_year);
    auto schema = hash_join_out4.MakeSchema();
    // Predicate
    auto comp1 = expr_maker.ComparisonEq(s_suppkey, l_suppkey);
    auto comp2 = expr_maker.ComparisonEq(s_nationkey, n1_nationkey);
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join3))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(s_suppkey)
                     .AddLeftHashKey(s_nationkey)
                     .AddRightHashKey(l_suppkey)
                     .AddRightHashKey(n1_nationkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto supp_nation = hash_join_out4.GetOutput("supp_nation");
    auto cust_nation = hash_join_out4.GetOutput("cust_nation");
    auto volume = hash_join_out4.GetOutput("volume");
    auto l_year = hash_join_out4.GetOutput("l_year");
    // Make the aggregate expressions
    auto volume_sum = expr_maker.AggSum(volume);
    // Add them to the helper.
    agg_out.AddGroupByTerm("supp_nation", supp_nation);
    agg_out.AddGroupByTerm("cust_nation", cust_nation);
    agg_out.AddGroupByTerm("l_year", l_year);
    agg_out.AddAggTerm("volume", volume_sum);
    // Make the output schema
    agg_out.AddOutput("supp_nation", agg_out.GetGroupByTermForOutput("supp_nation"));
    agg_out.AddOutput("cust_nation", agg_out.GetGroupByTermForOutput("cust_nation"));
    agg_out.AddOutput("l_year", agg_out.GetGroupByTermForOutput("l_year"));
    agg_out.AddOutput("volume", agg_out.GetAggTermForOutput("volume"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(supp_nation)
              .AddGroupByTerm(cust_nation)
              .AddGroupByTerm(l_year)
              .AddAggregateTerm(volume_sum)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Read previous layer
    auto supp_nation = agg_out.GetOutput("supp_nation");
    auto cust_nation = agg_out.GetOutput("cust_nation");
    auto l_year = agg_out.GetOutput("l_year");
    auto volume = agg_out.GetOutput("volume");

    order_by_out.AddOutput("supp_nation", supp_nation);
    order_by_out.AddOutput("cust_nation", cust_nation);
    order_by_out.AddOutput("l_year", l_year);
    order_by_out.AddOutput("volume", volume);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{supp_nation, optimizer::OrderByOrderingType::ASC};
    planner::SortKey clause2{cust_nation, optimizer::OrderByOrderingType::ASC};
    planner::SortKey clause3{l_year, optimizer::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .AddSortKey(clause3.first, clause3.second)
                   .Build();
  }
  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ11(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                             const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Supplier.
  auto s_table_oid = accessor->GetTableOid("supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // Partsupp.
  auto ps_table_oid = accessor->GetTableOid("partsupp");
  const auto &ps_schema = accessor->GetSchema(ps_table_oid);
  // Nation.
  auto n_table_oid = accessor->GetTableOid("nation");
  const auto &n_schema = accessor->GetSchema(n_table_oid);

  std::unique_ptr<planner::AbstractPlanNode> n_seq_scan1;
  execution::compiler::test::OutputSchemaHelper n_seq_scan_out1{0, &expr_maker};
  {
    // Read all needed columns
    auto n_name = expr_maker.CVE(n_schema.GetColumn("n_name").Oid(), type::TypeId::VARCHAR);
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumn("n_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> n1_col_oids = {n_schema.GetColumn("n_name").Oid(),
                                                   n_schema.GetColumn("n_nationkey").Oid()};
    // Make the output schema
    n_seq_scan_out1.AddOutput("n_nationkey", n_nationkey);
    auto schema = n_seq_scan_out1.MakeSchema();
    // Predicate
    auto germany = expr_maker.Constant("GERMANY");
    auto predicate = expr_maker.ComparisonEq(n_name, germany);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table_oid)
                      .SetColumnOids(std::move(n1_col_oids))
                      .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> n_seq_scan2;
  execution::compiler::test::OutputSchemaHelper n_seq_scan_out2{0, &expr_maker};
  {
    // Read all needed columns
    auto n_name = expr_maker.CVE(n_schema.GetColumn("n_name").Oid(), type::TypeId::VARCHAR);
    auto n_nationkey = expr_maker.CVE(n_schema.GetColumn("n_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> n2_col_oids = {n_schema.GetColumn("n_name").Oid(),
                                                   n_schema.GetColumn("n_nationkey").Oid()};
    // Make the output schema
    n_seq_scan_out2.AddOutput("n_nationkey", n_nationkey);
    auto schema = n_seq_scan_out2.MakeSchema();
    // Predicate
    auto germany = expr_maker.Constant("GERMANY");
    auto predicate = expr_maker.ComparisonEq(n_name, germany);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    n_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(n_table_oid)
                      .SetColumnOids(std::move(n2_col_oids))
                      .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan1;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out1{1, &expr_maker};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumn("s_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> s1_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                   s_schema.GetColumn("s_nationkey").Oid()};
    // Make the output schema
    s_seq_scan_out1.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out1.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out1.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(s_table_oid)
                      .SetColumnOids(std::move(s1_col_oids))
                      .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan2;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out2{1, &expr_maker};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_nationkey = expr_maker.CVE(s_schema.GetColumn("s_nationkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> s2_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                   s_schema.GetColumn("s_nationkey").Oid()};
    // Make the output schema
    s_seq_scan_out2.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out2.AddOutput("s_nationkey", s_nationkey);
    auto schema = s_seq_scan_out2.MakeSchema();

    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(s_table_oid)
                      .SetColumnOids(std::move(s2_col_oids))
                      .Build();
  }

  // Scan partsupp
  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan1;
  execution::compiler::test::OutputSchemaHelper ps_seq_scan_out1{1, &expr_maker};
  {
    // Read all needed columns
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumn("ps_suppkey").Oid(), type::TypeId::INTEGER);
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumn("ps_partkey").Oid(), type::TypeId::INTEGER);
    auto ps_supplycost = expr_maker.CVE(ps_schema.GetColumn("ps_supplycost").Oid(), type::TypeId::DECIMAL);
    auto ps_availqty = expr_maker.CVE(ps_schema.GetColumn("ps_availqty").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> ps_col_oids = {
        ps_schema.GetColumn("ps_suppkey").Oid(), ps_schema.GetColumn("ps_partkey").Oid(),
        ps_schema.GetColumn("ps_supplycost").Oid(), ps_schema.GetColumn("ps_availqty").Oid()};
    // Make the output schema
    ps_seq_scan_out1.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out1.AddOutput("ps_partkey", ps_partkey);
    ps_seq_scan_out1.AddOutput("ps_supplycost", ps_supplycost);
    ps_seq_scan_out1.AddOutput("ps_availqty", ps_availqty);
    auto schema = ps_seq_scan_out1.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    ps_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                       .SetScanPredicate(nullptr)
                       .SetTableOid(ps_table_oid)
                       .SetColumnOids(std::move(ps_col_oids))
                       .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan2;
  execution::compiler::test::OutputSchemaHelper ps_seq_scan_out2{1, &expr_maker};
  {
    // Read all needed columns
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumn("ps_suppkey").Oid(), type::TypeId::INTEGER);
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumn("ps_partkey").Oid(), type::TypeId::INTEGER);
    auto ps_supplycost = expr_maker.CVE(ps_schema.GetColumn("ps_supplycost").Oid(), type::TypeId::DECIMAL);
    auto ps_availqty = expr_maker.CVE(ps_schema.GetColumn("ps_availqty").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> ps2_col_oids = {
        ps_schema.GetColumn("ps_suppkey").Oid(), ps_schema.GetColumn("ps_partkey").Oid(),
        ps_schema.GetColumn("ps_supplycost").Oid(), ps_schema.GetColumn("ps_availqty").Oid()};
    // Make the output schema
    ps_seq_scan_out2.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out2.AddOutput("ps_partkey", ps_partkey);
    ps_seq_scan_out2.AddOutput("ps_supplycost", ps_supplycost);
    ps_seq_scan_out2.AddOutput("ps_availqty", ps_availqty);
    auto schema = ps_seq_scan_out2.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    ps_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                       .SetScanPredicate(nullptr)
                       .SetTableOid(ps_table_oid)
                       .SetColumnOids(std::move(ps2_col_oids))
                       .Build();
  }

  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1_1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1_1{0, &expr_maker};
  {
    // Read left columns
    auto n_nationkey = n_seq_scan_out1.GetOutput("n_nationkey");
    // Read right columns
    auto s_suppkey = s_seq_scan_out1.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out1.GetOutput("s_nationkey");
    // Make Schema
    hash_join_out1_1.AddOutput("s_suppkey", s_suppkey);
    auto schema = hash_join_out1_1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonEq(n_nationkey, s_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1_1 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(n_seq_scan1))
                       .AddChild(std::move(s_seq_scan1))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(n_nationkey)
                       .AddRightHashKey(s_nationkey)
                       .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> hash_join1_2;
  execution::compiler::test::OutputSchemaHelper hash_join_out1_2{0, &expr_maker};
  {
    // Read left columns
    auto n_nationkey = n_seq_scan_out2.GetOutput("n_nationkey");
    // Read right columns
    auto s_suppkey = s_seq_scan_out2.GetOutput("s_suppkey");
    auto s_nationkey = s_seq_scan_out2.GetOutput("s_nationkey");
    // Make Schema
    hash_join_out1_2.AddOutput("s_suppkey", s_suppkey);
    auto schema = hash_join_out1_2.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonEq(n_nationkey, s_nationkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1_2 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(n_seq_scan2))
                       .AddChild(std::move(s_seq_scan2))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(n_nationkey)
                       .AddRightHashKey(s_nationkey)
                       .Build();
  }

  // Hash Join 2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2_1;
  execution::compiler::test::OutputSchemaHelper hash_join_out2_1{0, &expr_maker};
  {
    // Read left columns
    auto s_suppkey = hash_join_out1_1.GetOutput("s_suppkey");
    // Read right columns
    auto ps_suppkey = ps_seq_scan_out1.GetOutput("ps_suppkey");
    // auto ps_partkey = ps_seq_scan_out1.GetOutput("ps_partkey");
    auto ps_supplycost = ps_seq_scan_out1.GetOutput("ps_supplycost");
    auto ps_availqty = ps_seq_scan_out1.GetOutput("ps_availqty");
    // Make Schema
    hash_join_out2_1.AddOutput("ps_supplycost", ps_supplycost);
    hash_join_out2_1.AddOutput("ps_availqty", ps_availqty);
    auto schema = hash_join_out2_1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonEq(s_suppkey, ps_suppkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2_1 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(hash_join1_1))
                       .AddChild(std::move(ps_seq_scan1))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(s_suppkey)
                       .AddRightHashKey(ps_suppkey)
                       .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> hash_join2_2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2_2{0, &expr_maker};
  {
    // Read left columns
    auto s_suppkey = hash_join_out1_2.GetOutput("s_suppkey");
    // Read right columns
    auto ps_suppkey = ps_seq_scan_out2.GetOutput("ps_suppkey");
    auto ps_partkey = ps_seq_scan_out2.GetOutput("ps_partkey");
    auto ps_supplycost = ps_seq_scan_out2.GetOutput("ps_supplycost");
    auto ps_availqty = ps_seq_scan_out2.GetOutput("ps_availqty");
    // Make Schema
    hash_join_out2_2.AddOutput("ps_supplycost", ps_supplycost);
    hash_join_out2_2.AddOutput("ps_availqty", ps_availqty);
    hash_join_out2_2.AddOutput("ps_partkey", ps_partkey);
    auto schema = hash_join_out2_2.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonEq(s_suppkey, ps_suppkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2_2 = builder.SetOutputSchema(std::move(schema))
                       .AddChild(std::move(hash_join1_2))
                       .AddChild(std::move(ps_seq_scan2))
                       .SetJoinType(planner::LogicalJoinType::INNER)
                       .SetJoinPredicate(predicate)
                       .AddLeftHashKey(s_suppkey)
                       .AddRightHashKey(ps_suppkey)
                       .Build();
  }

  // Aggregates
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg1;
  execution::compiler::test::OutputSchemaHelper agg_out1{0, &expr_maker};
  {
    // Read previous layer's output
    auto ps_supplycost = hash_join_out2_1.GetOutput("ps_supplycost");
    auto ps_availqty = hash_join_out2_1.GetOutput("ps_availqty");
    // Make the aggregate expressions
    auto value = expr_maker.OpMul(ps_supplycost, ps_availqty);
    auto value_sum = expr_maker.AggSum(value);
    // Add them to the helper.
    agg_out1.AddAggTerm("value_sum", value_sum);
    // Make the output schema
    agg_out1.AddOutput("value_sum", agg_out1.GetAggTermForOutput("value_sum"));
    auto schema = agg_out1.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg1 = builder.SetOutputSchema(std::move(schema))
               .AddAggregateTerm(value_sum)
               .AddChild(std::move(hash_join2_1))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(nullptr)
               .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> agg2;
  execution::compiler::test::OutputSchemaHelper agg_out2{1, &expr_maker};
  {
    // Read previous layer's output
    auto ps_supplycost = hash_join_out2_2.GetOutput("ps_supplycost");
    auto ps_availqty = hash_join_out2_2.GetOutput("ps_availqty");
    auto ps_partkey = hash_join_out2_2.GetOutput("ps_partkey");
    // Make the aggregate expressions
    auto value = expr_maker.OpMul(ps_supplycost, ps_availqty);
    auto value_sum = expr_maker.AggSum(value);
    // Add them to the helper.
    agg_out2.AddGroupByTerm("ps_partkey", ps_partkey);
    agg_out2.AddAggTerm("value_sum", value_sum);
    // Make the output schema
    agg_out2.AddOutput("ps_partkey", agg_out2.GetGroupByTermForOutput("ps_partkey"));
    agg_out2.AddOutput("value_sum", agg_out2.GetAggTermForOutput("value_sum"));
    auto schema = agg_out2.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg2 = builder.SetOutputSchema(std::move(schema))
               .AddGroupByTerm(ps_partkey)
               .AddAggregateTerm(value_sum)
               .AddChild(std::move(hash_join2_2))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(nullptr)
               .Build();
  }

  // BNL
  std::unique_ptr<planner::AbstractPlanNode> bnl;
  execution::compiler::test::OutputSchemaHelper bnl_out{0, &expr_maker};
  {
    // Left
    auto value_sum1 = agg_out1.GetOutput("value_sum");
    // Right
    auto ps_partkey = agg_out2.GetOutput("ps_partkey");
    auto value_sum2 = agg_out2.GetOutput("value_sum");
    // Make the output schema
    bnl_out.AddOutput("value_sum", value_sum2);
    bnl_out.AddOutput("ps_partkey", ps_partkey);
    auto schema = bnl_out.MakeSchema();
    // Predicate
    auto left_comp = expr_maker.OpMul(value_sum1, expr_maker.Constant(0.00001f));
    auto predicate = expr_maker.ComparisonGt(value_sum2, left_comp);
    // Build
    planner::NestedLoopJoinPlanNode::Builder builder;
    bnl = builder.AddChild(std::move(agg1))
              .AddChild(std::move(agg2))
              .SetOutputSchema(std::move(schema))
              .SetJoinType(planner::LogicalJoinType::INNER)
              .SetJoinPredicate(predicate)
              .Build();
  }

  // Sort
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Read previous layer
    auto ps_partkey = bnl_out.GetOutput("ps_partkey");
    auto value_sum = bnl_out.GetOutput("value_sum");

    order_by_out.AddOutput("ps_partkey", ps_partkey);
    order_by_out.AddOutput("value_sum", value_sum);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{value_sum, optimizer::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(bnl))
                   .AddSortKey(clause1.first, clause1.second)
                   .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ16(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                             const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Part.
  auto p_table_oid = accessor->GetTableOid("part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Partsupp.
  auto ps_table_oid = accessor->GetTableOid("partsupp");
  const auto &ps_schema = accessor->GetSchema(ps_table_oid);
  // Supplier.
  auto s_table_oid = accessor->GetTableOid("supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto p_brand = expr_maker.CVE(p_schema.GetColumn("p_brand").Oid(), type::TypeId::VARCHAR);
    auto p_type = expr_maker.CVE(p_schema.GetColumn("p_type").Oid(), type::TypeId::VARCHAR);
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_size = expr_maker.CVE(p_schema.GetColumn("p_size").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> p_col_oids = {
        p_schema.GetColumn("p_brand").Oid(), p_schema.GetColumn("p_type").Oid(), p_schema.GetColumn("p_partkey").Oid(),
        p_schema.GetColumn("p_size").Oid()};
    // Make the output schema
    p_seq_scan_out.AddOutput("p_brand", p_brand);
    p_seq_scan_out.AddOutput("p_type", p_type);
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_size", p_size);
    auto schema = p_seq_scan_out.MakeSchema();

    // Predicate
    auto brand_comp = expr_maker.ComparisonNeq(p_brand, expr_maker.Constant("Brand#45"));
    auto type_comp = expr_maker.ComparisonNotLike(p_type, expr_maker.Constant("MEDIUM POLISHED%"));
    auto size_comp = expr_maker.ConjunctionOr(
        expr_maker.ComparisonEq(p_size, expr_maker.Constant(49)),
        expr_maker.ConjunctionOr(
            expr_maker.ComparisonEq(p_size, expr_maker.Constant(14)),
            expr_maker.ConjunctionOr(
                expr_maker.ComparisonEq(p_size, expr_maker.Constant(23)),
                expr_maker.ConjunctionOr(
                    expr_maker.ComparisonEq(p_size, expr_maker.Constant(45)),
                    expr_maker.ConjunctionOr(
                        expr_maker.ComparisonEq(p_size, expr_maker.Constant(19)),
                        expr_maker.ConjunctionOr(
                            expr_maker.ComparisonEq(p_size, expr_maker.Constant(3)),
                            expr_maker.ConjunctionOr(expr_maker.ComparisonEq(p_size, expr_maker.Constant(36)),
                                                     expr_maker.ComparisonEq(p_size, expr_maker.Constant(9)))))))));
    auto predicate = expr_maker.ConjunctionAnd(brand_comp, expr_maker.ConjunctionAnd(type_comp, size_comp));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    p_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_col_oids))
                     .Build();
  }

  //  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_comment = expr_maker.CVE(s_schema.GetColumn("s_comment").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                  s_schema.GetColumn("s_comment").Oid()};
    // Make the output schema
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    auto schema = s_seq_scan_out.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonLike(s_comment, expr_maker.Constant("%Customer%Complaints%"));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    s_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_col_oids))
                     .Build();
  }

  // Scan partsupp
  std::unique_ptr<planner::AbstractPlanNode> ps_seq_scan;
  execution::compiler::test::OutputSchemaHelper ps_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto ps_suppkey = expr_maker.CVE(ps_schema.GetColumn("ps_suppkey").Oid(), type::TypeId::INTEGER);
    auto ps_partkey = expr_maker.CVE(ps_schema.GetColumn("ps_partkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> ps_col_oids = {ps_schema.GetColumn("ps_suppkey").Oid(),
                                                   ps_schema.GetColumn("ps_partkey").Oid()};
    // Make the output schema
    ps_seq_scan_out.AddOutput("ps_suppkey", ps_suppkey);
    ps_seq_scan_out.AddOutput("ps_partkey", ps_partkey);
    auto schema = ps_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    ps_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(ps_table_oid)
                      .SetColumnOids(std::move(ps_col_oids))
                      .Build();
  }

  // First hash join
  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns
    auto p_brand = p_seq_scan_out.GetOutput("p_brand");
    auto p_type = p_seq_scan_out.GetOutput("p_type");
    auto p_size = p_seq_scan_out.GetOutput("p_size");
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    // Right columns
    auto ps_suppkey = ps_seq_scan_out.GetOutput("ps_suppkey");
    auto ps_partkey = ps_seq_scan_out.GetOutput("ps_partkey");
    // Output Schema
    hash_join_out1.AddOutput("p_brand", p_brand);
    hash_join_out1.AddOutput("p_type", p_type);
    hash_join_out1.AddOutput("p_size", p_size);
    hash_join_out1.AddOutput("ps_suppkey", ps_suppkey);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(p_partkey, ps_partkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(ps_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(ps_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns
    auto p_brand = hash_join_out1.GetOutput("p_brand");
    auto p_type = hash_join_out1.GetOutput("p_type");
    auto p_size = hash_join_out1.GetOutput("p_size");
    auto ps_suppkey = hash_join_out1.GetOutput("ps_suppkey");
    // Output Schema
    hash_join_out2.AddOutput("p_brand", p_brand);
    hash_join_out2.AddOutput("p_type", p_type);
    hash_join_out2.AddOutput("p_size", p_size);
    hash_join_out2.AddOutput("ps_suppkey", ps_suppkey);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(s_suppkey, ps_suppkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(ps_suppkey)
                     .SetJoinType(planner::LogicalJoinType::RIGHT_ANTI)
                     .SetJoinPredicate(predicate)
                     .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto p_brand = hash_join_out2.GetOutput("p_brand");
    auto p_type = hash_join_out2.GetOutput("p_type");
    auto p_size = hash_join_out2.GetOutput("p_size");
    auto ps_suppkey = hash_join_out2.GetOutput("ps_suppkey");
    // Make the aggregate expressions
    auto supplier_cnt = expr_maker.AggCount(ps_suppkey, true);
    // Add them to the helper.
    agg_out.AddGroupByTerm("p_brand", p_brand);
    agg_out.AddGroupByTerm("p_type", p_type);
    agg_out.AddGroupByTerm("p_size", p_size);
    agg_out.AddAggTerm("supplier_cnt", supplier_cnt);
    // Make the output schema
    agg_out.AddOutput("p_brand", agg_out.GetGroupByTermForOutput("p_brand"));
    agg_out.AddOutput("p_type", agg_out.GetGroupByTermForOutput("p_type"));
    agg_out.AddOutput("p_size", agg_out.GetGroupByTermForOutput("p_size"));
    agg_out.AddOutput("supplier_cnt", agg_out.GetAggTermForOutput("supplier_cnt"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(p_brand)
              .AddGroupByTerm(p_type)
              .AddGroupByTerm(p_size)
              .AddAggregateTerm(supplier_cnt)
              .AddChild(std::move(hash_join2))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Read previous layer
    auto p_brand = agg_out.GetOutput("p_brand");
    auto p_type = agg_out.GetOutput("p_type");
    auto p_size = agg_out.GetOutput("p_size");
    auto supplier_cnt = agg_out.GetOutput("supplier_cnt");

    order_by_out.AddOutput("p_brand", p_brand);
    order_by_out.AddOutput("p_type", p_type);
    order_by_out.AddOutput("p_size", p_size);
    order_by_out.AddOutput("supplier_cnt", supplier_cnt);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{supplier_cnt, optimizer::OrderByOrderingType::DESC};
    planner::SortKey clause2{p_brand, optimizer::OrderByOrderingType::ASC};
    planner::SortKey clause3{p_type, optimizer::OrderByOrderingType::ASC};
    planner::SortKey clause4{p_size, optimizer::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .AddSortKey(clause3.first, clause3.second)
                   .AddSortKey(clause4.first, clause4.second)
                   .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ18(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                             const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Customer.
  auto c_table_oid = accessor->GetTableOid("customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Orders.
  auto o_table_oid = accessor->GetTableOid("orders");
  const auto &o_schema = accessor->GetSchema(o_table_oid);
  // Lineitem.
  auto l_table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(l_table_oid);
  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_name = expr_maker.CVE(c_schema.GetColumn("c_name").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_col_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                                  c_schema.GetColumn("c_name").Oid()};
    // Make the output schema
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_name", c_name);
    auto schema = c_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    c_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_col_oids))
                     .Build();
  }
  // Scan orders
  std::unique_ptr<planner::AbstractPlanNode> o_seq_scan;
  execution::compiler::test::OutputSchemaHelper o_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto o_orderkey = expr_maker.CVE(o_schema.GetColumn("o_orderkey").Oid(), type::TypeId::INTEGER);
    auto o_custkey = expr_maker.CVE(o_schema.GetColumn("o_custkey").Oid(), type::TypeId::INTEGER);
    auto o_orderdate = expr_maker.CVE(o_schema.GetColumn("o_orderdate").Oid(), type::TypeId::DATE);
    auto o_totalprice = expr_maker.CVE(o_schema.GetColumn("o_totalprice").Oid(), type::TypeId::DECIMAL);
    std::vector<catalog::col_oid_t> o_col_oids = {
        o_schema.GetColumn("o_orderkey").Oid(), o_schema.GetColumn("o_custkey").Oid(),
        o_schema.GetColumn("o_orderdate").Oid(), o_schema.GetColumn("o_totalprice").Oid()};
    // Make the output schema
    o_seq_scan_out.AddOutput("o_orderkey", o_orderkey);
    o_seq_scan_out.AddOutput("o_custkey", o_custkey);
    o_seq_scan_out.AddOutput("o_orderdate", o_orderdate);
    o_seq_scan_out.AddOutput("o_totalprice", o_totalprice);
    auto schema = o_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    o_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(o_table_oid)
                     .SetColumnOids(std::move(o_col_oids))
                     .Build();
  }
  // Scan lineitem1
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan1;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out1{0, &expr_maker};
  {
    // Read all needed columns
    auto l_quantity = expr_maker.CVE(l_schema.GetColumn("l_quantity").Oid(), type::TypeId::DECIMAL);
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumn("l_orderkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> l_col_oids = {
        l_schema.GetColumn("l_quantity").Oid(),
        l_schema.GetColumn("l_orderkey").Oid(),
    };
    // Make the output schema
    l_seq_scan_out1.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out1.AddOutput("l_orderkey", l_orderkey);
    auto schema = l_seq_scan_out1.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan1 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(l_table_oid)
                      .SetColumnOids(std::move(l_col_oids))
                      .Build();
  }
  // Scan lineitem2
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan2;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out2{1, &expr_maker};
  {
    // Read all needed columns
    auto l_quantity = expr_maker.CVE(l_schema.GetColumn("l_quantity").Oid(), type::TypeId::DECIMAL);
    auto l_orderkey = expr_maker.CVE(l_schema.GetColumn("l_orderkey").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> l2_col_oids = {
        l_schema.GetColumn("l_quantity").Oid(),
        l_schema.GetColumn("l_orderkey").Oid(),
    };
    // Make the output schema
    l_seq_scan_out2.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out2.AddOutput("l_orderkey", l_orderkey);
    auto schema = l_seq_scan_out2.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan2 = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(nullptr)
                      .SetTableOid(l_table_oid)
                      .SetColumnOids(std::move(l2_col_oids))
                      .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg1;
  execution::compiler::test::OutputSchemaHelper agg_out1{0, &expr_maker};
  {
    // Read previous layer's output
    auto l_orderkey = l_seq_scan_out1.GetOutput("l_orderkey");
    auto l_quantity = l_seq_scan_out1.GetOutput("l_quantity");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    // Add them to the helper.
    agg_out1.AddGroupByTerm("l_orderkey", l_orderkey);
    agg_out1.AddAggTerm("sum_qty", sum_qty);
    // Make the output schema
    agg_out1.AddOutput("l_orderkey", agg_out1.GetGroupByTermForOutput("l_orderkey"));
    auto schema = agg_out1.MakeSchema();
    // Make having
    auto having = expr_maker.ComparisonGt(agg_out1.GetAggTermForOutput("sum_qty"), expr_maker.Constant(300.0f));
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg1 = builder.SetOutputSchema(std::move(schema))
               .AddGroupByTerm(l_orderkey)
               .AddAggregateTerm(sum_qty)
               .AddChild(std::move(l_seq_scan1))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(having)
               .Build();
  }
  // First hash join
  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns
    auto l_orderkey = agg_out1.GetOutput("l_orderkey");
    // Right columns
    auto o_orderkey = o_seq_scan_out.GetOutput("o_orderkey");
    auto o_orderdate = o_seq_scan_out.GetOutput("o_orderdate");
    auto o_totalprice = o_seq_scan_out.GetOutput("o_totalprice");
    auto o_custkey = o_seq_scan_out.GetOutput("o_custkey");
    // Output Schema
    hash_join_out1.AddOutput("o_orderkey", o_orderkey);
    hash_join_out1.AddOutput("o_orderdate", o_orderdate);
    hash_join_out1.AddOutput("o_totalprice", o_totalprice);
    hash_join_out1.AddOutput("o_custkey", o_custkey);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(l_orderkey, o_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(agg1))
                     .AddChild(std::move(o_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(l_orderkey)
                     .AddRightHashKey(o_orderkey)
                     .SetJoinType(planner::LogicalJoinType::RIGHT_SEMI)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_name = c_seq_scan_out.GetOutput("c_name");
    // Right columns
    auto o_orderkey = hash_join_out1.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out1.GetOutput("o_orderdate");
    auto o_totalprice = hash_join_out1.GetOutput("o_totalprice");
    auto o_custkey = hash_join_out1.GetOutput("o_custkey");
    // Output Schema
    hash_join_out2.AddOutput("c_name", c_name);
    hash_join_out2.AddOutput("c_custkey", c_custkey);
    hash_join_out2.AddOutput("o_orderkey", o_orderkey);
    hash_join_out2.AddOutput("o_orderdate", o_orderdate);
    hash_join_out2.AddOutput("o_totalprice", o_totalprice);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(c_custkey, o_custkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(o_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make third hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns
    auto c_name = hash_join_out2.GetOutput("c_name");
    auto c_custkey = hash_join_out2.GetOutput("c_custkey");
    auto o_orderkey = hash_join_out2.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out2.GetOutput("o_orderdate");
    auto o_totalprice = hash_join_out2.GetOutput("o_totalprice");
    // Right columns
    auto l_orderkey = l_seq_scan_out2.GetOutput("l_orderkey");
    auto l_quantity = l_seq_scan_out2.GetOutput("l_quantity");
    // Output Schema
    hash_join_out3.AddOutput("c_name", c_name);
    hash_join_out3.AddOutput("c_custkey", c_custkey);
    hash_join_out3.AddOutput("o_orderkey", o_orderkey);
    hash_join_out3.AddOutput("o_orderdate", o_orderdate);
    hash_join_out3.AddOutput("o_totalprice", o_totalprice);
    hash_join_out3.AddOutput("l_quantity", l_quantity);
    auto schema = hash_join_out3.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(o_orderkey, l_orderkey);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(l_seq_scan2))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(o_orderkey)
                     .AddRightHashKey(l_orderkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg2;
  execution::compiler::test::OutputSchemaHelper agg_out2{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_name = hash_join_out3.GetOutput("c_name");
    auto c_custkey = hash_join_out3.GetOutput("c_custkey");
    auto o_orderkey = hash_join_out3.GetOutput("o_orderkey");
    auto o_orderdate = hash_join_out3.GetOutput("o_orderdate");
    auto o_totalprice = hash_join_out3.GetOutput("o_totalprice");
    auto l_quantity = hash_join_out3.GetOutput("l_quantity");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    // Add them to the helper.
    agg_out2.AddGroupByTerm("c_name", c_name);
    agg_out2.AddGroupByTerm("c_custkey", c_custkey);
    agg_out2.AddGroupByTerm("o_orderkey", o_orderkey);
    agg_out2.AddGroupByTerm("o_orderdate", o_orderdate);
    agg_out2.AddGroupByTerm("o_totalprice", o_totalprice);
    agg_out2.AddAggTerm("sum_qty", sum_qty);
    // Make the output schema
    agg_out2.AddOutput("c_name", agg_out2.GetGroupByTermForOutput("c_name"));
    agg_out2.AddOutput("c_custkey", agg_out2.GetGroupByTermForOutput("c_custkey"));
    agg_out2.AddOutput("o_orderkey", agg_out2.GetGroupByTermForOutput("o_orderkey"));
    agg_out2.AddOutput("o_orderdate", agg_out2.GetGroupByTermForOutput("o_orderdate"));
    agg_out2.AddOutput("o_totalprice", agg_out2.GetGroupByTermForOutput("o_totalprice"));
    agg_out2.AddOutput("sum_qty", agg_out2.GetAggTermForOutput("sum_qty"));
    auto schema = agg_out2.MakeSchema();
    // Make having
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg2 = builder.SetOutputSchema(std::move(schema))
               .AddGroupByTerm(c_name)
               .AddGroupByTerm(c_custkey)
               .AddGroupByTerm(o_orderkey)
               .AddGroupByTerm(o_orderdate)
               .AddGroupByTerm(o_totalprice)
               .AddAggregateTerm(sum_qty)
               .AddChild(std::move(hash_join3))
               .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
               .SetHavingClausePredicate(nullptr)
               .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  execution::compiler::test::OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Read previous layer
    auto c_name = agg_out2.GetOutput("c_name");
    auto c_custkey = agg_out2.GetOutput("c_custkey");
    auto o_orderkey = agg_out2.GetOutput("o_orderkey");
    auto o_orderdate = agg_out2.GetOutput("o_orderdate");
    auto o_totalprice = agg_out2.GetOutput("o_totalprice");
    auto sum_qty = agg_out2.GetOutput("sum_qty");
    order_by_out.AddOutput("c_name", c_name);
    order_by_out.AddOutput("c_custkey", c_custkey);
    order_by_out.AddOutput("o_orderkey", o_orderkey);
    order_by_out.AddOutput("o_orderdate", o_orderdate);
    order_by_out.AddOutput("o_totalprice", o_totalprice);
    order_by_out.AddOutput("sum_qty", sum_qty);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{o_totalprice, optimizer::OrderByOrderingType::DESC};
    planner::SortKey clause2{o_orderdate, optimizer::OrderByOrderingType::ASC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(agg2))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*order_by, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(order_by));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
TPCHQuery::MakeExecutableQ19(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                             const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Lineitem.
  auto l_table_oid = accessor->GetTableOid("lineitem");
  const auto &l_schema = accessor->GetSchema(l_table_oid);
  // Part.
  auto p_table_oid = accessor->GetTableOid("part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Lineitem scan
  std::unique_ptr<planner::AbstractPlanNode> l_seq_scan;
  execution::compiler::test::OutputSchemaHelper l_seq_scan_out{1, &expr_maker};
  {
    // Read all needed columns
    auto l_extendedprice = expr_maker.CVE(l_schema.GetColumn("l_extendedprice").Oid(), type::TypeId::DECIMAL);
    auto l_discount = expr_maker.CVE(l_schema.GetColumn("l_discount").Oid(), type::TypeId::DECIMAL);
    auto l_partkey = expr_maker.CVE(l_schema.GetColumn("l_partkey").Oid(), type::TypeId::INTEGER);
    auto l_quantity = expr_maker.CVE(l_schema.GetColumn("l_quantity").Oid(), type::TypeId::DECIMAL);
    auto l_shipmode = expr_maker.CVE(l_schema.GetColumn("l_shipmode").Oid(), type::TypeId::VARCHAR);
    auto l_shipinstruct = expr_maker.CVE(l_schema.GetColumn("l_shipinstruct").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> l_col_oids = {
        l_schema.GetColumn("l_extendedprice").Oid(), l_schema.GetColumn("l_discount").Oid(),
        l_schema.GetColumn("l_partkey").Oid(),       l_schema.GetColumn("l_quantity").Oid(),
        l_schema.GetColumn("l_shipmode").Oid(),      l_schema.GetColumn("l_shipinstruct").Oid()};
    // Make the output schema
    l_seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    l_seq_scan_out.AddOutput("l_discount", l_discount);
    l_seq_scan_out.AddOutput("l_partkey", l_partkey);
    l_seq_scan_out.AddOutput("l_quantity", l_quantity);
    l_seq_scan_out.AddOutput("l_shipmode", l_shipmode);
    l_seq_scan_out.AddOutput("l_shipinstruct", l_shipinstruct);
    auto schema = l_seq_scan_out.MakeSchema();

    // Predicate.
    auto shipmode_comp = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(l_shipmode, expr_maker.Constant("AIR")),
                                                  expr_maker.ComparisonEq(l_shipmode, expr_maker.Constant("AIR REG")));
    auto shipinstruct_comp = expr_maker.ComparisonEq(l_shipinstruct, expr_maker.Constant("DELIVER IN PERSON"));
    auto predicate = expr_maker.ConjunctionAnd(shipmode_comp, shipinstruct_comp);

    // Build
    planner::SeqScanPlanNode::Builder builder;
    l_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(predicate)
                     .SetTableOid(l_table_oid)
                     .SetColumnOids(std::move(l_col_oids))
                     .Build();
  }
  // Part Scan
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto p_brand = expr_maker.CVE(p_schema.GetColumn("p_brand").Oid(), type::TypeId::VARCHAR);
    auto p_container = expr_maker.CVE(p_schema.GetColumn("p_container").Oid(), type::TypeId::VARCHAR);
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_size = expr_maker.CVE(p_schema.GetColumn("p_size").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> p_col_oids = {
        p_schema.GetColumn("p_brand").Oid(), p_schema.GetColumn("p_container").Oid(),
        p_schema.GetColumn("p_partkey").Oid(), p_schema.GetColumn("p_size").Oid()};
    // Make the output schema
    p_seq_scan_out.AddOutput("p_brand", p_brand);
    p_seq_scan_out.AddOutput("p_container", p_container);
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_size", p_size);
    auto schema = p_seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    p_seq_scan = builder.SetOutputSchema(std::move(schema))
                     .SetScanPredicate(nullptr)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_col_oids))
                     .Build();
  }
  // Hash Join 1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{0, &expr_maker};
  {
    // Left columns
    auto p_brand = p_seq_scan_out.GetOutput("p_brand");
    auto p_container = p_seq_scan_out.GetOutput("p_container");
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_size = p_seq_scan_out.GetOutput("p_size");
    // Right columns
    auto l_partkey = l_seq_scan_out.GetOutput("l_partkey");
    auto l_quantity = l_seq_scan_out.GetOutput("l_quantity");
    auto l_discount = l_seq_scan_out.GetOutput("l_discount");
    auto l_extendedprice = l_seq_scan_out.GetOutput("l_extendedprice");
    // Output Schema
    hash_join_out1.AddOutput("l_extendedprice", l_extendedprice);
    hash_join_out1.AddOutput("l_discount", l_discount);
    hash_join_out1.AddOutput("p_brand", p_brand);
    hash_join_out1.AddOutput("p_size", p_size);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate1
    execution::compiler::test::ExpressionMaker::ManagedExpression predicate1, predicate2, predicate3;
    auto gen_predicate_clause = [&](const std::string &brand, const std::vector<std::string> &sm, float lo_qty,
                                    float hi_qty, int32_t lo_size, int32_t hi_size) {
      auto brand_comp = expr_maker.ComparisonEq(p_brand, expr_maker.Constant(brand));
      auto container_comp = expr_maker.ConjunctionOr(
          expr_maker.ComparisonEq(p_container, expr_maker.Constant(sm[0])),
          expr_maker.ConjunctionOr(
              expr_maker.ComparisonEq(p_container, expr_maker.Constant(sm[1])),
              expr_maker.ConjunctionOr(expr_maker.ComparisonEq(p_container, expr_maker.Constant(sm[2])),
                                       expr_maker.ComparisonEq(p_container, expr_maker.Constant(sm[3])))));
      auto qty_lo_comp = expr_maker.ComparisonGe(l_quantity, expr_maker.Constant(lo_qty));
      auto qty_hi_comp = expr_maker.ComparisonLe(l_quantity, expr_maker.Constant(hi_qty));
      auto qty_comp = expr_maker.ConjunctionAnd(qty_lo_comp, qty_hi_comp);
      auto size_lo_comp = expr_maker.ComparisonGe(p_size, expr_maker.Constant(lo_size));
      auto size_hi_comp = expr_maker.ComparisonLe(p_size, expr_maker.Constant(hi_size));
      auto size_comp = expr_maker.ConjunctionAnd(size_lo_comp, size_hi_comp);
      return expr_maker.ConjunctionAnd(
          brand_comp, expr_maker.ConjunctionAnd(container_comp, expr_maker.ConjunctionAnd(qty_comp, size_comp)));
    };
    predicate1 = gen_predicate_clause("Brand#12", {"SM CASE", "SM BOX", "SM PACK", "SM PKG"}, 1, 11, 1, 5);
    predicate2 = gen_predicate_clause("Brand#23", {"MED BAG", "MED BOX", "MED PKG", "MED PACK"}, 10, 20, 1, 10);
    predicate3 = gen_predicate_clause("Brand#34", {"LG CASE", "LG BOX", "LG PACK", "LG PKG"}, 20, 30, 1, 15);
    auto predicate = expr_maker.ConjunctionOr(predicate1, expr_maker.ConjunctionOr(predicate2, predicate3));
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(l_seq_scan))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(l_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto l_extendedprice = hash_join_out1.GetOutput("l_extendedprice");
    auto l_discount = hash_join_out1.GetOutput("l_discount");
    // Make the aggregate expressions
    auto one_const = expr_maker.Constant(1.0f);
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(l_extendedprice, expr_maker.OpMin(one_const, l_discount)));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    auto schema = agg_out.MakeSchema();
    // Make having
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join1))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*agg, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(agg));
}

}  // namespace noisepage::tpch
