#include "test_util/ssb/star_schema_query.h"

#include "catalog/catalog_accessor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/expression_maker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/sql/sql_def.h"
#include "loggers/execution_logger.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::ssb {
std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ1Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};
    // Make the predicate: d_year=1993
    auto d_year_constant = expr_maker.Constant(1993);
    auto predicate = expr_maker.ComparisonEq(d_year, d_year_constant);
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_extendedprice = expr_maker.CVE(lo_schema.GetColumn("lo_extendedprice").Oid(), type::TypeId::INTEGER);
    auto lo_discount = expr_maker.CVE(lo_schema.GetColumn("lo_discount").Oid(), type::TypeId::INTEGER);
    auto lo_quantity = expr_maker.CVE(lo_schema.GetColumn("lo_quantity").Oid(), type::TypeId::INTEGER);

    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_quantity").Oid(), lo_schema.GetColumn("lo_discount").Oid(),
        lo_schema.GetColumn("lo_extendedprice").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make predicate: lo_discount between 1 and 3 AND lo_quantity < 25
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.ComparisonBetween(lo_discount, expr_maker.Constant(1), expr_maker.Constant(3)),
        expr_maker.ComparisonLt(lo_quantity, expr_maker.Constant(25)));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
    lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    auto schema = lo_seq_scan_out.MakeSchema();
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(std::move(schema))
                      .SetScanPredicate(predicate)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // date <-> lineorder join.
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
  execution::compiler::test::OutputSchemaHelper hash_join_out{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
    auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
    // Output Schema
    hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
    hash_join_out.AddOutput("lo_discount", lo_discount);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(d_seq_scan))
                    .AddChild(std::move(lo_seq_scan))
                    .SetOutputSchema(hash_join_out.MakeSchema())
                    .AddLeftHashKey(d_datekey)
                    .AddRightHashKey(lo_orderdate)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                    .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
    auto lo_discount = hash_join_out.GetOutput("lo_discount");
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan!
  auto query = execution::compiler::CompilationContext::Compile(*agg, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(agg));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ1Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_yearmonthnum = expr_maker.CVE(d_schema.GetColumn("d_yearmonthnum").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_yearmonthnum").Oid()};
    // Make the predicate: d_yearmonthnum = 199401
    auto predicate = expr_maker.ComparisonEq(d_yearmonthnum, expr_maker.Constant(199401));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_extendedprice = expr_maker.CVE(lo_schema.GetColumn("lo_extendedprice").Oid(), type::TypeId::INTEGER);
    auto lo_discount = expr_maker.CVE(lo_schema.GetColumn("lo_discount").Oid(), type::TypeId::INTEGER);
    auto lo_quantity = expr_maker.CVE(lo_schema.GetColumn("lo_quantity").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_quantity").Oid(), lo_schema.GetColumn("lo_discount").Oid(),
        lo_schema.GetColumn("lo_extendedprice").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make predicate: lo_discount between 4 and 6 AND lo_quantity between 26 and 35
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.ComparisonBetween(lo_discount, expr_maker.Constant(4), expr_maker.Constant(6)),
        expr_maker.ComparisonBetween(lo_quantity, expr_maker.Constant(26), expr_maker.Constant(35)));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
    lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(predicate)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // date <-> lineorder join.
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
  execution::compiler::test::OutputSchemaHelper hash_join_out{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
    auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
    // Output Schema
    hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
    hash_join_out.AddOutput("lo_discount", lo_discount);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(d_seq_scan))
                    .AddChild(std::move(lo_seq_scan))
                    .SetOutputSchema(hash_join_out.MakeSchema())
                    .AddLeftHashKey(d_datekey)
                    .AddRightHashKey(lo_orderdate)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                    .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
    auto lo_discount = hash_join_out.GetOutput("lo_discount");
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan!
  auto query = execution::compiler::CompilationContext::Compile(*agg, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(agg));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ1Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);
  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_weeknuminyear = expr_maker.CVE(d_schema.GetColumn("d_weeknuminyear").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_weeknuminyear").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};
    // Make the predicate: d_weeknuminyear = 6 AND d_year = 1994
    auto predicate = expr_maker.ConjunctionAnd(expr_maker.ComparisonEq(d_weeknuminyear, expr_maker.Constant(6)),
                                               expr_maker.ComparisonEq(d_year, expr_maker.Constant(1994)));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_extendedprice = expr_maker.CVE(lo_schema.GetColumn("lo_extendedprice").Oid(), type::TypeId::INTEGER);
    auto lo_discount = expr_maker.CVE(lo_schema.GetColumn("lo_discount").Oid(), type::TypeId::INTEGER);
    auto lo_quantity = expr_maker.CVE(lo_schema.GetColumn("lo_quantity").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_quantity").Oid(), lo_schema.GetColumn("lo_discount").Oid(),
        lo_schema.GetColumn("lo_extendedprice").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make predicate: lo_discount between 5 and 7 AND lo_quantity between 26 and 35
    auto predicate = expr_maker.ConjunctionAnd(
        expr_maker.ComparisonBetween(lo_discount, expr_maker.Constant(5), expr_maker.Constant(7)),
        expr_maker.ComparisonBetween(lo_quantity, expr_maker.Constant(26), expr_maker.Constant(35)));
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_extendedprice", lo_extendedprice);
    lo_seq_scan_out.AddOutput("lo_discount", lo_discount);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(predicate)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // date <-> lineorder join.
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
  execution::compiler::test::OutputSchemaHelper hash_join_out{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_extendedprice = lo_seq_scan_out.GetOutput("lo_extendedprice");
    auto lo_discount = lo_seq_scan_out.GetOutput("lo_discount");
    // Output Schema
    hash_join_out.AddOutput("lo_extendedprice", lo_extendedprice);
    hash_join_out.AddOutput("lo_discount", lo_discount);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(d_seq_scan))
                    .AddChild(std::move(lo_seq_scan))
                    .SetOutputSchema(hash_join_out.MakeSchema())
                    .AddLeftHashKey(d_datekey)
                    .AddRightHashKey(lo_orderdate)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                    .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto lo_extendedprice = hash_join_out.GetOutput("lo_extendedprice");
    auto lo_discount = hash_join_out.GetOutput("lo_discount");
    auto revenue = expr_maker.AggSum(expr_maker.OpMul(lo_extendedprice, lo_discount));
    // Add them to the helper.
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddChild(std::move(hash_join))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Compile plan!
  auto query = execution::compiler::CompilationContext::Compile(*agg, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(agg));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ2Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Part
  auto p_table_oid = accessor->GetTableOid("ssbm.part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{1, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumn("p_brand1").Oid(), type::TypeId::VARCHAR);
    auto p_category = expr_maker.CVE(p_schema.GetColumn("p_category").Oid(), type::TypeId::VARCHAR);

    std::vector<catalog::col_oid_t> p_oids = {p_schema.GetColumn("p_partkey").Oid(),
                                              p_schema.GetColumn("p_brand1").Oid(),
                                              p_schema.GetColumn("p_category").Oid()};
    // Make the predicate: p_category = 'MFGR#12'
    auto predicate = expr_maker.ComparisonEq(p_category, expr_maker.Constant("MFGR#12"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_region = expr_maker.CVE(s_schema.GetColumn("s_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_region").Oid()};
    // Make the predicate: s_region = 'AMERICA'
    auto predicate = expr_maker.ComparisonEq(s_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumn("lo_partkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);

    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_revenue").Oid(), lo_schema.GetColumn("lo_suppkey").Oid(),
        lo_schema.GetColumn("lo_partkey").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(p_partkey, lo_partkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ2 <-> date       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(p_brand1)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto revenue = agg_out.GetOutput("revenue");
    auto d_year = agg_out.GetOutput("d_year");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    // Setup output.
    sort_out.AddOutput("revenue", revenue);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("p_brand1", p_brand1);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, optimizer::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ2Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Part
  auto p_table_oid = accessor->GetTableOid("ssbm.part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{1, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumn("p_brand1").Oid(), type::TypeId::VARCHAR);

    std::vector<catalog::col_oid_t> p_oids = {p_schema.GetColumn("p_partkey").Oid(),
                                              p_schema.GetColumn("p_brand1").Oid()};
    // Make the predicate: p_brand1 between 'MFGR#2221' and 'MFGR#2228'
    auto predicate =
        expr_maker.ComparisonBetween(p_brand1, expr_maker.Constant("MFGR#2221"), expr_maker.Constant("MFGR#2228"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_region = expr_maker.CVE(s_schema.GetColumn("s_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_region").Oid()};
    // Make the predicate: s_region = 'ASIA'
    auto predicate = expr_maker.ComparisonEq(s_region, expr_maker.Constant("ASIA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumn("lo_partkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_revenue").Oid(), lo_schema.GetColumn("lo_suppkey").Oid(),
        lo_schema.GetColumn("lo_partkey").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(p_partkey, lo_partkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ2 <-> date       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(p_brand1)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto revenue = agg_out.GetOutput("revenue");
    auto d_year = agg_out.GetOutput("d_year");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    // Setup output.
    sort_out.AddOutput("revenue", revenue);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("p_brand1", p_brand1);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, optimizer::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ2Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Part
  auto p_table_oid = accessor->GetTableOid("ssbm.part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{1, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumn("p_brand1").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> p_oids = {p_schema.GetColumn("p_partkey").Oid(),
                                              p_schema.GetColumn("p_brand1").Oid()};
    // Make the predicate: p_brand1 = 'MFGR#2221'
    auto predicate = expr_maker.ComparisonEq(p_brand1, expr_maker.Constant("MFGR#2221"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_region = expr_maker.CVE(s_schema.GetColumn("s_region").Oid(), type::TypeId::VARCHAR);

    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_region").Oid()};
    // Make the predicate: s_region = 'EUROPE'
    auto predicate = expr_maker.ComparisonEq(s_region, expr_maker.Constant("EUROPE"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumn("lo_partkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_revenue").Oid(), lo_schema.GetColumn("lo_suppkey").Oid(),
        lo_schema.GetColumn("lo_partkey").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(p_partkey, lo_partkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ2 <-> date       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(hash_join2))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(p_brand1)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto revenue = agg_out.GetOutput("revenue");
    auto d_year = agg_out.GetOutput("d_year");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    // Setup output.
    sort_out.AddOutput("revenue", revenue);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("p_brand1", p_brand1);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, optimizer::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ3Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);

    // Make predicate: d_year >= 1992 and d_year <= 1997
    auto predicate = expr_maker.ComparisonBetween(d_year, expr_maker.Constant(1992), expr_maker.Constant(1997));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_nation = expr_maker.CVE(c_schema.GetColumn("c_nation").Oid(), type::TypeId::VARCHAR);
    auto c_region = expr_maker.CVE(c_schema.GetColumn("c_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                              c_schema.GetColumn("c_nation").Oid(),
                                              c_schema.GetColumn("c_region").Oid()};
    // Make the predicate: c_region = 'ASIA'
    auto predicate = expr_maker.ComparisonEq(c_region, expr_maker.Constant("ASIA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nation", c_nation);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_nation = expr_maker.CVE(s_schema.GetColumn("s_nation").Oid(), type::TypeId::VARCHAR);
    auto s_region = expr_maker.CVE(s_schema.GetColumn("s_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_nation").Oid(),
                                              s_schema.GetColumn("s_region").Oid()};
    // Make the predicate: s_region = 'ASIA'
    auto predicate = expr_maker.ComparisonEq(s_region, expr_maker.Constant("ASIA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nation", s_nation);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_revenue").Oid(), lo_schema.GetColumn("lo_suppkey").Oid(),
        lo_schema.GetColumn("lo_custkey").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nation = c_seq_scan_out.GetOutput("c_nation");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_nation", c_nation);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nation = s_seq_scan_out.GetOutput("s_nation");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_nation = hash_join_out1.GetOutput("c_nation");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_nation", c_nation);
    hash_join_out2.AddOutput("s_nation", s_nation);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_nation = hash_join_out2.GetOutput("c_nation");
    auto s_nation = hash_join_out2.GetOutput("s_nation");
    // Output Schema
    hash_join_out3.AddOutput("c_nation", c_nation);
    hash_join_out3.AddOutput("s_nation", s_nation);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_nation = hash_join_out3.GetOutput("c_nation");
    auto s_nation = hash_join_out3.GetOutput("s_nation");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_nation", c_nation);
    agg_out.AddGroupByTerm("s_nation", s_nation);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_nation", agg_out.GetGroupByTermForOutput("c_nation"));
    agg_out.AddOutput("s_nation", agg_out.GetGroupByTermForOutput("s_nation"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_nation)
              .AddGroupByTerm(s_nation)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_nation = agg_out.GetOutput("c_nation");
    auto s_nation = agg_out.GetOutput("s_nation");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_nation", c_nation);
    sort_out.AddOutput("s_nation", s_nation);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(revenue, optimizer::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ3Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> d_oids = {d_schema.GetColumn("d_datekey").Oid(),
                                              d_schema.GetColumn("d_year").Oid()};  // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_year >= 1992 and d_year <= 1997
    auto predicate = expr_maker.ComparisonBetween(d_year, expr_maker.Constant(1992), expr_maker.Constant(1997));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_nation = expr_maker.CVE(c_schema.GetColumn("c_nation").Oid(), type::TypeId::VARCHAR);
    auto c_city = expr_maker.CVE(c_schema.GetColumn("c_city").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                              c_schema.GetColumn("c_nation").Oid(), c_schema.GetColumn("c_city").Oid()};
    // Make the predicate: c_nation = 'UNITED STATES'
    auto predicate = expr_maker.ComparisonEq(c_nation, expr_maker.Constant("UNITED STATES"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_city", c_city);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_nation = expr_maker.CVE(s_schema.GetColumn("s_nation").Oid(), type::TypeId::VARCHAR);
    auto s_city = expr_maker.CVE(s_schema.GetColumn("s_city").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_nation").Oid(), s_schema.GetColumn("s_city").Oid()};
    // Make the predicate: s_nation = 'UNITED STATES'
    auto predicate = expr_maker.ComparisonEq(s_nation, expr_maker.Constant("UNITED STATES"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    std::vector<catalog::col_oid_t> lo_col_oids = {
        lo_schema.GetColumn("lo_revenue").Oid(), lo_schema.GetColumn("lo_suppkey").Oid(),
        lo_schema.GetColumn("lo_custkey").Oid(), lo_schema.GetColumn("lo_orderdate").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_city = c_seq_scan_out.GetOutput("c_city");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_city", c_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_city = hash_join_out1.GetOutput("c_city");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_city", c_city);
    hash_join_out2.AddOutput("s_city", s_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_city = hash_join_out2.GetOutput("c_city");
    auto s_city = hash_join_out2.GetOutput("s_city");
    // Output Schema
    hash_join_out3.AddOutput("c_city", c_city);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_city = hash_join_out3.GetOutput("c_city");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_city", c_city);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_city", agg_out.GetGroupByTermForOutput("c_city"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_city)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_city = agg_out.GetOutput("c_city");
    auto s_city = agg_out.GetOutput("s_city");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_city", c_city);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(revenue, optimizer::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ3Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector d_oids = {d_schema.GetColumn("d_datekey").Oid(), d_schema.GetColumn("d_year").Oid()};

    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_year >= 1992 and d_year <= 1997
    auto predicate = expr_maker.ComparisonBetween(d_year, expr_maker.Constant(1992), expr_maker.Constant(1997));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_city = expr_maker.CVE(c_schema.GetColumn("c_city").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                              c_schema.GetColumn("c_city").Oid()};

    // Make the predicate: c_city='UNITED KI1' or c_city='UNITED KI5'
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(c_city, expr_maker.Constant("UNITED KI1")),
                                              expr_maker.ComparisonEq(c_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_city", c_city);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_city = expr_maker.CVE(s_schema.GetColumn("s_city").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_city").Oid()};

    // Make the predicate: s_city='UNITED KI1' or s_city='UNITED KI5'
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(s_city, expr_maker.Constant("UNITED KI1")),
                                              expr_maker.ComparisonEq(s_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    std::vector lo_col_oids = {lo_schema.GetColumn("lo_orderdate").Oid(), lo_schema.GetColumn("lo_custkey").Oid(),
                               lo_schema.GetColumn("lo_suppkey").Oid(), lo_schema.GetColumn("lo_revenue").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_city = c_seq_scan_out.GetOutput("c_city");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_city", c_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_city = hash_join_out1.GetOutput("c_city");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_city", c_city);
    hash_join_out2.AddOutput("s_city", s_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_city = hash_join_out2.GetOutput("c_city");
    auto s_city = hash_join_out2.GetOutput("s_city");
    // Output Schema
    hash_join_out3.AddOutput("c_city", c_city);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_city = hash_join_out3.GetOutput("c_city");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_city", c_city);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_city", agg_out.GetGroupByTermForOutput("c_city"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_city)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_city = agg_out.GetOutput("c_city");
    auto s_city = agg_out.GetOutput("s_city");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_city", c_city);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(revenue, optimizer::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ3Part4(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{0, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    auto d_yearmonth = expr_maker.CVE(d_schema.GetColumn("d_yearmonth").Oid(), type::TypeId::VARCHAR);
    std::vector d_oids = {d_schema.GetColumn("d_datekey").Oid(), d_schema.GetColumn("d_year").Oid(),
                          d_schema.GetColumn("d_yearmonth").Oid()};
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Make predicate: d_yearmonth = 'Dec1997'
    auto predicate = expr_maker.ComparisonEq(d_yearmonth, expr_maker.Constant("Dec1997"));
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_city = expr_maker.CVE(c_schema.GetColumn("c_city").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                              c_schema.GetColumn("c_city").Oid()};

    // Make the predicate: c_city='UNITED KI1' or c_city='UNITED KI5'
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(c_city, expr_maker.Constant("UNITED KI1")),
                                              expr_maker.ComparisonEq(c_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_city", c_city);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_city = expr_maker.CVE(s_schema.GetColumn("s_city").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                              s_schema.GetColumn("s_city").Oid()};

    // Make the predicate: s_city='UNITED KI1' or s_city='UNITED KI5'
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(s_city, expr_maker.Constant("UNITED KI1")),
                                              expr_maker.ComparisonEq(s_city, expr_maker.Constant("UNITED KI5")));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    std::vector lo_col_oids = {lo_schema.GetColumn("lo_orderdate").Oid(), lo_schema.GetColumn("lo_custkey").Oid(),
                               lo_schema.GetColumn("lo_suppkey").Oid(), lo_schema.GetColumn("lo_revenue").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // customer <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_city = c_seq_scan_out.GetOutput("c_city");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("c_city", c_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto c_city = hash_join_out1.GetOutput("c_city");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("c_city", c_city);
    hash_join_out2.AddOutput("s_city", s_city);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // date <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto c_city = hash_join_out2.GetOutput("c_city");
    auto s_city = hash_join_out2.GetOutput("s_city");
    // Output Schema
    hash_join_out3.AddOutput("c_city", c_city);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("d_year", d_year);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(d_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(d_datekey)
                     .AddRightHashKey(lo_orderdate)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(d_datekey, lo_orderdate))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_city = hash_join_out3.GetOutput("c_city");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto d_year = hash_join_out3.GetOutput("d_year");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto revenue = expr_maker.AggSum(lo_revenue);
    // Add them to the helper.
    agg_out.AddGroupByTerm("c_city", c_city);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddAggTerm("revenue", revenue);
    // Make the output schema.
    agg_out.AddOutput("c_city", agg_out.GetGroupByTermForOutput("c_city"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("revenue", agg_out.GetAggTermForOutput("revenue"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddAggregateTerm(revenue)
              .AddGroupByTerm(c_city)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(d_year)
              .AddChild(std::move(hash_join3))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto c_city = agg_out.GetOutput("c_city");
    auto s_city = agg_out.GetOutput("s_city");
    auto d_year = agg_out.GetOutput("d_year");
    auto revenue = agg_out.GetOutput("revenue");
    // Setup output.
    sort_out.AddOutput("c_city", c_city);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("revenue", revenue);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(revenue, optimizer::OrderByOrderingType::DESC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ4Part1(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Part
  auto p_table_oid = accessor->GetTableOid("ssbm.part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{1, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector d_col_oids = {d_schema.GetColumn("d_datekey").Oid(), d_schema.GetColumn("d_year").Oid()};
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(nullptr)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_col_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_nation = expr_maker.CVE(c_schema.GetColumn("c_nation").Oid(), type::TypeId::VARCHAR);
    auto c_region = expr_maker.CVE(c_schema.GetColumn("c_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_col_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                                  c_schema.GetColumn("c_nation").Oid(),
                                                  c_schema.GetColumn("c_region").Oid()};
    // Make the predicate: c_region = 'AMERICA'
    auto predicate = expr_maker.ComparisonEq(c_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    c_seq_scan_out.AddOutput("c_nation", c_nation);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_col_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_region = expr_maker.CVE(s_schema.GetColumn("s_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                  s_schema.GetColumn("s_region").Oid()};
    // Make the predicate: s_region = 'AMERICA'
    auto predicate = expr_maker.ComparisonEq(s_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_col_oids))
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_mfgr = expr_maker.CVE(p_schema.GetColumn("p_mfgr").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> p_col_oids = {p_schema.GetColumn("p_partkey").Oid(),
                                                  p_schema.GetColumn("p_mfgr").Oid()};
    // Make the predicate: p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2'
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(p_mfgr, expr_maker.Constant("MFGR#1")),
                                              expr_maker.ComparisonEq(p_mfgr, expr_maker.Constant("MFGR#2")));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_col_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumn("lo_partkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    auto lo_supplycost = expr_maker.CVE(lo_schema.GetColumn("lo_supplycost").Oid(), type::TypeId::INTEGER);
    std::vector lo_col_oids = {lo_schema.GetColumn("lo_orderdate").Oid(), lo_schema.GetColumn("lo_custkey").Oid(),
                               lo_schema.GetColumn("lo_suppkey").Oid(),   lo_schema.GetColumn("lo_partkey").Oid(),
                               lo_schema.GetColumn("lo_revenue").Oid(),   lo_schema.GetColumn("lo_supplycost").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    lo_seq_scan_out.AddOutput("lo_supplycost", lo_supplycost);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    auto lo_supplycost = lo_seq_scan_out.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_custkey", lo_custkey);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(p_partkey, lo_partkey))
                     .Build();
  }

  // customer <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    auto c_nation = c_seq_scan_out.GetOutput("c_nation");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_custkey = hash_join_out1.GetOutput("lo_custkey");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out1.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("lo_supplycost", lo_supplycost);
    hash_join_out2.AddOutput("c_nation", c_nation);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out2.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out2.GetOutput("lo_supplycost");
    auto c_nation = hash_join_out2.GetOutput("c_nation");
    // Output Schema
    hash_join_out3.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("lo_supplycost", lo_supplycost);
    hash_join_out3.AddOutput("c_nation", c_nation);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ3 <-> date       ==> HJ4
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  execution::compiler::test::OutputSchemaHelper hash_join_out4{0, &expr_maker};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out3.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out3.GetOutput("lo_supplycost");
    auto c_nation = hash_join_out3.GetOutput("c_nation");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out4.AddOutput("d_year", d_year);
    hash_join_out4.AddOutput("c_nation", c_nation);
    hash_join_out4.AddOutput("lo_revenue", lo_revenue);
    hash_join_out4.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto d_year = hash_join_out4.GetOutput("d_year");
    auto c_nation = hash_join_out4.GetOutput("c_nation");
    auto lo_revenue = hash_join_out4.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out4.GetOutput("lo_supplycost");
    auto profit = expr_maker.AggSum(expr_maker.OpMin(lo_revenue, lo_supplycost));
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("c_nation", c_nation);
    agg_out.AddAggTerm("profit", profit);
    // Make the output schema.
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("c_nation", agg_out.GetGroupByTermForOutput("c_nation"));
    agg_out.AddOutput("profit", agg_out.GetAggTermForOutput("profit"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(c_nation)
              .AddAggregateTerm(profit)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto d_year = agg_out.GetOutput("d_year");
    auto c_nation = agg_out.GetOutput("c_nation");
    auto profit = agg_out.GetOutput("profit");
    // Setup output.
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("c_nation", c_nation);
    sort_out.AddOutput("profit", profit);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(c_nation, optimizer::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ4Part2(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Part
  auto p_table_oid = accessor->GetTableOid("ssbm.part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{1, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector d_col_oids = {d_schema.GetColumn("d_datekey").Oid(), d_schema.GetColumn("d_year").Oid()};
    // Make predicate: d_year = 1997 or d_year = 1998
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(d_year, expr_maker.Constant(1997)),
                                              expr_maker.ComparisonEq(d_year, expr_maker.Constant(1998)));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_col_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_region = expr_maker.CVE(c_schema.GetColumn("c_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_col_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                                  c_schema.GetColumn("c_region").Oid()};
    // Make the predicate: c_region = 'AMERICA'
    auto predicate = expr_maker.ComparisonEq(c_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_col_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_region = expr_maker.CVE(s_schema.GetColumn("s_region").Oid(), type::TypeId::VARCHAR);
    auto s_nation = expr_maker.CVE(s_schema.GetColumn("s_nation").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                  s_schema.GetColumn("s_region").Oid(),
                                                  s_schema.GetColumn("s_nation").Oid()};
    // Make the predicate: s_region = 'AMERICA'
    auto predicate = expr_maker.ComparisonEq(s_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_nation", s_nation);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_col_oids))
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_mfgr = expr_maker.CVE(p_schema.GetColumn("p_mfgr").Oid(), type::TypeId::VARCHAR);
    auto p_category = expr_maker.CVE(p_schema.GetColumn("p_category").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> p_col_oids = {p_schema.GetColumn("p_partkey").Oid(),
                                                  p_schema.GetColumn("p_mfgr").Oid(),
                                                  p_schema.GetColumn("p_category").Oid()};

    // Make the predicate: p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2'
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(p_mfgr, expr_maker.Constant("MFGR#1")),
                                              expr_maker.ComparisonEq(p_mfgr, expr_maker.Constant("MFGR#2")));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_category", p_category);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_col_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumn("lo_partkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    auto lo_supplycost = expr_maker.CVE(lo_schema.GetColumn("lo_supplycost").Oid(), type::TypeId::INTEGER);
    std::vector lo_col_oids = {lo_schema.GetColumn("lo_orderdate").Oid(), lo_schema.GetColumn("lo_custkey").Oid(),
                               lo_schema.GetColumn("lo_suppkey").Oid(),   lo_schema.GetColumn("lo_partkey").Oid(),
                               lo_schema.GetColumn("lo_revenue").Oid(),   lo_schema.GetColumn("lo_supplycost").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    lo_seq_scan_out.AddOutput("lo_supplycost", lo_supplycost);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_category = p_seq_scan_out.GetOutput("p_category");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    auto lo_supplycost = lo_seq_scan_out.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_custkey", lo_custkey);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("p_category", p_category);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(p_partkey, lo_partkey))
                     .Build();
  }

  // customer <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_custkey = hash_join_out1.GetOutput("lo_custkey");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out1.GetOutput("lo_supplycost");
    auto p_category = hash_join_out1.GetOutput("p_category");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out2.AddOutput("p_category", p_category);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_nation = s_seq_scan_out.GetOutput("s_nation");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out2.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out2.GetOutput("lo_supplycost");
    auto p_category = hash_join_out2.GetOutput("p_category");
    // Output Schema
    hash_join_out3.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out3.AddOutput("s_nation", s_nation);
    hash_join_out3.AddOutput("p_category", p_category);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ3 <-> date       ==> HJ4
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  execution::compiler::test::OutputSchemaHelper hash_join_out4{0, &expr_maker};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out3.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out3.GetOutput("lo_supplycost");
    auto s_nation = hash_join_out3.GetOutput("s_nation");
    auto p_category = hash_join_out3.GetOutput("p_category");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out4.AddOutput("d_year", d_year);
    hash_join_out4.AddOutput("s_nation", s_nation);
    hash_join_out4.AddOutput("p_category", p_category);
    hash_join_out4.AddOutput("lo_revenue", lo_revenue);
    hash_join_out4.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto d_year = hash_join_out4.GetOutput("d_year");
    auto s_nation = hash_join_out4.GetOutput("s_nation");
    auto p_category = hash_join_out4.GetOutput("p_category");
    auto lo_revenue = hash_join_out4.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out4.GetOutput("lo_supplycost");
    auto profit = expr_maker.AggSum(expr_maker.OpMin(lo_revenue, lo_supplycost));
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("s_nation", s_nation);
    agg_out.AddGroupByTerm("p_category", p_category);
    agg_out.AddAggTerm("profit", profit);
    // Make the output schema.
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("s_nation", agg_out.GetGroupByTermForOutput("s_nation"));
    agg_out.AddOutput("p_category", agg_out.GetGroupByTermForOutput("p_category"));
    agg_out.AddOutput("profit", agg_out.GetAggTermForOutput("profit"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(s_nation)
              .AddGroupByTerm(p_category)
              .AddAggregateTerm(profit)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto d_year = agg_out.GetOutput("d_year");
    auto s_nation = agg_out.GetOutput("s_nation");
    auto p_category = agg_out.GetOutput("p_category");
    auto profit = agg_out.GetOutput("profit");
    // Setup output.
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("s_nation", s_nation);
    sort_out.AddOutput("p_category", p_category);
    sort_out.AddOutput("profit", profit);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(s_nation, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(p_category, optimizer::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>
SSBQuery::SSBMakeExecutableQ4Part3(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                   const execution::exec::ExecutionSettings &exec_settings) {
  execution::compiler::test::ExpressionMaker expr_maker;
  // Date
  auto d_table_oid = accessor->GetTableOid("ssbm.date");
  const auto &d_schema = accessor->GetSchema(d_table_oid);
  // Part
  auto p_table_oid = accessor->GetTableOid("ssbm.part");
  const auto &p_schema = accessor->GetSchema(p_table_oid);
  // Customer
  auto c_table_oid = accessor->GetTableOid("ssbm.customer");
  const auto &c_schema = accessor->GetSchema(c_table_oid);
  // Supplier
  auto s_table_oid = accessor->GetTableOid("ssbm.supplier");
  const auto &s_schema = accessor->GetSchema(s_table_oid);
  // LineOrder
  auto lo_table_oid = accessor->GetTableOid("ssbm.lineorder");
  const auto &lo_schema = accessor->GetSchema(lo_table_oid);

  // Scan date
  std::unique_ptr<planner::AbstractPlanNode> d_seq_scan;
  execution::compiler::test::OutputSchemaHelper d_seq_scan_out{1, &expr_maker};
  {
    auto d_datekey = expr_maker.CVE(d_schema.GetColumn("d_datekey").Oid(), type::TypeId::INTEGER);
    auto d_year = expr_maker.CVE(d_schema.GetColumn("d_year").Oid(), type::TypeId::INTEGER);
    std::vector d_col_oids = {d_schema.GetColumn("d_datekey").Oid(), d_schema.GetColumn("d_year").Oid()};
    // Make predicate: d_year = 1997 or d_year = 1998
    auto predicate = expr_maker.ConjunctionOr(expr_maker.ComparisonEq(d_year, expr_maker.Constant(1997)),
                                              expr_maker.ComparisonEq(d_year, expr_maker.Constant(1998)));
    // Make output schema.
    d_seq_scan_out.AddOutput("d_datekey", d_datekey);
    d_seq_scan_out.AddOutput("d_year", d_year);
    // Build plan node.
    d_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(d_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(d_table_oid)
                     .SetColumnOids(std::move(d_col_oids))
                     .Build();
  }

  // Scan customer
  std::unique_ptr<planner::AbstractPlanNode> c_seq_scan;
  execution::compiler::test::OutputSchemaHelper c_seq_scan_out{0, &expr_maker};
  {
    auto c_custkey = expr_maker.CVE(c_schema.GetColumn("c_custkey").Oid(), type::TypeId::INTEGER);
    auto c_region = expr_maker.CVE(c_schema.GetColumn("c_region").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> c_col_oids = {c_schema.GetColumn("c_custkey").Oid(),
                                                  c_schema.GetColumn("c_region").Oid()};
    // Make the predicate: c_region = 'AMERICA'
    auto predicate = expr_maker.ComparisonEq(c_region, expr_maker.Constant("AMERICA"));
    // Make output schema.
    c_seq_scan_out.AddOutput("c_custkey", c_custkey);
    // Build plan node.
    c_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(c_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(c_table_oid)
                     .SetColumnOids(std::move(c_col_oids))
                     .Build();
  }

  // Scan supplier
  std::unique_ptr<planner::AbstractPlanNode> s_seq_scan;
  execution::compiler::test::OutputSchemaHelper s_seq_scan_out{0, &expr_maker};
  {
    auto s_suppkey = expr_maker.CVE(s_schema.GetColumn("s_suppkey").Oid(), type::TypeId::INTEGER);
    auto s_city = expr_maker.CVE(s_schema.GetColumn("s_city").Oid(), type::TypeId::VARCHAR);
    auto s_nation = expr_maker.CVE(s_schema.GetColumn("s_nation").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> s_col_oids = {s_schema.GetColumn("s_suppkey").Oid(),
                                                  s_schema.GetColumn("s_city").Oid(),
                                                  s_schema.GetColumn("s_nation").Oid()};
    // Make the predicate: s_nation = 'UNITED STATES'
    auto predicate = expr_maker.ComparisonEq(s_nation, expr_maker.Constant("UNITED STATES"));
    // Make output schema.
    s_seq_scan_out.AddOutput("s_suppkey", s_suppkey);
    s_seq_scan_out.AddOutput("s_city", s_city);
    // Build plan node.
    s_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(s_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(s_table_oid)
                     .SetColumnOids(std::move(s_col_oids))
                     .Build();
  }

  // Scan part
  std::unique_ptr<planner::AbstractPlanNode> p_seq_scan;
  execution::compiler::test::OutputSchemaHelper p_seq_scan_out{0, &expr_maker};
  {
    auto p_partkey = expr_maker.CVE(p_schema.GetColumn("p_partkey").Oid(), type::TypeId::INTEGER);
    auto p_brand1 = expr_maker.CVE(p_schema.GetColumn("p_brand1").Oid(), type::TypeId::VARCHAR);
    auto p_category = expr_maker.CVE(p_schema.GetColumn("p_category").Oid(), type::TypeId::VARCHAR);
    std::vector<catalog::col_oid_t> p_col_oids = {p_schema.GetColumn("p_partkey").Oid(),
                                                  p_schema.GetColumn("p_brand1").Oid(),
                                                  p_schema.GetColumn("p_category").Oid()};
    // Make the predicate: p_category = 'MFGR#14'
    auto predicate = expr_maker.ComparisonEq(p_category, expr_maker.Constant("MFGR#14"));
    // Make output schema.
    p_seq_scan_out.AddOutput("p_partkey", p_partkey);
    p_seq_scan_out.AddOutput("p_brand1", p_brand1);
    // Build plan node.
    p_seq_scan = planner::SeqScanPlanNode::Builder{}
                     .SetOutputSchema(p_seq_scan_out.MakeSchema())
                     .SetScanPredicate(predicate)
                     .SetTableOid(p_table_oid)
                     .SetColumnOids(std::move(p_col_oids))
                     .Build();
  }

  // Scan lineorder.
  std::unique_ptr<planner::AbstractPlanNode> lo_seq_scan;
  execution::compiler::test::OutputSchemaHelper lo_seq_scan_out{1, &expr_maker};
  {
    auto lo_orderdate = expr_maker.CVE(lo_schema.GetColumn("lo_orderdate").Oid(), type::TypeId::INTEGER);
    auto lo_custkey = expr_maker.CVE(lo_schema.GetColumn("lo_custkey").Oid(), type::TypeId::INTEGER);
    auto lo_suppkey = expr_maker.CVE(lo_schema.GetColumn("lo_suppkey").Oid(), type::TypeId::INTEGER);
    auto lo_partkey = expr_maker.CVE(lo_schema.GetColumn("lo_partkey").Oid(), type::TypeId::INTEGER);
    auto lo_revenue = expr_maker.CVE(lo_schema.GetColumn("lo_revenue").Oid(), type::TypeId::INTEGER);
    auto lo_supplycost = expr_maker.CVE(lo_schema.GetColumn("lo_supplycost").Oid(), type::TypeId::INTEGER);
    std::vector lo_col_oids = {lo_schema.GetColumn("lo_orderdate").Oid(), lo_schema.GetColumn("lo_custkey").Oid(),
                               lo_schema.GetColumn("lo_suppkey").Oid(),   lo_schema.GetColumn("lo_partkey").Oid(),
                               lo_schema.GetColumn("lo_revenue").Oid(),   lo_schema.GetColumn("lo_supplycost").Oid()};
    // Make output schema.
    lo_seq_scan_out.AddOutput("lo_orderdate", lo_orderdate);
    lo_seq_scan_out.AddOutput("lo_custkey", lo_custkey);
    lo_seq_scan_out.AddOutput("lo_suppkey", lo_suppkey);
    lo_seq_scan_out.AddOutput("lo_partkey", lo_partkey);
    lo_seq_scan_out.AddOutput("lo_revenue", lo_revenue);
    lo_seq_scan_out.AddOutput("lo_supplycost", lo_supplycost);
    // Build plan node.
    planner::SeqScanPlanNode::Builder builder;
    lo_seq_scan = builder.SetOutputSchema(lo_seq_scan_out.MakeSchema())
                      .SetScanPredicate(nullptr)
                      .SetTableOid(lo_table_oid)
                      .SetColumnOids(std::move(lo_col_oids))
                      .Build();
  }

  // part <-> lineorder ==> HJ1
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  execution::compiler::test::OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left columns.
    auto p_partkey = p_seq_scan_out.GetOutput("p_partkey");
    auto p_brand1 = p_seq_scan_out.GetOutput("p_brand1");
    // Right columns.
    auto lo_orderdate = lo_seq_scan_out.GetOutput("lo_orderdate");
    auto lo_custkey = lo_seq_scan_out.GetOutput("lo_custkey");
    auto lo_suppkey = lo_seq_scan_out.GetOutput("lo_suppkey");
    auto lo_partkey = lo_seq_scan_out.GetOutput("lo_partkey");
    auto lo_revenue = lo_seq_scan_out.GetOutput("lo_revenue");
    auto lo_supplycost = lo_seq_scan_out.GetOutput("lo_supplycost");
    // Output Schema
    hash_join_out1.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out1.AddOutput("lo_custkey", lo_custkey);
    hash_join_out1.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out1.AddOutput("p_brand1", p_brand1);
    hash_join_out1.AddOutput("lo_revenue", lo_revenue);
    hash_join_out1.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(p_seq_scan))
                     .AddChild(std::move(lo_seq_scan))
                     .SetOutputSchema(hash_join_out1.MakeSchema())
                     .AddLeftHashKey(p_partkey)
                     .AddRightHashKey(lo_partkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(p_partkey, lo_partkey))
                     .Build();
  }

  // customer <-> HJ1   ==> HJ2
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  execution::compiler::test::OutputSchemaHelper hash_join_out2{1, &expr_maker};
  {
    // Left columns.
    auto c_custkey = c_seq_scan_out.GetOutput("c_custkey");
    // Right columns.
    auto lo_orderdate = hash_join_out1.GetOutput("lo_orderdate");
    auto lo_custkey = hash_join_out1.GetOutput("lo_custkey");
    auto lo_suppkey = hash_join_out1.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out1.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out1.GetOutput("lo_supplycost");
    auto p_brand1 = hash_join_out1.GetOutput("p_brand1");
    // Output Schema
    hash_join_out2.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out2.AddOutput("lo_suppkey", lo_suppkey);
    hash_join_out2.AddOutput("p_brand1", p_brand1);
    hash_join_out2.AddOutput("lo_revenue", lo_revenue);
    hash_join_out2.AddOutput("lo_supplycost", lo_supplycost);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(c_seq_scan))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(hash_join_out2.MakeSchema())
                     .AddLeftHashKey(c_custkey)
                     .AddRightHashKey(lo_custkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(c_custkey, lo_custkey))
                     .Build();
  }

  // supplier <-> HJ2       ==> HJ3
  std::unique_ptr<planner::AbstractPlanNode> hash_join3;
  execution::compiler::test::OutputSchemaHelper hash_join_out3{0, &expr_maker};
  {
    // Left columns.
    auto s_suppkey = s_seq_scan_out.GetOutput("s_suppkey");
    auto s_city = s_seq_scan_out.GetOutput("s_city");
    // Right columns.
    auto lo_orderdate = hash_join_out2.GetOutput("lo_orderdate");
    auto lo_suppkey = hash_join_out2.GetOutput("lo_suppkey");
    auto lo_revenue = hash_join_out2.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out2.GetOutput("lo_supplycost");
    auto p_brand1 = hash_join_out2.GetOutput("p_brand1");
    // Output Schema
    hash_join_out3.AddOutput("lo_orderdate", lo_orderdate);
    hash_join_out3.AddOutput("s_city", s_city);
    hash_join_out3.AddOutput("p_brand1", p_brand1);
    hash_join_out3.AddOutput("lo_revenue", lo_revenue);
    hash_join_out3.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join3 = builder.AddChild(std::move(s_seq_scan))
                     .AddChild(std::move(hash_join2))
                     .SetOutputSchema(hash_join_out3.MakeSchema())
                     .AddLeftHashKey(s_suppkey)
                     .AddRightHashKey(lo_suppkey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(s_suppkey, lo_suppkey))
                     .Build();
  }

  // HJ3 <-> date       ==> HJ4
  std::unique_ptr<planner::AbstractPlanNode> hash_join4;
  execution::compiler::test::OutputSchemaHelper hash_join_out4{0, &expr_maker};
  {
    // Left columns.
    auto lo_orderdate = hash_join_out3.GetOutput("lo_orderdate");
    auto lo_revenue = hash_join_out3.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out3.GetOutput("lo_supplycost");
    auto s_city = hash_join_out3.GetOutput("s_city");
    auto p_brand1 = hash_join_out3.GetOutput("p_brand1");
    // Right columns.
    auto d_datekey = d_seq_scan_out.GetOutput("d_datekey");
    auto d_year = d_seq_scan_out.GetOutput("d_year");
    // Output Schema
    hash_join_out4.AddOutput("d_year", d_year);
    hash_join_out4.AddOutput("s_city", s_city);
    hash_join_out4.AddOutput("p_brand1", p_brand1);
    hash_join_out4.AddOutput("lo_revenue", lo_revenue);
    hash_join_out4.AddOutput("lo_supplycost", lo_supplycost);
    // Build.
    planner::HashJoinPlanNode::Builder builder;
    hash_join4 = builder.AddChild(std::move(hash_join3))
                     .AddChild(std::move(d_seq_scan))
                     .SetOutputSchema(hash_join_out4.MakeSchema())
                     .AddLeftHashKey(lo_orderdate)
                     .AddRightHashKey(d_datekey)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(expr_maker.ComparisonEq(lo_orderdate, d_datekey))
                     .Build();
  }

  // Aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  execution::compiler::test::OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto d_year = hash_join_out4.GetOutput("d_year");
    auto s_city = hash_join_out4.GetOutput("s_city");
    auto p_brand1 = hash_join_out4.GetOutput("p_brand1");
    auto lo_revenue = hash_join_out4.GetOutput("lo_revenue");
    auto lo_supplycost = hash_join_out4.GetOutput("lo_supplycost");
    auto profit = expr_maker.AggSum(expr_maker.OpMin(lo_revenue, lo_supplycost));
    // Add them to the helper.
    agg_out.AddGroupByTerm("d_year", d_year);
    agg_out.AddGroupByTerm("s_city", s_city);
    agg_out.AddGroupByTerm("p_brand1", p_brand1);
    agg_out.AddAggTerm("profit", profit);
    // Make the output schema.
    agg_out.AddOutput("d_year", agg_out.GetGroupByTermForOutput("d_year"));
    agg_out.AddOutput("s_city", agg_out.GetGroupByTermForOutput("s_city"));
    agg_out.AddOutput("p_brand1", agg_out.GetGroupByTermForOutput("p_brand1"));
    agg_out.AddOutput("profit", agg_out.GetAggTermForOutput("profit"));
    // Build plan node.
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(agg_out.MakeSchema())
              .AddGroupByTerm(d_year)
              .AddGroupByTerm(s_city)
              .AddGroupByTerm(p_brand1)
              .AddAggregateTerm(profit)
              .AddChild(std::move(hash_join4))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Sort.
  std::unique_ptr<planner::AbstractPlanNode> sort;
  execution::compiler::test::OutputSchemaHelper sort_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto d_year = agg_out.GetOutput("d_year");
    auto s_city = agg_out.GetOutput("s_city");
    auto p_brand1 = agg_out.GetOutput("p_brand1");
    auto profit = agg_out.GetOutput("profit");
    // Setup output.
    sort_out.AddOutput("d_year", d_year);
    sort_out.AddOutput("s_city", s_city);
    sort_out.AddOutput("p_brand1", p_brand1);
    sort_out.AddOutput("profit", profit);
    // Build.
    sort = planner::OrderByPlanNode::Builder{}
               .SetOutputSchema(sort_out.MakeSchema())
               .AddChild(std::move(agg))
               .AddSortKey(d_year, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(s_city, optimizer::OrderByOrderingType::ASC)
               .AddSortKey(p_brand1, optimizer::OrderByOrderingType::ASC)
               .Build();
  }

  // Compile plan
  auto query = execution::compiler::CompilationContext::Compile(*sort, exec_settings, accessor.get());
  return std::make_tuple(std::move(query), std::move(sort));
}

}  // namespace terrier::ssb
