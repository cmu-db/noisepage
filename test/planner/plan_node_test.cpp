#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "test_util/test_harness.h"

namespace noisepage::planner {

class PlanNodeTest : public TerrierTest {
 public:
  static std::unique_ptr<OutputSchema> BuildOneColumnSchema(std::string name, const type::TypeId type) {
    auto pred = std::make_unique<parser::ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
    std::vector<OutputSchema::Column> cols;
    cols.emplace_back(OutputSchema::Column(std::move(name), type, std::move(pred)));
    return std::make_unique<OutputSchema>(std::move(cols));
  }
};

// NOLINTNEXTLINE
TEST(PlanNodeTest, AnalyzePlanTest) {
  catalog::db_oid_t db_oid(1);
  catalog::table_oid_t table_oid(3);

  AnalyzePlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(db_oid)
                  .SetTableOid(table_oid)
                  .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                  .Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(PlanNodeType::ANALYZE, plan->GetPlanNodeType());
  EXPECT_EQ(plan->GetDatabaseOid(), db_oid);
  EXPECT_EQ(plan->GetTableOid(), table_oid);

  // Make sure that the hash and equality function works correctly
  AnalyzePlanNode::Builder builder2;
  auto plan2 = builder2.SetDatabaseOid(catalog::db_oid_t(db_oid))
                   .SetTableOid(table_oid)
                   .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                   .Build();
  EXPECT_EQ(plan->GetDatabaseOid(), plan2->GetDatabaseOid());
  EXPECT_EQ(plan->GetTableOid(), plan2->GetTableOid());
  EXPECT_EQ(*plan, *plan2);
  EXPECT_EQ(plan->Hash(), plan2->Hash());

  // Make different variations of the plan node and make
  // sure that they are not equal
  for (int i = 0; i < 3; i++) {
    catalog::db_oid_t other_db_oid = db_oid;
    catalog::table_oid_t other_table_oid = table_oid;
    auto other_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER);

    switch (i) {
      case 0:
        other_db_oid = catalog::db_oid_t(999);
        break;
      case 1:
        other_table_oid = catalog::table_oid_t(777);
        break;
      case 2:
        other_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
    }

    AnalyzePlanNode::Builder builder3;
    auto plan3 = builder3.SetDatabaseOid(other_db_oid)
                     .SetTableOid(other_table_oid)
                     .SetOutputSchema(std::move(other_schema))
                     .Build();
    EXPECT_NE(*plan, *plan3);
    EXPECT_NE(plan->Hash(), plan3->Hash());
  }
}

// Test creation of simple two table join.
// We construct the plan for the following query: SELECT table1.col1 FROM table1, table2 WHERE table1.col1 =
// table2.col2; NOLINTNEXTLINE
TEST(PlanNodeTest, HashJoinPlanTest) {
  SeqScanPlanNode::Builder seq_scan_builder;
  HashJoinPlanNode::Builder hash_join_builder;

  auto scan_pred_1 = std::make_unique<parser::StarExpression>();
  // Build left scan
  auto seq_scan_1 =
      seq_scan_builder.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
          .SetTableOid(catalog::table_oid_t(1))
          .SetDatabaseOid(catalog::db_oid_t(0))
          .SetScanPredicate(common::ManagedPointer(scan_pred_1).CastManagedPointerTo<parser::AbstractExpression>())
          .SetIsForUpdateFlag(false)
          .Build();

  EXPECT_EQ(PlanNodeType::SEQSCAN, seq_scan_1->GetPlanNodeType());
  EXPECT_EQ(0, seq_scan_1->GetChildrenSize());
  EXPECT_EQ(catalog::table_oid_t(1), seq_scan_1->GetTableOid());
  EXPECT_EQ(catalog::db_oid_t(0), seq_scan_1->GetDatabaseOid());
  EXPECT_EQ(parser::ExpressionType::STAR, seq_scan_1->GetScanPredicate()->GetExpressionType());
  EXPECT_FALSE(seq_scan_1->IsForUpdate());

  auto scan_pred_2 = std::make_unique<parser::StarExpression>();
  auto seq_scan_2 =
      seq_scan_builder.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col2", type::TypeId::INTEGER))
          .SetTableOid(catalog::table_oid_t(2))
          .SetDatabaseOid(catalog::db_oid_t(0))
          .SetScanPredicate(common::ManagedPointer(scan_pred_2).CastManagedPointerTo<parser::AbstractExpression>())
          .SetIsForUpdateFlag(false)
          .Build();

  EXPECT_EQ(PlanNodeType::SEQSCAN, seq_scan_2->GetPlanNodeType());
  EXPECT_EQ(0, seq_scan_2->GetChildrenSize());
  EXPECT_EQ(catalog::table_oid_t(2), seq_scan_2->GetTableOid());
  EXPECT_EQ(catalog::db_oid_t(0), seq_scan_2->GetDatabaseOid());
  EXPECT_EQ(parser::ExpressionType::STAR, seq_scan_2->GetScanPredicate()->GetExpressionType());
  EXPECT_FALSE(seq_scan_2->IsForUpdate());

  std::vector<std::unique_ptr<parser::AbstractExpression>> expr_children;
  expr_children.push_back(std::make_unique<parser::ColumnValueExpression>("table1", "col1"));
  expr_children.push_back(std::make_unique<parser::ColumnValueExpression>("table2", "col2"));
  auto cmp_expression =
      std::make_unique<parser::ComparisonExpression>(parser::ExpressionType::COMPARE_EQUAL, std::move(expr_children));

  auto hash_join_plan =
      hash_join_builder.SetJoinType(LogicalJoinType::INNER)
          .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
          .SetJoinPredicate(common::ManagedPointer(cmp_expression).CastManagedPointerTo<parser::AbstractExpression>())
          .AddChild(std::move(seq_scan_1))
          .AddChild(std::move(seq_scan_2))
          .Build();

  EXPECT_EQ(PlanNodeType::HASHJOIN, hash_join_plan->GetPlanNodeType());
  EXPECT_EQ(2, hash_join_plan->GetChildrenSize());
  EXPECT_EQ(LogicalJoinType::INNER, hash_join_plan->GetLogicalJoinType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, hash_join_plan->GetJoinPredicate()->GetExpressionType());
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, AggregatePlanTest) {
  parser::AbstractExpression *predicate = new parser::StarExpression();
  auto cve = std::make_unique<parser::ColumnValueExpression>("tbl", "col1");
  auto dve = std::make_unique<parser::DerivedValueExpression>(type::TypeId::INTEGER, 0, 0);
  auto gb_term = reinterpret_cast<parser::AbstractExpression *>(dve.get());
  std::vector<std::unique_ptr<parser::AbstractExpression>> children;
  children.emplace_back(std::move(cve));

  auto *aggr_term =
      new parser::AggregateExpression(parser::ExpressionType::AGGREGATE_COUNT, std::move(children), false);

  planner::AggregatePlanNode::Builder builder;
  builder.AddAggregateTerm(common::ManagedPointer(aggr_term));
  builder.AddGroupByTerm(common::ManagedPointer(gb_term));
  builder.SetHavingClausePredicate(common::ManagedPointer(predicate));
  builder.SetAggregateStrategyType(planner::AggregateStrategyType::HASH);
  builder.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER));
  auto plan = builder.Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(PlanNodeType::AGGREGATE, plan->GetPlanNodeType());
  EXPECT_EQ(plan->GetAggregateStrategyType(), planner::AggregateStrategyType::HASH);
  EXPECT_EQ(plan->GetAggregateTerms().size(), 1);
  EXPECT_EQ(*plan->GetAggregateTerms()[0], *aggr_term);
  EXPECT_EQ(*plan->GetHavingClausePredicate(), *predicate);

  planner::AggregatePlanNode::Builder builder2;
  auto nc_aggr = dynamic_cast<parser::AggregateExpression *>(aggr_term->Copy().release());
  auto predicate2 = predicate->Copy();

  builder2.AddAggregateTerm(common::ManagedPointer(nc_aggr));
  builder2.AddGroupByTerm(common::ManagedPointer(gb_term));
  builder2.SetHavingClausePredicate(common::ManagedPointer(predicate2));
  builder2.SetAggregateStrategyType(planner::AggregateStrategyType::HASH);
  builder2.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER));
  auto plan2 = builder2.Build();
  EXPECT_EQ(*plan, *plan2);
  EXPECT_EQ(plan->Hash(), plan2->Hash());

  // Make different variations of the plan node and make
  // sure that they are not equal
  for (int i = 0; i < 4; i++) {
    auto other_predicate = predicate->Copy();
    auto dve_copy = dve->Copy();
    auto other_strategy = planner::AggregateStrategyType::HASH;
    parser::AggregateExpression *other_aggr = dynamic_cast<parser::AggregateExpression *>(aggr_term->Copy().release());
    auto other_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER);

    switch (i) {
      case 0:
        other_predicate = std::make_unique<parser::ColumnValueExpression>("tbl", "col");
        break;
      case 1:
        dve_copy = std::make_unique<parser::DerivedValueExpression>(type::TypeId::INTEGER, 0, 1);
        break;
      case 2: {
        auto o_cve = std::make_unique<parser::ColumnValueExpression>("tbl", "col");
        std::vector<std::unique_ptr<parser::AbstractExpression>> children;
        children.emplace_back(std::move(o_cve));
        delete other_aggr;
        other_aggr = new parser::AggregateExpression(parser::ExpressionType::AGGREGATE_MAX, std::move(children), false);
        break;
      }
      case 3:
        other_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
    }

    planner::AggregatePlanNode::Builder builder3;
    auto plan3 = builder3.AddAggregateTerm(common::ManagedPointer(other_aggr))
                     .AddGroupByTerm(common::ManagedPointer(dve_copy))
                     .SetHavingClausePredicate(common::ManagedPointer(other_predicate))
                     .SetAggregateStrategyType(other_strategy)
                     .SetOutputSchema(std::move(other_schema))
                     .Build();
    EXPECT_NE(*plan, *plan3);
    EXPECT_NE(plan->Hash(), plan3->Hash());
    delete other_aggr;
  }

  delete aggr_term;
  delete nc_aggr;
  delete predicate;
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, CSVScanPlanTest) {
  catalog::db_oid_t db_oid(1);
  std::string file_name = "/home/file.txt";
  char delimiter = ',';
  char quote = '"';
  char escape = '\\';
  std::vector<type::TypeId> value_types = {type::TypeId::INTEGER};

  planner::CSVScanPlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(db_oid)
                  .SetIsForUpdateFlag(false)
                  .SetFileName(file_name)
                  .SetDelimiter(delimiter)
                  .SetQuote(quote)
                  .SetEscape(escape)
                  .SetValueTypes(value_types)
                  .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                  .Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(PlanNodeType::CSVSCAN, plan->GetPlanNodeType());
  EXPECT_EQ(plan->GetDatabaseOid(), db_oid);
  EXPECT_EQ(plan->GetFileName(), file_name);
  EXPECT_EQ(plan->GetDelimiterChar(), delimiter);
  EXPECT_EQ(plan->GetQuoteChar(), quote);
  EXPECT_EQ(plan->GetEscapeChar(), escape);
  EXPECT_EQ(plan->GetValueTypes(), value_types);
  EXPECT_EQ(plan->IsForUpdate(), false);

  planner::CSVScanPlanNode::Builder builder2;
  auto plan2 = builder2.SetDatabaseOid(db_oid)
                   .SetIsForUpdateFlag(false)
                   .SetFileName(file_name)
                   .SetDelimiter(delimiter)
                   .SetQuote(quote)
                   .SetEscape(escape)
                   .SetValueTypes(value_types)
                   .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                   .Build();
  EXPECT_EQ(*plan, *plan2);
  EXPECT_EQ(plan->Hash(), plan2->Hash());

  // Make different variations of the plan node and make
  // sure that they are not equal
  for (int i = 0; i < 8; i++) {
    catalog::db_oid_t o_db_oid(1);
    std::string o_file_name = "/home/file.txt";
    char o_delimiter = ',';
    char o_quote = '"';
    char o_escape = '\\';
    std::vector<type::TypeId> o_value_types = {type::TypeId::INTEGER};
    auto o_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER);
    auto o_update = false;

    switch (i) {
      case 0:
        o_db_oid = catalog::db_oid_t(999);
        break;
      case 1:
        o_file_name = "/home/file2.txt";
        break;
      case 2:
        o_delimiter = ' ';
        break;
      case 3:
        o_quote = 'q';
        break;
      case 4:
        o_escape = '\0';
        break;
      case 5:
        o_value_types = {type::TypeId::VARCHAR};
        break;
      case 6:
        o_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
      case 7:
        o_update = true;
        break;
    }

    planner::CSVScanPlanNode::Builder builder3;
    auto plan3 = builder3.SetDatabaseOid(o_db_oid)
                     .SetIsForUpdateFlag(o_update)
                     .SetFileName(o_file_name)
                     .SetDelimiter(o_delimiter)
                     .SetQuote(o_quote)
                     .SetEscape(o_escape)
                     .SetValueTypes(o_value_types)
                     .SetOutputSchema(std::move(o_schema))
                     .Build();
    EXPECT_NE(*plan, *plan3);
    EXPECT_NE(plan->Hash(), plan3->Hash());
  }
}
}  // namespace noisepage::planner
