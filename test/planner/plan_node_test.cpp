#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/hash_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "util/test_harness.h"

namespace terrier::planner {

class PlanNodeTest : public TerrierTest {
 public:
  static std::shared_ptr<OutputSchema> BuildOneColumnSchema(std::string name, const type::TypeId type) {
    OutputSchema::Column col(std::move(name), type);
    std::vector<OutputSchema::Column> cols;
    cols.push_back(col);
    auto schema = std::make_shared<OutputSchema>(cols);
    return schema;
  }
};

// NOLINTNEXTLINE
TEST(PlanNodeTest, AnalyzePlanTest) {
  catalog::db_oid_t db_oid(1);
  catalog::namespace_oid_t ns_oid(2);
  catalog::table_oid_t table_oid(3);

  AnalyzePlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(db_oid)
                  .SetNamespaceOid(ns_oid)
                  .SetTableOid(table_oid)
                  .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                  .Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(PlanNodeType::ANALYZE, plan->GetPlanNodeType());
  EXPECT_EQ(plan->GetDatabaseOid(), db_oid);
  EXPECT_EQ(plan->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(plan->GetTableOid(), table_oid);

  // Make sure that the hash and equality function works correctly
  AnalyzePlanNode::Builder builder2;
  auto plan2 = builder2.SetDatabaseOid(catalog::db_oid_t(db_oid))
                   .SetNamespaceOid(catalog::namespace_oid_t(2))
                   .SetTableOid(table_oid)
                   .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                   .Build();
  EXPECT_EQ(plan->GetDatabaseOid(), plan2->GetDatabaseOid());
  EXPECT_EQ(plan->GetNamespaceOid(), plan2->GetNamespaceOid());
  EXPECT_EQ(plan->GetTableOid(), plan2->GetTableOid());
  EXPECT_EQ(*plan, *plan2);
  EXPECT_EQ(plan->Hash(), plan2->Hash());

  // Make different variations of the plan node and make
  // sure that they are not equal
  for (int i = 0; i < 4; i++) {
    catalog::db_oid_t other_db_oid = db_oid;
    catalog::namespace_oid_t other_ns_oid = ns_oid;
    catalog::table_oid_t other_table_oid = table_oid;
    auto other_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER);

    switch (i) {
      case 0:
        other_db_oid = catalog::db_oid_t(999);
        break;
      case 1:
        other_ns_oid = catalog::namespace_oid_t(888);
        break;
      case 2:
        other_table_oid = catalog::table_oid_t(777);
        break;
      case 3:
        other_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
    }

    AnalyzePlanNode::Builder builder3;
    auto plan3 = builder3.SetDatabaseOid(other_db_oid)
                     .SetNamespaceOid(other_ns_oid)
                     .SetTableOid(other_table_oid)
                     .SetOutputSchema(other_schema)
                     .Build();
    EXPECT_NE(*plan, *plan3);
    EXPECT_NE(plan->Hash(), plan3->Hash());
  }
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, CreateDatabasePlanTest) {
  parser::PostgresParser pgparser;
  auto stms = pgparser.BuildParseTree("CREATE DATABASE test");
  EXPECT_EQ(1, stms.size());
  auto *create_stmt = static_cast<parser::CreateStatement *>(stms[0].get());

  CreateDatabasePlanNode::Builder builder;
  auto plan = builder.SetFromCreateStatement(create_stmt).Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_STREQ("test", plan->GetDatabaseName().c_str());
  EXPECT_EQ(PlanNodeType::CREATE_DATABASE, plan->GetPlanNodeType());
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, DropDatabasePlanTest) {
  parser::PostgresParser pgparser;
  auto stms = pgparser.BuildParseTree("DROP DATABASE test");
  EXPECT_EQ(1, stms.size());
  auto *drop_stmt = static_cast<parser::DropStatement *>(stms[0].get());

  DropDatabasePlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(catalog::db_oid_t(0)).SetFromDropStatement(drop_stmt).Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(catalog::db_oid_t(0), plan->GetDatabaseOid());
  EXPECT_FALSE(plan->IsIfExists());
  EXPECT_EQ(PlanNodeType::DROP_DATABASE, plan->GetPlanNodeType());
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, DropDatabasePlanIfExistsTest) {
  parser::PostgresParser pgparser;
  auto stms = pgparser.BuildParseTree("DROP DATABASE IF EXISTS test");
  EXPECT_EQ(1, stms.size());
  auto *drop_stmt = static_cast<parser::DropStatement *>(stms[0].get());

  DropDatabasePlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(catalog::db_oid_t(0)).SetFromDropStatement(drop_stmt).Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(catalog::db_oid_t(0), plan->GetDatabaseOid());
  EXPECT_TRUE(plan->IsIfExists());
  EXPECT_EQ(PlanNodeType::DROP_DATABASE, plan->GetPlanNodeType());
}

// Test creation of simple two table join.
// We construct the plan for the following query: SELECT table1.col1 FROM table1, table2 WHERE table1.col1 =
// table2.col2; NOLINTNEXTLINE
TEST(PlanNodeTest, HashJoinPlanTest) {
  SeqScanPlanNode::Builder seq_scan_builder;
  HashPlanNode::Builder hash_builder;
  HashJoinPlanNode::Builder hash_join_builder;

  // Build left scan
  auto seq_scan_1 = seq_scan_builder.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                        .SetTableOid(catalog::table_oid_t(1))
                        .SetDatabaseOid(catalog::db_oid_t(0))
                        .SetScanPredicate(new parser::StarExpression())
                        .SetIsForUpdateFlag(false)
                        .SetIsParallelFlag(true)
                        .Build();

  EXPECT_EQ(PlanNodeType::SEQSCAN, seq_scan_1->GetPlanNodeType());
  EXPECT_EQ(0, seq_scan_1->GetChildrenSize());
  EXPECT_EQ(catalog::table_oid_t(1), seq_scan_1->GetTableOid());
  EXPECT_EQ(catalog::db_oid_t(0), seq_scan_1->GetDatabaseOid());
  EXPECT_EQ(parser::ExpressionType::STAR, seq_scan_1->GetScanPredicate()->GetExpressionType());
  EXPECT_FALSE(seq_scan_1->IsForUpdate());
  EXPECT_TRUE(seq_scan_1->IsParallel());

  auto seq_scan_2 = seq_scan_builder.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col2", type::TypeId::INTEGER))
                        .SetTableOid(catalog::table_oid_t(2))
                        .SetDatabaseOid(catalog::db_oid_t(0))
                        .SetScanPredicate(new parser::StarExpression())
                        .SetIsForUpdateFlag(false)
                        .SetIsParallelFlag(true)
                        .Build();

  EXPECT_EQ(PlanNodeType::SEQSCAN, seq_scan_2->GetPlanNodeType());
  EXPECT_EQ(0, seq_scan_2->GetChildrenSize());
  EXPECT_EQ(catalog::table_oid_t(2), seq_scan_2->GetTableOid());
  EXPECT_EQ(catalog::db_oid_t(0), seq_scan_2->GetDatabaseOid());
  EXPECT_EQ(parser::ExpressionType::STAR, seq_scan_2->GetScanPredicate()->GetExpressionType());
  EXPECT_FALSE(seq_scan_2->IsForUpdate());
  EXPECT_TRUE(seq_scan_2->IsParallel());

  auto hash_plan = hash_builder.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col2", type::TypeId::INTEGER))
                       .AddHashKey(new parser::ColumnValueExpression("col2", "table2"))
                       .AddChild(std::move(seq_scan_2))
                       .Build();

  EXPECT_EQ(PlanNodeType::HASH, hash_plan->GetPlanNodeType());
  EXPECT_EQ(1, hash_plan->GetChildrenSize());
  EXPECT_EQ(1, hash_plan->GetHashKeys().size());
  EXPECT_EQ(parser::ExpressionType::COLUMN_VALUE, hash_plan->GetHashKeys()[0]->GetExpressionType());

  std::vector<const parser::AbstractExpression *> expr_children;
  expr_children.push_back(new parser::ColumnValueExpression("col1", "table1"));
  expr_children.push_back(new parser::ColumnValueExpression("col2", "table2"));
  auto cmp_expression =
      new parser::ComparisonExpression(parser::ExpressionType::COMPARE_EQUAL, std::move(expr_children));

  auto hash_join_plan = hash_join_builder.SetJoinType(LogicalJoinType::INNER)
                            .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                            .SetJoinPredicate(cmp_expression)
                            .AddChild(std::move(seq_scan_1))
                            .AddChild(std::move(hash_plan))
                            .Build();

  EXPECT_EQ(PlanNodeType::HASHJOIN, hash_join_plan->GetPlanNodeType());
  EXPECT_EQ(2, hash_join_plan->GetChildrenSize());
  EXPECT_EQ(LogicalJoinType::INNER, hash_join_plan->GetLogicalJoinType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, hash_join_plan->GetJoinPredicate()->GetExpressionType());
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, AggregatePlanTest) {
  auto *predicate = new parser::StarExpression();
  auto *cve = new parser::ColumnValueExpression("tbl", "col1");
  auto *aggr_term = new parser::AggregateExpression(parser::ExpressionType::AGGREGATE_COUNT, {cve}, false);

  planner::AggregatePlanNode::Builder builder;
  builder.AddAggregateTerm(aggr_term);
  builder.SetGroupByColOffsets({0});
  builder.SetHavingClausePredicate(predicate);
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
  auto aggr_copy = reinterpret_cast<const parser::AggregateExpression*>(aggr_term->Copy());
  auto nc_aggr = const_cast<parser::AggregateExpression*>(aggr_copy);
  auto predicate2 = predicate->Copy();

  builder2.AddAggregateTerm(nc_aggr);
  builder2.SetGroupByColOffsets({0});
  builder2.SetHavingClausePredicate(predicate2);
  builder2.SetAggregateStrategyType(planner::AggregateStrategyType::HASH);
  builder2.SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER));
  auto plan2 = builder2.Build();
  EXPECT_EQ(*plan, *plan2);
  EXPECT_EQ(plan->Hash(), plan2->Hash());

  // Make different variations of the plan node and make
  // sure that they are not equal
  for (int i = 0; i < 4; i++) {
    auto other_predicate = predicate->Copy();
    std::vector<unsigned> col_offsets = {0};
    auto other_strategy = planner::AggregateStrategyType::HASH;
    auto other_aggr = aggr_term->Copy();
    auto other_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER);

    switch (i) {
      case 0:
        delete other_predicate;
        other_predicate = new parser::ColumnValueExpression("tbl", "col");
        break;
      case 1:
        col_offsets = {1,2};
        break;
      case 2: {
        delete other_aggr;
        auto *o_cve = new parser::ColumnValueExpression("tbl", "col");
        other_aggr = new parser::AggregateExpression(parser::ExpressionType::AGGREGATE_MAX, {o_cve}, false);
        break;
      }
      case 3:
        other_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
    }

    auto other_aggr_term = reinterpret_cast<const parser::AggregateExpression*>(other_aggr);
    auto nc_other_aggr = const_cast<parser::AggregateExpression*>(other_aggr_term);

    planner::AggregatePlanNode::Builder builder3;
    auto plan3 = builder3.AddAggregateTerm(nc_other_aggr)
                         .SetGroupByColOffsets(col_offsets)
                         .SetHavingClausePredicate(other_predicate)
                         .SetAggregateStrategyType(other_strategy)
                         .SetOutputSchema(other_schema)
                         .Build();
    EXPECT_NE(*plan, *plan3);
    EXPECT_NE(plan->Hash(), plan3->Hash());
  }
}

// NOLINTNEXTLINE
TEST(PlanNodeTest, CSVScanPlanTest) {
  catalog::db_oid_t db_oid(1);
  catalog::namespace_oid_t ns_oid(2);
  std::string file_name = "/home/file.txt";
  char delimiter = ',';
  char quote = '"';
  char escape = '\\';
  std::string null_string = "";
  std::vector<type::TypeId> value_types = { type::TypeId::INTEGER };

  planner::CSVScanPlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(db_oid)
                     .SetNamespaceOid(ns_oid)
                     .SetIsParallelFlag(true)
                     .SetIsForUpdateFlag(false)
                     .SetFileName(file_name)
                     .SetDelimiter(delimiter)
                     .SetQuote(quote)
                     .SetEscape(escape)
                     .SetNullString(null_string)
                     .SetValueTypes(value_types)
                     .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                     .Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(PlanNodeType::CSVSCAN, plan->GetPlanNodeType());
  EXPECT_EQ(plan->GetDatabaseOid(), db_oid);
  EXPECT_EQ(plan->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(plan->GetFileName(), file_name);
  EXPECT_EQ(plan->GetDelimiterChar(), delimiter);
  EXPECT_EQ(plan->GetQuoteChar(), quote);
  EXPECT_EQ(plan->GetEscapeChar(), escape);
  EXPECT_EQ(plan->GetNullString(), null_string);
  EXPECT_EQ(plan->GetValueTypes(), value_types);
  EXPECT_EQ(plan->IsForUpdate(), false);
  EXPECT_EQ(plan->IsParallel(), true);

  planner::CSVScanPlanNode::Builder builder2;
  auto plan2 = builder2.SetDatabaseOid(db_oid)
                       .SetNamespaceOid(ns_oid)
                       .SetIsParallelFlag(true)
                       .SetIsForUpdateFlag(false)
                       .SetFileName(file_name)
                       .SetDelimiter(delimiter)
                       .SetQuote(quote)
                       .SetEscape(escape)
                       .SetNullString(null_string)
                       .SetValueTypes(value_types)
                       .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER))
                       .Build();
  EXPECT_EQ(*plan, *plan2);
  EXPECT_EQ(plan->Hash(), plan2->Hash());

  // Make different variations of the plan node and make
  // sure that they are not equal
  for (int i = 0; i < 11; i++) {
    catalog::db_oid_t o_db_oid(1);
    catalog::namespace_oid_t o_ns_oid(2);
    std::string o_file_name = "/home/file.txt";
    char o_delimiter = ',';
    char o_quote = '"';
    char o_escape = '\\';
    std::string o_null_string = "";
    std::vector<type::TypeId> o_value_types = { type::TypeId::INTEGER };
    auto o_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER);
    auto o_parallel = true;
    auto o_update = false;

    switch (i) {
      case 0:
        o_db_oid = catalog::db_oid_t(999);
        break;
      case 1:
        o_ns_oid = catalog::namespace_oid_t(3);
        break;
      case 2:
        o_file_name = "/home/file2.txt";
        break;
      case 3:
        o_delimiter = ' ';
        break;
      case 4:
        o_quote = 'q';
        break;
      case 5:
        o_escape = '\0';
        break;
      case 6:
        o_null_string = "NULL";
        break;
      case 7:
        o_value_types = { type::TypeId::VARCHAR };
        break;
      case 8:
        o_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
      case 9:
        o_parallel = false;
        break;
      case 10:
        o_update = true;
        break;
    }

    planner::CSVScanPlanNode::Builder builder3;
    auto plan3 = builder3.SetDatabaseOid(o_db_oid)
                         .SetNamespaceOid(o_ns_oid)
                         .SetIsParallelFlag(o_parallel)
                         .SetIsForUpdateFlag(o_update)
                         .SetFileName(o_file_name)
                         .SetDelimiter(o_delimiter)
                         .SetQuote(o_quote)
                         .SetEscape(o_escape)
                         .SetNullString(o_null_string)
                         .SetValueTypes(o_value_types)
                         .SetOutputSchema(o_schema)
                         .Build();
    EXPECT_NE(*plan, *plan3);
    EXPECT_NE(plan->Hash(), plan3->Hash());
  }
}
}  // namespace terrier::planner
