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
#include "util/test_harness.h"

namespace terrier::planner {

class PlanNodeTest : public TerrierTest {
 public:
  static std::shared_ptr<OutputSchema> BuildOneColumnSchema(std::string name, const type::TypeId type,
                                                            const bool nullable, const catalog::col_oid_t oid) {
    OutputSchema::Column col(std::move(name), type, nullable, oid);
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
                  .SetOutputSchema(
                      PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER, false, catalog::col_oid_t(1)))
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
                   .SetOutputSchema(
                       PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER, false, catalog::col_oid_t(1)))
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
    auto other_schema = PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER, false, catalog::col_oid_t(1));

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
        other_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER, false, catalog::col_oid_t(1));
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
  auto seq_scan_1 = seq_scan_builder
                        .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER, false,
                                                                            catalog::col_oid_t(1)))
                        .SetTableOid(catalog::table_oid_t(1))
                        .SetDatabaseOid(catalog::db_oid_t(0))
                        .SetScanPredicate(std::make_shared<parser::StarExpression>())
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

  auto seq_scan_2 = seq_scan_builder
                        .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col2", type::TypeId::INTEGER, false,
                                                                            catalog::col_oid_t(2)))

                        .SetTableOid(catalog::table_oid_t(2))
                        .SetDatabaseOid(catalog::db_oid_t(0))
                        .SetScanPredicate(std::make_shared<parser::StarExpression>())
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

  auto hash_plan = hash_builder
                       .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col2", type::TypeId::INTEGER, false,
                                                                           catalog::col_oid_t(2)))
                       .AddHashKey(std::make_shared<parser::ColumnValueExpression>("table2", "col2"))
                       .AddChild(std::move(seq_scan_2))
                       .Build();

  EXPECT_EQ(PlanNodeType::HASH, hash_plan->GetPlanNodeType());
  EXPECT_EQ(1, hash_plan->GetChildrenSize());
  EXPECT_EQ(1, hash_plan->GetHashKeys().size());
  EXPECT_EQ(parser::ExpressionType::COLUMN_VALUE, hash_plan->GetHashKeys()[0]->GetExpressionType());

  std::vector<std::shared_ptr<parser::AbstractExpression>> expr_children;
  expr_children.push_back(std::make_shared<parser::ColumnValueExpression>("table1", "col1"));
  expr_children.push_back(std::make_shared<parser::ColumnValueExpression>("table2", "col2"));
  auto cmp_expression =
      std::make_shared<parser::ComparisonExpression>(parser::ExpressionType::COMPARE_EQUAL, std::move(expr_children));

  auto hash_join_plan = hash_join_builder.SetJoinType(LogicalJoinType::INNER)
                            .SetOutputSchema(PlanNodeTest::BuildOneColumnSchema("col1", type::TypeId::INTEGER, false,
                                                                                catalog::col_oid_t(1)))
                            .SetJoinPredicate(std::move(cmp_expression))
                            .AddChild(std::move(seq_scan_1))
                            .AddChild(std::move(hash_plan))
                            .Build();

  EXPECT_EQ(PlanNodeType::HASHJOIN, hash_join_plan->GetPlanNodeType());
  EXPECT_EQ(2, hash_join_plan->GetChildrenSize());
  EXPECT_EQ(LogicalJoinType::INNER, hash_join_plan->GetLogicalJoinType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, hash_join_plan->GetJoinPredicate()->GetExpressionType());
}
}  // namespace terrier::planner
