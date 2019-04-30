#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/comparison_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/postgresparser.h"
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
                       .AddHashKey(std::make_shared<parser::TupleValueExpression>("col2", "table2"))
                       .AddChild(std::move(seq_scan_2))
                       .Build();

  EXPECT_EQ(PlanNodeType::HASH, hash_plan->GetPlanNodeType());
  EXPECT_EQ(1, hash_plan->GetChildrenSize());
  EXPECT_EQ(1, hash_plan->GetHashKeys().size());
  EXPECT_EQ(parser::ExpressionType::VALUE_TUPLE, hash_plan->GetHashKeys()[0]->GetExpressionType());

  std::vector<std::shared_ptr<parser::AbstractExpression>> expr_children;
  expr_children.push_back(std::make_shared<parser::TupleValueExpression>("col1", "table1"));
  expr_children.push_back(std::make_shared<parser::TupleValueExpression>("col2", "table2"));
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
