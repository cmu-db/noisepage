#include <memory>
#include "parser/postgresparser.h"
#include "plan_node/create_plan_node.h"
#include "plan_node/drop_plan_node.h"
#include "util/test_harness.h"

namespace terrier::plan_node {

// NOLINTNEXTLINE
TEST(PlanNodeTests, CreateDatabasePlanTest) {
  parser::PostgresParser pgparser;
  auto stms = pgparser.BuildParseTree("CREATE DATABASE test");
  EXPECT_EQ(1, stms.size());
  auto *create_stmt = static_cast<parser::CreateStatement *>(stms[0].get());

  CreatePlanNode::Builder builder;
  auto plan = builder.SetFromCreateStatement(create_stmt).Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_STREQ("test", plan->GetDatabaseName().c_str());
  EXPECT_EQ(CreateType::DB, plan->GetCreateType());
}

// NOLINTNEXTLINE
TEST(PlanNodeTests, DropDatabasePlanTest) {
  parser::PostgresParser pgparser;
  auto stms = pgparser.BuildParseTree("DROP DATABASE test");
  EXPECT_EQ(1, stms.size());
  auto *drop_stmt = static_cast<parser::DropStatement *>(stms[0].get());

  DropPlanNode::Builder builder;
  auto plan = builder.SetFromDropStatement(drop_stmt).Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_STREQ("test", plan->GetDatabaseName().c_str());
  EXPECT_EQ(DropType::DB, plan->GetDropType());
}

}  // namespace terrier::plan_node
