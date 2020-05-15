#include <planner/plannodes/alter_plan_node.h>

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
#include "planner/plannodes/seq_scan_plan_node.h"
#include "test_util/test_harness.h"
#include "type/transient_value_factory.h"

namespace terrier::planner {

class PlanNodeTest : public TerrierTest {
 public:
  static std::unique_ptr<OutputSchema> BuildOneColumnSchema(std::string name, const type::TypeId type) {
    auto pred = std::make_unique<parser::ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true));
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

// NOLINTNEXTLINE
TEST(PlanNodeTest, AlterPlanTest) {
  catalog::table_oid_t table_oid(3);

  AlterPlanNode::Builder builder;
  auto default_val = parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(999));
  catalog::Schema::Column col("col1", type::TypeId::INTEGER, false, default_val);
  catalog::Schema::Column col2("col1", type::TypeId::INTEGER, false, default_val);
  catalog::Schema::Column col3("col3", type::TypeId::INTEGER, false, default_val);

  // test correctness of add_cmd helpers
  auto add_cmd = std::make_unique<AlterPlanNode::AddColumnCmd>(std::move(col), nullptr, nullptr, nullptr);

  EXPECT_EQ(add_cmd->GetType(), parser::AlterTableStatement::AlterType::AddColumn);

  // test that the equality check of add column works as intended
  auto add_cmd2 = std::make_unique<AlterPlanNode::AddColumnCmd>(std::move(col2), nullptr, nullptr, nullptr);
  EXPECT_EQ(*add_cmd, *add_cmd2);
  auto add_cmd3 = std::make_unique<AlterPlanNode::AddColumnCmd>(std::move(col3), nullptr, nullptr, nullptr);
  EXPECT_FALSE(*add_cmd == *add_cmd3);

  // test correctness of drop_cmd helpers
  auto drop_cmd = std::make_unique<AlterPlanNode::DropColumnCmd>("col", false, false, catalog::INVALID_COLUMN_OID);

  EXPECT_EQ(drop_cmd->GetType(), parser::AlterTableStatement::AlterType::DropColumn);
  EXPECT_EQ(drop_cmd->IsIfExist(), false);
  EXPECT_EQ(drop_cmd->IsCascade(), false);

  // test that the equality check of drop column works as intended
  auto drop_cmd2 = std::make_unique<AlterPlanNode::DropColumnCmd>("col", false, false, catalog::INVALID_COLUMN_OID);
  EXPECT_EQ(*drop_cmd, *drop_cmd2);

  // Make different variations of the drop column and make sure that they ar
  // enot equal to drop_cmd
  auto drop_cmd3 = std::make_unique<AlterPlanNode::DropColumnCmd>("col3", false, false, catalog::INVALID_COLUMN_OID);
  auto drop_cmd4 = std::make_unique<AlterPlanNode::DropColumnCmd>("col", true, false, catalog::INVALID_COLUMN_OID);
  auto drop_cmd5 = std::make_unique<AlterPlanNode::DropColumnCmd>("col", false, true, catalog::INVALID_COLUMN_OID);
  auto drop_cmd6 = std::make_unique<AlterPlanNode::DropColumnCmd>("col", false, false, catalog::col_oid_t(777));
  EXPECT_FALSE(*drop_cmd == *drop_cmd3);
  EXPECT_FALSE(*drop_cmd == *drop_cmd3);
  EXPECT_FALSE(*drop_cmd == *drop_cmd4);
  EXPECT_FALSE(*drop_cmd == *drop_cmd5);
  EXPECT_FALSE(*drop_cmd == *drop_cmd6);

  // test correctness of AlterPlanNode helpers
  std::vector<std::unique_ptr<AlterCmdBase>> cmds;
  cmds.push_back(std::move(add_cmd));
  auto plan =
      builder.SetTableOid(table_oid).SetCommands(std::move(cmds)).SetColumnOIDs({catalog::INVALID_COLUMN_OID}).Build();

  EXPECT_TRUE(plan != nullptr);
  EXPECT_EQ(plan->GetPlanNodeType(), PlanNodeType::ALTER);
  EXPECT_EQ(plan->GetTableOid(), table_oid);
  std::vector<catalog::col_oid_t> col_oids{catalog::INVALID_COLUMN_OID};
  EXPECT_EQ(plan->GetColumnOids(), col_oids);

  for (auto cmd : plan->GetCommands()) {
    EXPECT_EQ(cmd->GetType(), parser::AlterTableStatement::AlterType::AddColumn);
  }

  // Make different variations of the plan node and make sure that it is not
  // equal to the original plan
  for (int i = 0; i < 3; i++) {
    catalog::table_oid_t other_table_oid = table_oid;
    std::vector<catalog::col_oid_t> other_col_oids{catalog::INVALID_COLUMN_OID};
    std::vector<std::unique_ptr<AlterCmdBase>> other_cmds;
    other_cmds.push_back(std::move(add_cmd2));
    switch (i) {
      case 0:
        other_table_oid = catalog::table_oid_t(777);
        break;
      case 1:
        other_col_oids[0] = catalog::col_oid_t(777);
        break;
      case 2:
        other_cmds[0] = std::move(drop_cmd);
        break;
    }

    AlterPlanNode::Builder builder2;
    auto plan2 = builder2.SetTableOid(other_table_oid)
                     .SetCommands(std::move(other_cmds))
                     .SetColumnOIDs(std::move(other_col_oids))
                     .Build();
    EXPECT_NE(*plan, *plan2);
    EXPECT_NE(plan->Hash(), plan2->Hash());
  }
}

// Test creation of simple two table join.
// We construct the plan for the following query: SELECT table1.col1 FROM
// table1, table2 WHERE table1.col1 = table2.col2; NOLINTNEXTLINE
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
  catalog::namespace_oid_t ns_oid(2);
  std::string file_name = "/home/file.txt";
  char delimiter = ',';
  char quote = '"';
  char escape = '\\';
  std::vector<type::TypeId> value_types = {type::TypeId::INTEGER};

  planner::CSVScanPlanNode::Builder builder;
  auto plan = builder.SetDatabaseOid(db_oid)
                  .SetNamespaceOid(ns_oid)
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
  EXPECT_EQ(plan->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(plan->GetFileName(), file_name);
  EXPECT_EQ(plan->GetDelimiterChar(), delimiter);
  EXPECT_EQ(plan->GetQuoteChar(), quote);
  EXPECT_EQ(plan->GetEscapeChar(), escape);
  EXPECT_EQ(plan->GetValueTypes(), value_types);
  EXPECT_EQ(plan->IsForUpdate(), false);

  planner::CSVScanPlanNode::Builder builder2;
  auto plan2 = builder2.SetDatabaseOid(db_oid)
                   .SetNamespaceOid(ns_oid)
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
  for (int i = 0; i < 9; i++) {
    catalog::db_oid_t o_db_oid(1);
    catalog::namespace_oid_t o_ns_oid(2);
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
        o_value_types = {type::TypeId::VARCHAR};
        break;
      case 7:
        o_schema = PlanNodeTest::BuildOneColumnSchema("XXXX", type::TypeId::INTEGER);
        break;
      case 8:
        o_update = true;
        break;
    }

    planner::CSVScanPlanNode::Builder builder3;
    auto plan3 = builder3.SetDatabaseOid(o_db_oid)
                     .SetNamespaceOid(o_ns_oid)
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
}  // namespace terrier::planner
