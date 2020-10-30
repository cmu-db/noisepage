#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "binder/sql_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "loggers/optimizer_logger.h"
#include "main/db_main.h"
#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimization_context.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/physical_operators.h"
#include "optimizer/plan_generator.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/rules/implementation_rules.h"
#include "parser/create_statement.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression_defs.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "storage/garbage_collector.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace noisepage {

class OperatorTransformerTest : public TerrierTest {
 protected:
  std::string default_database_name_ = "test_db";
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_a_oid_;
  catalog::table_oid_t table_b_oid_;
  catalog::index_oid_t a_index_oid_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  binder::BindNodeVisitor *binder_;
  std::unique_ptr<optimizer::QueryToOperatorTransformer> operator_transformer_;
  std::unique_ptr<optimizer::AbstractOptimizerNode> operator_tree_;
  std::vector<optimizer::OpType> op_types_;
  std::unique_ptr<optimizer::TrivialCostModel> trivial_cost_model_;
  std::unique_ptr<optimizer::OptimizerContext> optimizer_context_;
  std::unique_ptr<optimizer::OptimizationContext> optimization_context_;

  std::unique_ptr<DBMain> db_main_;

  void SetUpTables() {
    // create database
    txn_ = txn_manager_->BeginTransaction();
    OPTIMIZER_LOG_DEBUG("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(txn_), default_database_name_, true);
    // commit the transactions
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    OPTIMIZER_LOG_DEBUG("database %s created!", default_database_name_.c_str());

    // get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TypeId::INTEGER);
    auto varchar_default = parser::ConstantValueExpression(type::TypeId::VARCHAR);

    // create table A
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("a1", type::TypeId::INTEGER, true, int_default);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, 20, true, varchar_default);
    auto schema_a = catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "a", schema_a);
    auto table_a = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema_a);
    EXPECT_TRUE(accessor_->SetTablePointer(table_a_oid_, table_a));

    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);

    // create Table B
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);

    // Create the column definition (no OIDs) for CREATE TABLE b(b1 int, B2 varchar)
    std::vector<catalog::Schema::Column> cols_b;
    cols_b.emplace_back("b1", type::TypeId::INTEGER, true, int_default);
    cols_b.emplace_back("b2", type::TypeId::VARCHAR, 20, true, varchar_default);

    auto schema_b = catalog::Schema(cols_b);
    table_b_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "b", schema_b);
    auto table_b = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema_b);
    EXPECT_TRUE(accessor_->SetTablePointer(table_b_oid_, table_b));
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

    // create index on a1
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);

    auto col = catalog::IndexSchema::Column(
        "a1", type::TypeId::INTEGER, true,
        parser::ColumnValueExpression(db_oid_, table_a_oid_, accessor_->GetSchema(table_a_oid_).GetColumn("a1").Oid()));
    auto idx_schema = catalog::IndexSchema({col}, storage::index::IndexType::BWTREE, true, true, false, true);
    a_index_oid_ = accessor_->CreateIndex(accessor_->GetDefaultNamespace(), table_a_oid_, "a_index", idx_schema);
    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(accessor_->GetIndexSchema(a_index_oid_));
    auto index = index_builder.Build();

    EXPECT_TRUE(accessor_->SetIndexPointer(a_index_oid_, index));
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void SetUp() override {
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    SetUpTables();
    // prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_oid_, DISABLED);
    binder_ = new binder::BindNodeVisitor(common::ManagedPointer(accessor_), db_oid_);

    trivial_cost_model_ = std::make_unique<optimizer::TrivialCostModel>();
    optimizer_context_ = std::make_unique<optimizer::OptimizerContext>(
        common::ManagedPointer(trivial_cost_model_).CastManagedPointerTo<optimizer::AbstractCostModel>());
    // TODO(WAN): footgun API
    optimizer_context_->SetTxn(txn_);
    optimizer_context_->SetCatalogAccessor(accessor_.get());
    optimizer_context_->SetStatsStorage(db_main_->GetStatsStorage().Get());

    auto *properties = new optimizer::PropertySet();
    optimization_context_ = std::make_unique<optimizer::OptimizationContext>(optimizer_context_.get(), properties);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
    delete binder_;
    operator_transformer_.reset(nullptr);
    operator_tree_.reset(nullptr);
  }

  std::string GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode> op) const {
    std::string info = "{";
    {
      info += "\"Op\":";
      info += "\"" + op->Contents()->GetName() + "\",";
      auto children = op->GetChildren();
      if (!children.empty()) {
        info += "\"Children\":[";
        {
          bool is_first = true;
          for (const auto &child : children) {
            if (is_first) {
              is_first = false;
            } else {
              info += ",";
            }
            info += GenerateOperatorAudit(child);
          }
        }
        info += "]";
      }
    }
    info += '}';
    return info;
  }
};

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementSimpleTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT A.A1 FROM A";

  std::string ref = R"({"Op":"LogicalGet",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  auto logical_get = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, InsertStatementSimpleTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string insert_sql = "INSERT INTO A (A1, A2) VALUES (5, \'MY DATA\')";

  std::string ref = R"({"Op":"LogicalInsert",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(insert_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  auto logical_insert = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalInsert>();
  EXPECT_EQ(db_oid_, logical_insert->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_insert->GetTableOid());
  EXPECT_EQ(std::vector<catalog::col_oid_t>({catalog::col_oid_t(1), catalog::col_oid_t(2)}),
            logical_insert->GetColumns());

  auto insert_value_a1 =
      logical_insert->GetValues().Get()[0][0][0].CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(insert_value_a1->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(insert_value_a1->Peek<int64_t>(), 5);

  auto insert_value_a2 =
      logical_insert->GetValues().Get()[0][0][1].CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(insert_value_a2->GetReturnValueType(), type::TypeId::VARCHAR);
  EXPECT_EQ(insert_value_a2->Peek<std::string_view>(), "MY DATA");
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, InsertStatementSelectTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string insert_sql = "INSERT INTO A (A1) SELECT B1 FROM B WHERE B1 > 0";

  std::string ref =
      "{\"Op\":\"LogicalInsertSelect\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(insert_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalInsertSelect
  auto logical_insert_select = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalInsertSelect>();
  EXPECT_EQ(db_oid_, logical_insert_select->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_insert_select->GetTableOid());

  // Test LogicalFilter
  auto logical_filter = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalFilter>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_GREATER_THAN,
            logical_filter->GetPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get =
      operator_tree_->GetChildren()[0]->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, UpdateStatementSimpleTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string update_sql = "UPDATE A SET A1 = 999 WHERE A1 >= 1";

  std::string ref =
      "{\"Op\":\"LogicalUpdate\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(update_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalUpdate
  auto logical_update = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalUpdate>();
  EXPECT_EQ(db_oid_, logical_update->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_update->GetTableOid());

  auto update_clause = logical_update->GetUpdateClauses()[0].Get();
  EXPECT_EQ("a1", update_clause->GetColumnName());
  auto constant = update_clause->GetUpdateValue().CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(constant->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(constant->Peek<int64_t>(), 999);

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
            logical_get->GetPredicates()[0].GetExpr()->GetExpressionType());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementAggregateTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT MAX(b1) FROM B GROUP BY b2";

  std::string ref =
      "{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalAggregateAndGroupBy
  auto logical_aggregate_and_group_by =
      operator_tree_->Contents()->GetContentsAs<optimizer::LogicalAggregateAndGroupBy>();
  auto column_expr =
      logical_aggregate_and_group_by->GetColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ("b2", column_expr->GetColumnName());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementDistinctTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT DISTINCT B1 FROM B WHERE B1 <= 5";

  std::string ref =
      "{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalFilter
  auto logical_filter = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalFilter>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
            logical_filter->GetPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get =
      operator_tree_->GetChildren()[0]->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementOrderByTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT b1 FROM B ORDER BY b2 ASC LIMIT 2 OFFSET 1";

  std::string ref =
      "{\"Op\":\"LogicalLimit\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalLimit
  auto logical_limit = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalLimit>();
  EXPECT_EQ(2, logical_limit->GetLimit());
  EXPECT_EQ(1, logical_limit->GetOffset());
  EXPECT_EQ(optimizer::OrderByOrderingType::ASC, logical_limit->GetSortDirections()[0]);
  EXPECT_EQ(
      "b2",
      logical_limit->GetSortExpressions()[0].CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementLeftJoinTest) {
  // Check if star expression is correctly processed
  OPTIMIZER_LOG_DEBUG("Checking STAR expression in select and subselect");

  std::string select_sql = "SELECT * FROM A LEFT OUTER JOIN B ON A.A1 < B.B1";

  std::string ref =
      "{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalLeftJoin
  auto logical_left_join = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalLeftJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_LESS_THAN,
            logical_left_join->GetJoinPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementRightJoinTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT * FROM A RIGHT JOIN B ON A.A1 > B.B1";

  std::string ref =
      "{\"Op\":\"LogicalRightJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalRightJoin
  auto logical_right_join = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalRightJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_GREATER_THAN,
            logical_right_join->GetJoinPredicates()[0].GetExpr().Get()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementInnerJoinTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT * FROM A Inner JOIN B ON A.A1 = B.B1";

  std::string ref =
      "{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalInnerJoin
  auto logical_inner_join = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalInnerJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL,
            logical_inner_join->GetJoinPredicates()[0].GetExpr().Get()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementLeftSemiJoinTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT * FROM A WHERE A1 in (SELECT B1 from B)";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementOuterJoinTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT * FROM A FULL OUTER JOIN B ON A.A1 = B.B1";

  std::string ref =
      "{\"Op\":\"LogicalOuterJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalOuterJoin
  auto logical_outer_join = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalOuterJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL,
            logical_outer_join->GetJoinPredicates()[0].GetExpr().Get()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementComplexTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql =
      "SELECT A.A1, B.B2 FROM A INNER JOIN b ON a.a1 = b.b1 WHERE a1 < 100 "
      "GROUP BY A.a1, B.b2 HAVING a1 > 50 ORDER BY a1";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementMarkJoinTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT * FROM A WHERE A1 = 0 AND A1 IN (SELECT B1 FROM B WHERE B1 IN (SELECT A1 FROM A))";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementStarNestedSelectTest) {
  // Check if star expression is correctly processed
  OPTIMIZER_LOG_DEBUG("Checking STAR expression in nested select from.");

  std::string select_sql =
      "SELECT * FROM A LEFT OUTER JOIN (SELECT * FROM B INNER JOIN A ON B1 = A1) AS C ON C.B1 = a.A1";

  std::string ref =
      "{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalQueryDerivedGet\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementNestedColumnTest) {
  // Check if nested select columns are correctly processed
  OPTIMIZER_LOG_DEBUG("Checking nested select columns.");

  std::string select_sql = "SELECT A1, (SELECT B2 FROM B where B2 IS NULL LIMIT 1) FROM A";

  std::string ref = R"({"Op":"LogicalGet",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementDiffTableSameSchemaTest) {
  // Test select from different table instances from the same physical schema
  std::string select_sql = "SELECT * FROM A, A as AA where A.a1 = AA.a1";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementSelectListAliasTest) {
  OPTIMIZER_LOG_DEBUG("Checking select_list and table alias binding");

  std::string select_sql = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DeleteStatementWhereTest) {
  std::string delete_sql = "DELETE FROM b WHERE b1 = 1 AND b2 > 'str'";

  std::string ref =
      "{\"Op\":\"LogicalDelete\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(delete_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalDelete
  auto logical_delete = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDelete>();
  EXPECT_EQ(db_oid_, logical_delete->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_delete->GetTableOid());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get->GetTableOid());
  EXPECT_TRUE(logical_get->GetIsForUpdate());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, logical_get->GetPredicates()[0].GetExpr()->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_GREATER_THAN,
            logical_get->GetPredicates()[1].GetExpr()->GetExpressionType());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, AggregateComplexTest) {
  // Check if nested select columns are correctly processed
  OPTIMIZER_LOG_DEBUG("Checking aggregate in subselect.");

  std::string select_sql = "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT MAX(b1) FROM B);";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalAggregateAndGroupBy\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, OperatorComplexTest) {
  // Check if nested select columns are correctly processed
  OPTIMIZER_LOG_DEBUG("Checking if operator expressions are correctly parsed.");

  std::string select_sql = "SELECT A.a1 FROM A WHERE 2 * A.a1 IN (SELECT b1+1 FROM B);";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test LogicalFilter
  auto logical_filter = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalFilter>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_IN, logical_filter->GetPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left =
      operator_tree_->GetChildren()[0]->GetChildren()[0]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right =
      operator_tree_->GetChildren()[0]->GetChildren()[1]->Contents()->GetContentsAs<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SubqueryComplexTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");

  std::string select_sql =
      "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND "
      "b1 > (SELECT a1 FROM A WHERE a1 > 0)) AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalSingleJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}]}]},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}]}";

  auto parse_tree = parser::PostgresParser::BuildParseTree(select_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateDatabaseTest) {
  std::string create_sql = "CREATE DATABASE C;";

  std::string ref = R"({"Op":"LogicalCreateDatabase",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateDatabase>();
  EXPECT_EQ("c", logical_create->GetDatabaseName());

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateDatabaseToPhysicalCreateDatabase rule;
  EXPECT_TRUE(rule.Check(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATEDATABASE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateDatabase");
  auto cd = op->GetContentsAs<optimizer::CreateDatabase>();
  EXPECT_EQ(cd->GetDatabaseName(), "c");

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_DATABASE);
  auto cdpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateDatabasePlanNode>();
  EXPECT_EQ(cdpn->GetDatabaseName(), "c");
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateTableTest) {
  std::string create_sql =
      "CREATE TABLE C ( C1 int NOT NULL, C2 varchar(255) NOT NULL UNIQUE, C3 INT REFERENCES A(A1), C4 INT DEFAULT 14 "
      "CHECK (C4<100), PRIMARY KEY(C1));";

  std::string ref = R"({"Op":"LogicalCreateTable",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto ns_oid = accessor_->GetDefaultNamespace();

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateTable>();
  EXPECT_EQ(ns_oid, logical_create->GetNamespaceOid());
  auto create_stmt = statement.CastManagedPointerTo<parser::CreateStatement>();
  EXPECT_EQ(logical_create->GetColumns(), create_stmt->GetColumns());
  EXPECT_EQ(logical_create->GetForeignKeys(), create_stmt->GetForeignKeys());

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateTableToPhysicalCreateTable rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATETABLE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateTable");
  auto ct = op->GetContentsAs<optimizer::CreateTable>();

  // TODO(WAN): for mysterious reasons, all names are lowercased
  EXPECT_EQ(ct->GetTableName(), "c");
  EXPECT_EQ(ct->GetColumns().size(), 4);

  // c1
  EXPECT_EQ(ct->GetColumns()[0]->GetColumnName(), "c1");
  EXPECT_EQ(ct->GetColumns()[0]->GetColumnType(), parser::ColumnDefinition::DataType::INT);
  EXPECT_FALSE(ct->GetColumns()[0]->IsNullable());
  EXPECT_FALSE(ct->GetColumns()[0]->IsUnique());
  EXPECT_TRUE(ct->GetColumns()[0]->IsPrimaryKey());
  EXPECT_EQ(ct->GetColumns()[0]->GetDefaultExpression(), nullptr);
  EXPECT_EQ(ct->GetColumns()[0]->GetCheckExpression(), nullptr);

  // c2
  EXPECT_EQ(ct->GetColumns()[1]->GetColumnName(), "c2");
  EXPECT_EQ(ct->GetColumns()[1]->GetColumnType(), parser::ColumnDefinition::DataType::VARCHAR);
  EXPECT_FALSE(ct->GetColumns()[1]->IsNullable());
  EXPECT_TRUE(ct->GetColumns()[1]->IsUnique());
  EXPECT_FALSE(ct->GetColumns()[1]->IsPrimaryKey());
  EXPECT_EQ(ct->GetColumns()[1]->GetDefaultExpression(), nullptr);
  EXPECT_EQ(ct->GetColumns()[1]->GetCheckExpression(), nullptr);

  // c3
  EXPECT_EQ(ct->GetColumns()[2]->GetColumnName(), "c3");
  EXPECT_EQ(ct->GetColumns()[2]->GetColumnType(), parser::ColumnDefinition::DataType::INT);
  EXPECT_TRUE(ct->GetColumns()[2]->IsNullable());
  EXPECT_FALSE(ct->GetColumns()[2]->IsUnique());
  EXPECT_FALSE(ct->GetColumns()[2]->IsPrimaryKey());
  EXPECT_EQ(ct->GetColumns()[2]->GetDefaultExpression(), nullptr);
  EXPECT_EQ(ct->GetColumns()[2]->GetCheckExpression(), nullptr);
  EXPECT_EQ(ct->GetForeignKeys().size(), 1);
  EXPECT_EQ(ct->GetForeignKeys()[0]->GetForeignKeySources()[0], "c3");
  EXPECT_EQ(ct->GetForeignKeys()[0]->GetForeignKeySinks()[0], "a1");
  EXPECT_EQ(ct->GetForeignKeys()[0]->GetForeignKeySinkTableName(), "a");

  // c4
  EXPECT_EQ(ct->GetColumns()[3]->GetColumnName(), "c4");
  EXPECT_EQ(ct->GetColumns()[3]->GetColumnType(), parser::ColumnDefinition::DataType::INT);
  EXPECT_TRUE(ct->GetColumns()[3]->IsNullable());
  EXPECT_FALSE(ct->GetColumns()[3]->IsUnique());
  EXPECT_FALSE(ct->GetColumns()[3]->IsPrimaryKey());

  auto def_expr = ct->GetColumns()[3]->GetDefaultExpression();
  EXPECT_EQ(def_expr->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(*(def_expr.CastManagedPointerTo<parser::ConstantValueExpression>()),
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(14)));

  auto chk_expr = ct->GetColumns()[3]->GetCheckExpression();
  EXPECT_EQ(chk_expr->GetExpressionType(), parser::ExpressionType::COMPARE_LESS_THAN);
  EXPECT_EQ(chk_expr->GetChild(0)->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(chk_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>()->GetTableName(), "c");
  EXPECT_EQ(chk_expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName(), "c4");
  EXPECT_EQ(chk_expr->GetChild(1)->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(*(chk_expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>()),
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(100)));

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_TABLE);
  auto ctpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateTablePlanNode>();

  EXPECT_EQ(ctpn->GetTableName(), "c");
  EXPECT_EQ(ctpn->GetSchema()->GetColumns().size(), 4);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[0].Name(), "c1");
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[1].Name(), "c2");
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[2].Name(), "c3");
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[3].Name(), "c4");
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[0].Type(), type::TypeId::INTEGER);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[1].Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[2].Type(), type::TypeId::INTEGER);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[3].Type(), type::TypeId::INTEGER);
  EXPECT_FALSE(ctpn->GetSchema()->GetColumns()[0].Nullable());
  EXPECT_FALSE(ctpn->GetSchema()->GetColumns()[1].Nullable());
  EXPECT_TRUE(ctpn->GetSchema()->GetColumns()[2].Nullable());
  EXPECT_TRUE(ctpn->GetSchema()->GetColumns()[3].Nullable());
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[0].AttrSize(), 4);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[1].MaxVarlenSize(), 255);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[2].AttrSize(), 4);
  EXPECT_EQ(ctpn->GetSchema()->GetColumns()[3].AttrSize(), 4);
  EXPECT_EQ(*ctpn->GetSchema()->GetColumns()[3].StoredExpression(),
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(14)));

  EXPECT_TRUE(ctpn->HasPrimaryKey());
  EXPECT_EQ(ctpn->GetPrimaryKey().primary_key_cols_.size(), 1);
  EXPECT_EQ(ctpn->GetPrimaryKey().primary_key_cols_[0], "c1");
  EXPECT_EQ(ctpn->GetPrimaryKey().constraint_name_, "c_pk_c1");
  EXPECT_EQ(ctpn->GetForeignKeys().size(), 1);
  EXPECT_EQ(ctpn->GetForeignKeys()[0].foreign_key_sources_.size(), 1);
  EXPECT_EQ(ctpn->GetForeignKeys()[0].foreign_key_sources_[0], "c3");
  EXPECT_EQ(ctpn->GetForeignKeys()[0].foreign_key_sinks_.size(), 1);
  EXPECT_EQ(ctpn->GetForeignKeys()[0].foreign_key_sinks_[0], "a1");
  EXPECT_EQ(ctpn->GetForeignKeys()[0].sink_table_name_, "a");
  EXPECT_EQ(ctpn->GetForeignKeys()[0].constraint_name_, "c_a_fkey");
  EXPECT_EQ(ctpn->GetForeignKeys()[0].del_action_, parser::FKConstrActionType::NOACTION);
  EXPECT_EQ(ctpn->GetForeignKeys()[0].upd_action_, parser::FKConstrActionType::NOACTION);
  EXPECT_EQ(ctpn->GetUniqueConstraints().size(), 1);
  EXPECT_EQ(ctpn->GetUniqueConstraints()[0].unique_cols_.size(), 1);
  EXPECT_EQ(ctpn->GetUniqueConstraints()[0].unique_cols_[0], "c2");
  EXPECT_EQ(ctpn->GetUniqueConstraints()[0].constraint_name_, "c_c2_key");
  EXPECT_EQ(ctpn->GetCheckConstraints().size(), 1);
  EXPECT_EQ(ctpn->GetCheckConstraints()[0].check_cols_.size(), 1);
  EXPECT_EQ(ctpn->GetCheckConstraints()[0].check_cols_[0], "c4");
  EXPECT_EQ(ctpn->GetCheckConstraints()[0].constraint_name_, "con_check");
  EXPECT_EQ(ctpn->GetCheckConstraints()[0].expr_type_, parser::ExpressionType::COMPARE_LESS_THAN);
  EXPECT_EQ(ctpn->GetCheckConstraints()[0].expr_value_,
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(100)));
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateIndexTest) {
  std::string create_sql = "CREATE UNIQUE INDEX idx_d ON A (lower(A2), A1);";
  std::string ref = R"({"Op":"LogicalCreateIndex",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto ns_oid = accessor_->GetDefaultNamespace();
  auto col_a1_oid = accessor_->GetSchema(table_a_oid_).GetColumn("a1").Oid();
  auto col_a2_oid = accessor_->GetSchema(table_a_oid_).GetColumn("a2").Oid();

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateIndex>();
  EXPECT_EQ(logical_create->GetTableOid(), table_a_oid_);
  EXPECT_EQ(logical_create->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(logical_create->GetIndexType(), parser::IndexType::BWTREE);
  EXPECT_EQ(logical_create->GetIndexName(), "idx_d");
  EXPECT_TRUE(logical_create->IsUnique());
  auto create_stmt = statement.CastManagedPointerTo<parser::CreateStatement>();
  EXPECT_EQ(logical_create->GetIndexAttr().size(), 2);
  EXPECT_EQ(logical_create->GetIndexAttr()[0], create_stmt->GetIndexAttributes()[0].GetExpression());
  auto col_attr = logical_create->GetIndexAttr()[1].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_attr->GetTableName(), "a");
  EXPECT_EQ(col_attr->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col_attr->GetColumnName(), "a1");
  EXPECT_EQ(col_attr->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(col_attr->GetDatabaseOid(), db_oid_);

  col_attr = logical_create->GetIndexAttr()[0]->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col_attr->GetTableName(), "a");
  EXPECT_EQ(col_attr->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col_attr->GetColumnName(), "a2");
  EXPECT_EQ(col_attr->GetColumnOid(), col_a2_oid);
  EXPECT_EQ(col_attr->GetDatabaseOid(), db_oid_);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateIndexToPhysicalCreateIndex rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATEINDEX);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateIndex");
  auto ci = op->GetContentsAs<optimizer::CreateIndex>();

  EXPECT_EQ(ci->GetIndexName(), "idx_d");
  EXPECT_EQ(ci->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ci->GetSchema()->GetColumns().size(), 2);

  auto child0 = ci->GetSchema()->GetColumns()[0].StoredExpression();
  EXPECT_EQ(child0->GetExpressionType(), parser::ExpressionType::FUNCTION);
  EXPECT_EQ(child0.CastManagedPointerTo<const parser::FunctionExpression>()->GetFuncName(), "lower");
  EXPECT_EQ(child0->GetChildren().size(), 1);
  EXPECT_EQ(child0->GetChildren()[0]->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(child0->GetChildren()[0].CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName(), "a2");
  EXPECT_EQ(child0->GetChildren()[0].CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnOid(), col_a2_oid);
  auto child1 = ci->GetSchema()->GetColumns()[1].StoredExpression();
  EXPECT_EQ(child1->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(child1.CastManagedPointerTo<const parser::ColumnValueExpression>()->GetColumnName(), "a1");
  EXPECT_EQ(child1.CastManagedPointerTo<const parser::ColumnValueExpression>()->GetColumnOid(), col_a1_oid);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_INDEX);
  auto cipn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateIndexPlanNode>();
  EXPECT_EQ(cipn->GetIndexName(), "idx_d");
  EXPECT_EQ(cipn->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(cipn->GetTableOid(), table_a_oid_);
  EXPECT_EQ(cipn->GetSchema()->GetColumns().size(), 2);
  EXPECT_EQ(cipn->GetSchema()->GetColumns(), ci->GetSchema()->GetColumns());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateFunctionTest) {
  std::string create_sql =
      "CREATE OR REPLACE FUNCTION increment ("
      " i DOUBLE"
      " )"
      " RETURNS DOUBLE AS $$ "
      " BEGIN RETURN i + 1; END; $$ "
      "LANGUAGE plpgsql;";

  std::string ref = R"({"Op":"LogicalCreateFunction",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto ns_oid = accessor_->GetDefaultNamespace();

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateFunction>();
  auto create_stmt = statement.CastManagedPointerTo<parser::CreateFunctionStatement>();
  EXPECT_EQ(logical_create->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(logical_create->GetFunctionName(), create_stmt->GetFuncName());
  EXPECT_EQ(logical_create->GetFunctionBody(), create_stmt->GetFuncBody());
  EXPECT_EQ(logical_create->GetReturnType(), create_stmt->GetFuncReturnType()->GetDataType());
  EXPECT_EQ(logical_create->GetUDFLanguage(), create_stmt->GetPLType());
  EXPECT_EQ(logical_create->IsReplace(), create_stmt->ShouldReplace());
  auto stmt_params = create_stmt->GetFuncParameters();
  auto op_params_names = logical_create->GetFunctionParameterNames();
  auto op_params_types = logical_create->GetFunctionParameterTypes();
  EXPECT_EQ(logical_create->GetParamCount(), stmt_params.size());
  for (size_t i = 0; i < create_stmt->GetFuncParameters().size(); i++) {
    EXPECT_EQ(op_params_names[i], stmt_params[i]->GetParamName());
    EXPECT_EQ(op_params_types[i], stmt_params[i]->GetDataType());
  }

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateFunctionToPhysicalCreateFunction rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATEFUNCTION);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateFunction");
  auto cf = op->GetContentsAs<optimizer::CreateFunction>();

  EXPECT_EQ(cf->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(cf->GetFunctionName(), create_stmt->GetFuncName());
  EXPECT_EQ(cf->GetFunctionBody(), create_stmt->GetFuncBody());
  EXPECT_EQ(cf->GetReturnType(), create_stmt->GetFuncReturnType()->GetDataType());
  EXPECT_EQ(cf->GetUDFLanguage(), create_stmt->GetPLType());
  EXPECT_EQ(cf->IsReplace(), create_stmt->ShouldReplace());
  EXPECT_EQ(cf->GetParamCount(), stmt_params.size());
  for (size_t i = 0; i < create_stmt->GetFuncParameters().size(); i++) {
    EXPECT_EQ(cf->GetFunctionParameterNames()[i], stmt_params[i]->GetParamName());
    EXPECT_EQ(cf->GetFunctionParameterTypes()[i], stmt_params[i]->GetDataType());
  }

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_FUNC);
  auto cfpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateFunctionPlanNode>();
  EXPECT_EQ(cfpn->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(cfpn->GetFunctionName(), create_stmt->GetFuncName());
  EXPECT_EQ(cfpn->GetFunctionBody(), create_stmt->GetFuncBody());
  EXPECT_EQ(cfpn->GetReturnType(), create_stmt->GetFuncReturnType()->GetDataType());
  EXPECT_EQ(cfpn->GetUDFLanguage(), create_stmt->GetPLType());
  EXPECT_EQ(cfpn->IsReplace(), create_stmt->ShouldReplace());
  EXPECT_EQ(cfpn->GetParamCount(), stmt_params.size());
  for (size_t i = 0; i < create_stmt->GetFuncParameters().size(); i++) {
    EXPECT_EQ(cfpn->GetFunctionParameterNames()[i], stmt_params[i]->GetParamName());
    EXPECT_EQ(cfpn->GetFunctionParameterTypes()[i], stmt_params[i]->GetDataType());
  }
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateNamespaceTest) {
  std::string create_sql = "CREATE SCHEMA e";

  std::string ref = R"({"Op":"LogicalCreateNamespace",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateNamespace>();
  EXPECT_EQ("e", logical_create->GetNamespaceName());

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateNamespaceToPhysicalCreateNamespace rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATENAMESPACE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateNamespace");
  auto cn = op->GetContentsAs<optimizer::CreateNamespace>();
  EXPECT_EQ(cn->GetNamespaceName(), "e");

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_NAMESPACE);
  auto cnpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateNamespacePlanNode>();
  EXPECT_EQ(cnpn->GetNamespaceName(), "e");
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateViewTest) {
  std::string create_sql = "CREATE VIEW a_view AS SELECT * FROM a WHERE a1 = 4;";

  std::string ref = R"({"Op":"LogicalCreateView",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto ns_oid = accessor_->GetDefaultNamespace();

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateView>();
  EXPECT_EQ(logical_create->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(logical_create->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(logical_create->GetViewName(), "a_view");

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateViewToPhysicalCreateView rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATEVIEW);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateView");
  auto cv = op->GetContentsAs<optimizer::CreateView>();
  EXPECT_EQ(cv->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(cv->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(cv->GetViewName(), "a_view");
  EXPECT_EQ(cv->GetViewQuery()->GetDepth(), 1);
  EXPECT_EQ(cv->GetViewQuery()->GetSelectTable()->GetTableName(), "a");
  EXPECT_EQ(cv->GetViewQuery()->GetSelectCondition()->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
  auto sc0 = cv->GetViewQuery()->GetSelectCondition()->GetChild(0);
  auto sc1 = cv->GetViewQuery()->GetSelectCondition()->GetChild(1);
  EXPECT_EQ(sc0->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(sc0.CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName(), "a1");
  EXPECT_EQ(sc1->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(*(sc1.CastManagedPointerTo<parser::ConstantValueExpression>()),
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(4)));

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_VIEW);
  auto cvpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateViewPlanNode>();
  EXPECT_EQ(cvpn->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(cvpn->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(cvpn->GetViewName(), "a_view");
  EXPECT_EQ(cvpn->GetViewQuery()->GetSelectTable()->GetTableName(), "a");
  EXPECT_EQ(cvpn->GetViewQuery()->GetSelectCondition()->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
  auto scpn0 = cvpn->GetViewQuery()->GetSelectCondition()->GetChild(0);
  auto scpn1 = cvpn->GetViewQuery()->GetSelectCondition()->GetChild(1);
  EXPECT_EQ(scpn0->GetExpressionType(), parser::ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(scpn0.CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName(), "a1");
  EXPECT_EQ(scpn1->GetExpressionType(), parser::ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(*(scpn1.CastManagedPointerTo<parser::ConstantValueExpression>()),
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(4)));
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, CreateTriggerTest) {
  std::string create_sql =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF a1 ON a "
      "FOR EACH ROW "
      "WHEN (OLD.a1 <> NEW.a1) "
      "EXECUTE PROCEDURE check_account_update(update_date);";
  std::string ref = R"({"Op":"LogicalCreateTrigger",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto ns_oid = accessor_->GetDefaultNamespace();
  auto col_a1_oid = accessor_->GetSchema(table_a_oid_).GetColumn("a1").Oid();

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical create
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalCreateTrigger>();
  auto create_stmt = statement.CastManagedPointerTo<parser::CreateStatement>();
  EXPECT_EQ(logical_create->GetTriggerName(), "check_update");
  EXPECT_EQ(logical_create->GetTriggerType(), create_stmt->GetTriggerType());
  EXPECT_EQ(logical_create->GetTriggerColumns().size(), create_stmt->GetTriggerColumns().size());
  EXPECT_EQ(logical_create->GetTriggerColumns().at(0), col_a1_oid);
  EXPECT_EQ(logical_create->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(logical_create->GetTableOid(), table_a_oid_);
  EXPECT_EQ(logical_create->GetTriggerFuncName(), create_stmt->GetTriggerFuncNames());
  EXPECT_EQ(logical_create->GetTriggerArgs(), create_stmt->GetTriggerArgs());
  EXPECT_EQ(logical_create->GetTriggerWhen()->GetChildrenSize(), 2);
  auto col1 = logical_create->GetTriggerWhen()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto col2 = logical_create->GetTriggerWhen()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(col1->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col2->GetTableOid(), table_a_oid_);
  EXPECT_EQ(col1->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(col2->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(col1->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(col2->GetDatabaseOid(), db_oid_);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalCreateTriggerToPhysicalCreateTrigger rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::CREATETRIGGER);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "CreateTrigger");
  auto ct = op->GetContentsAs<optimizer::CreateTrigger>();
  EXPECT_EQ(ct->GetTriggerName(), "check_update");
  EXPECT_EQ(ct->GetTriggerType(), create_stmt->GetTriggerType());
  EXPECT_EQ(ct->GetTriggerColumns().size(), create_stmt->GetTriggerColumns().size());
  EXPECT_EQ(ct->GetTriggerColumns().at(0), col_a1_oid);
  EXPECT_EQ(ct->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(ct->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ct->GetTriggerFuncName(), create_stmt->GetTriggerFuncNames());
  EXPECT_EQ(ct->GetTriggerArgs(), create_stmt->GetTriggerArgs());
  EXPECT_EQ(ct->GetTriggerWhen()->GetChildrenSize(), 2);
  auto ctc1 = ct->GetTriggerWhen()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto ctc2 = ct->GetTriggerWhen()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(ctc1->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ctc2->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ctc1->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(ctc2->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(ctc1->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(ctc2->GetDatabaseOid(), db_oid_);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::CREATE_TRIGGER);
  auto ctpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::CreateTriggerPlanNode>();
  EXPECT_EQ(ctpn->GetTriggerName(), "check_update");
  EXPECT_EQ(ctpn->GetTriggerType(), create_stmt->GetTriggerType());
  EXPECT_EQ(ctpn->GetTriggerColumns().size(), create_stmt->GetTriggerColumns().size());
  EXPECT_EQ(ctpn->GetTriggerColumns().at(0), col_a1_oid);
  EXPECT_EQ(ctpn->GetNamespaceOid(), ns_oid);
  EXPECT_EQ(ctpn->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ctpn->GetTriggerFuncName(), create_stmt->GetTriggerFuncNames());
  EXPECT_EQ(ctpn->GetTriggerArgs(), create_stmt->GetTriggerArgs());
  EXPECT_EQ(ctpn->GetTriggerWhen()->GetChildrenSize(), 2);
  auto ctpnc1 = ct->GetTriggerWhen()->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto ctpnc2 = ct->GetTriggerWhen()->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ(ctpnc1->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ctpnc2->GetTableOid(), table_a_oid_);
  EXPECT_EQ(ctpnc1->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(ctpnc2->GetColumnOid(), col_a1_oid);
  EXPECT_EQ(ctpnc1->GetDatabaseOid(), db_oid_);
  EXPECT_EQ(ctpnc2->GetDatabaseOid(), db_oid_);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DropDatabaseTest) {
  std::string drop_sql = "Drop DATABASE test_db;";

  std::string ref = R"({"Op":"LogicalDropDatabase",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical drop db
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropDatabase>();
  EXPECT_EQ(logical_create->GetDatabaseOID(), db_oid_);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropDatabaseToPhysicalDropDatabase rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPDATABASE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropDatabase");
  auto dd = op->GetContentsAs<optimizer::DropDatabase>();
  EXPECT_EQ(dd->GetDatabaseOID(), db_oid_);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_DATABASE);
  auto ddpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropDatabasePlanNode>();
  EXPECT_EQ(ddpn->GetDatabaseOid(), db_oid_);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DropTableTest) {
  std::string drop_sql = "DROP TABLE A;";

  std::string ref = R"({"Op":"LogicalDropTable",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical drop table
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropTable>();
  EXPECT_EQ(logical_create->GetTableOID(), table_a_oid_);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropTableToPhysicalDropTable rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPTABLE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropTable");
  auto dt = op->GetContentsAs<optimizer::DropTable>();
  EXPECT_EQ(dt->GetTableOID(), table_a_oid_);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_TABLE);
  auto dtpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropTablePlanNode>();
  EXPECT_EQ(dtpn->GetTableOid(), table_a_oid_);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DropIndexTest) {
  std::string drop_sql = "DROP index a_index ;";
  std::string ref = R"({"Op":"LogicalDropIndex",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical drop table
  auto logical_create = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropIndex>();
  EXPECT_EQ(logical_create->GetIndexOID(), a_index_oid_);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropIndexToPhysicalDropIndex rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPINDEX);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropIndex");
  auto di = op->GetContentsAs<optimizer::DropIndex>();
  EXPECT_EQ(di->GetIndexOID(), a_index_oid_);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_INDEX);
  auto dipn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropIndexPlanNode>();
  EXPECT_EQ(dipn->GetIndexOid(), a_index_oid_);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DropNamespaceIfExistsWhereExistTest) {
  std::string drop_sql = "DROP SCHEMA IF EXISTS public CASCADE;";
  std::string ref = R"({"Op":"LogicalDropNamespace",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  auto logical_drop = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropNamespace>();
  EXPECT_EQ(logical_drop->GetNamespaceOID(), catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropNamespaceToPhysicalDropNamespace rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPNAMESPACE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropNamespace");
  auto dn = op->GetContentsAs<optimizer::DropNamespace>();
  EXPECT_EQ(dn->GetNamespaceOID(), catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_NAMESPACE);
  auto dnpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropNamespacePlanNode>();
  EXPECT_EQ(dnpn->GetNamespaceOid(), catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DropNamespaceIfExistsWhereNotExistTest) {
  std::string drop_sql = "DROP SCHEMA IF EXISTS foo CASCADE;";
  std::string ref = R"({"Op":"LogicalDropNamespace",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  auto logical_drop = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropNamespace>();
  EXPECT_EQ(logical_drop->GetNamespaceOID(), catalog::INVALID_NAMESPACE_OID);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropNamespaceToPhysicalDropNamespace rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPNAMESPACE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropNamespace");
  auto dn = op->GetContentsAs<optimizer::DropNamespace>();
  EXPECT_EQ(dn->GetNamespaceOID(), catalog::INVALID_NAMESPACE_OID);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_NAMESPACE);
  auto dnpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropNamespacePlanNode>();
  EXPECT_EQ(dnpn->GetNamespaceOid(), catalog::INVALID_NAMESPACE_OID);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DISABLED_DropTriggerIfExistsWhereNotExistTest) {
  std::string drop_sql = "DROP TRIGGER IF EXISTS foo;";
  std::string ref = R"({"Op":"LogicalDropTrigger",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  auto logical_drop = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropTrigger>();
  EXPECT_EQ(logical_drop->GetTriggerOid(), catalog::INVALID_TRIGGER_OID);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropTriggerToPhysicalDropTrigger rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPVIEW);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropTrigger");
  auto dt = op->GetContentsAs<optimizer::DropTrigger>();
  EXPECT_EQ(dt->GetTriggerOid(), catalog::INVALID_TRIGGER_OID);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_VIEW);
  auto dtpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropTriggerPlanNode>();
  EXPECT_EQ(dtpn->GetTriggerOid(), catalog::INVALID_TRIGGER_OID);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DISABLED_DropViewIfExistsWhereNotExistTest) {
  std::string drop_sql = "DROP VIEW IF EXISTS foo;";
  std::string ref = R"({"Op":"LogicalDropView",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(drop_sql);
  auto statement = parse_tree->GetStatements()[0];

  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  auto logical_drop = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalDropView>();
  EXPECT_EQ(logical_drop->GetViewOid(), catalog::INVALID_VIEW_OID);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalDropViewToPhysicalDropView rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr.CastManagedPointerTo<optimizer::AbstractOptimizerNode>(), &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::DROPVIEW);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "DropView");
  auto dv = op->GetContentsAs<optimizer::DropView>();
  EXPECT_EQ(dv->GetViewOid(), catalog::INVALID_VIEW_OID);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::DROP_VIEW);
  auto dvpn = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::DropViewPlanNode>();
  EXPECT_EQ(dvpn->GetViewOid(), catalog::INVALID_VIEW_OID);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, AnalyzeTest) {
  std::string create_sql = "ANALYZE A(A1)";
  std::string ref = R"({"Op":"LogicalAnalyze",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  auto col_a1_oid = accessor_->GetSchema(table_a_oid_).GetColumn("a1").Oid();
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical analyze
  auto logical_analyze = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalAnalyze>();
  EXPECT_EQ(logical_analyze->GetColumns().size(), 1);
  EXPECT_EQ(logical_analyze->GetColumns().at(0), col_a1_oid);
  EXPECT_EQ(logical_analyze->GetTableOid(), table_a_oid_);
  EXPECT_EQ(operator_tree_->GetChildren().size(), 0);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalAnalyzeToPhysicalAnalyze rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr, &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::ANALYZE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "Analyze");
  auto physical_op = op->GetContentsAs<optimizer::Analyze>();
  EXPECT_EQ(physical_op->GetColumns().size(), 1);
  EXPECT_EQ(physical_op->GetColumns().at(0), col_a1_oid);
  EXPECT_EQ(physical_op->GetTableOid(), table_a_oid_);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::ANALYZE);
  auto analyze_plan = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::AnalyzePlanNode>();
  EXPECT_EQ(analyze_plan->GetColumnOids().size(), 1);
  EXPECT_EQ(analyze_plan->GetColumnOids().at(0), col_a1_oid);
  EXPECT_EQ(analyze_plan->GetTableOid(), table_a_oid_);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, AnalyzeTest2) {
  std::string create_sql = "ANALYZE A";
  std::string ref = R"({"Op":"LogicalAnalyze",})";

  auto parse_tree = parser::PostgresParser::BuildParseTree(create_sql);
  auto statement = parse_tree->GetStatements()[0];
  binder_->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);
  operator_transformer_ =
      std::make_unique<optimizer::QueryToOperatorTransformer>(common::ManagedPointer(accessor_), db_oid_);
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, common::ManagedPointer(parse_tree));
  auto info = GenerateOperatorAudit(common::ManagedPointer<optimizer::AbstractOptimizerNode>(operator_tree_));

  EXPECT_EQ(ref, info);

  // Test logical analyze
  auto logical_analyze = operator_tree_->Contents()->GetContentsAs<optimizer::LogicalAnalyze>();
  EXPECT_EQ(logical_analyze->GetColumns().size(), 0);
  EXPECT_EQ(logical_analyze->GetTableOid(), table_a_oid_);
  EXPECT_EQ(operator_tree_->GetChildren().size(), 0);

  auto optree_ptr = common::ManagedPointer(operator_tree_);
  auto *op_ctx = optimization_context_.get();
  std::vector<std::unique_ptr<optimizer::AbstractOptimizerNode>> transformed;

  optimizer::LogicalAnalyzeToPhysicalAnalyze rule;
  EXPECT_TRUE(rule.Check(optree_ptr, op_ctx));
  rule.Transform(optree_ptr, &transformed, op_ctx);

  auto op = transformed[0]->Contents();
  EXPECT_EQ(op->GetOpType(), optimizer::OpType::ANALYZE);
  EXPECT_TRUE(op->IsPhysical());
  EXPECT_EQ(op->GetName(), "Analyze");
  auto physical_op = op->GetContentsAs<optimizer::Analyze>();
  EXPECT_EQ(physical_op->GetColumns().size(), 0);
  EXPECT_EQ(physical_op->GetTableOid(), table_a_oid_);

  optimizer::PlanGenerator plan_generator{};
  optimizer::PropertySet property_set{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols{};
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols{};
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans{};
  std::vector<optimizer::ExprMap> children_expr_map{};

  auto plan_node =
      plan_generator.ConvertOpNode(txn_, accessor_.get(), transformed[0].get(), &property_set, required_cols,
                                   output_cols, std::move(children_plans), std::move(children_expr_map));
  EXPECT_EQ(plan_node->GetPlanNodeType(), planner::PlanNodeType::ANALYZE);
  auto analyze_plan = common::ManagedPointer(plan_node).CastManagedPointerTo<planner::AnalyzePlanNode>();
  EXPECT_EQ(analyze_plan->GetColumnOids().size(), 0);
  EXPECT_EQ(analyze_plan->GetTableOid(), table_a_oid_);
}
}  // namespace noisepage
