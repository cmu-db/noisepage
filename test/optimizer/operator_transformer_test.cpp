#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_expression.h"
#include "optimizer/query_to_operator_transformer.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/postgresparser.h"
#include "storage/garbage_collector.h"
#include "traffic_cop/statement.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "util/data_table_benchmark_util.h"
#include "util/test_harness.h"

using std::make_tuple;

using std::unique_ptr;
using std::vector;

namespace terrier {

class OperatorTransformerTest : public TerrierTest {
 private:
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  storage::BlockStore block_store_{1000, 1000};
  catalog::Catalog *catalog_;
  storage::GarbageCollector *gc_;

 protected:
  std::string default_database_name_ = "test_db";
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_a_oid_;
  catalog::table_oid_t table_b_oid_;
  parser::PostgresParser parser_;
  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  binder::BindNodeVisitor *binder_;
  optimizer::QueryToOperatorTransformer *operator_transformer_;
  optimizer::OperatorExpression *operator_tree_;
  std::vector<optimizer::OpType> op_types_;

  void SetUpTables() {
    // Initialize the transaction manager and GC
    timestamp_manager_ = new transaction::TimestampManager;
    deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
    txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, &buffer_pool_,
                                                       true, DISABLED);
    gc_ = new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, nullptr);

    // new catalog requires txn_manage and block_store as parameters
    catalog_ = new catalog::Catalog(txn_manager_, &block_store_);

    // create database
    txn_ = txn_manager_->BeginTransaction();
    OPTIMIZER_LOG_DEBUG("Creating database %s", default_database_name_.c_str());
    db_oid_ = catalog_->CreateDatabase(txn_, default_database_name_, true);
    // commit the transactions
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    OPTIMIZER_LOG_DEBUG("database %s created!", default_database_name_.c_str());

    // get default values of the columns
    auto int_default = parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
    auto varchar_default = parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::VARCHAR));

    // create table A
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    // Create the column definition (no OIDs) for CREATE TABLE A(A1 int, a2 varchar)
    std::vector<catalog::Schema::Column> cols_a;
    cols_a.emplace_back("a1", type::TypeId::INTEGER, true, int_default);
    cols_a.emplace_back("a2", type::TypeId::VARCHAR, 20, true, varchar_default);
    auto schema_a = catalog::Schema(cols_a);

    table_a_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "a", schema_a);
    auto table_a = new storage::SqlTable(&block_store_, schema_a);
    EXPECT_TRUE(accessor_->SetTablePointer(table_a_oid_, table_a));

    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    accessor_.reset(nullptr);

    // create Table B
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);

    // Create the column definition (no OIDs) for CREATE TABLE b(b1 int, B2 varchar)
    std::vector<catalog::Schema::Column> cols_b;
    cols_b.emplace_back("b1", type::TypeId::INTEGER, true, int_default);
    cols_b.emplace_back("b2", type::TypeId::VARCHAR, 20, true, varchar_default);

    auto schema_b = catalog::Schema(cols_b);
    table_b_oid_ = accessor_->CreateTable(accessor_->GetDefaultNamespace(), "b", schema_b);
    auto table_b = new storage::SqlTable(&block_store_, schema_b);
    EXPECT_TRUE(accessor_->SetTablePointer(table_b_oid_, table_b));
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
  }

  void TearDownTables() {
    catalog_->TearDown();
    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete catalog_;
    delete gc_;
    delete txn_manager_;
    delete deferred_action_manager_;
    delete timestamp_manager_;
  }

  void SetUp() override {
    TerrierTest::SetUp();
    SetUpTables();
    // prepare for testing
    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(txn_, db_oid_);
    binder_ = new binder::BindNodeVisitor(std::move(accessor_), default_database_name_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    accessor_.reset(nullptr);
    delete binder_;
    delete operator_transformer_;
    delete operator_tree_;
    TearDownTables();
    TerrierTest::TearDown();
  }

  std::string GenerateOperatorAudit(optimizer::OperatorExpression *op) const {
    std::string info = "{";
    {
      info += "\"Op\":";
      info += "\"" + op->GetOp().GetName() + "\",";
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  auto logical_get = operator_tree_->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, InsertStatementSimpleTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string insert_sql = "INSERT INTO A (A1, A2) VALUES (5, \'MY DATA\')";

  std::string ref = R"({"Op":"LogicalInsert",})";

  auto parse_tree = parser_.BuildParseTree(insert_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  auto logical_insert = operator_tree_->GetOp().As<optimizer::LogicalInsert>();
  EXPECT_EQ(db_oid_, logical_insert->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_insert->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_insert->GetTableOid());
  EXPECT_EQ(std::vector<catalog::col_oid_t>({catalog::col_oid_t(2), catalog::col_oid_t(1)}),
            logical_insert->GetColumns());

  auto insert_value_a1 =
      logical_insert->GetValues().Get()[0][0][0].CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(insert_value_a1->GetValue().Type(), type::TypeId::INTEGER);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(insert_value_a1->GetValue()), 5);

  auto insert_value_a2 =
      logical_insert->GetValues().Get()[0][0][1].CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(insert_value_a2->GetValue().Type(), type::TypeId::VARCHAR);
  EXPECT_EQ(type::TransientValuePeeker::PeekVarChar(insert_value_a2->GetValue()), "MY DATA");
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, InsertStatementSelectTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string insert_sql = "INSERT INTO A (A1) SELECT B1 FROM B WHERE B1 > 0";

  std::string ref =
      "{\"Op\":\"LogicalInsertSelect\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(insert_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalInsertSelect
  auto logical_insert_select = operator_tree_->GetOp().As<optimizer::LogicalInsertSelect>();
  EXPECT_EQ(db_oid_, logical_insert_select->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_insert_select->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_insert_select->GetTableOid());

  // Test LogicalFilter
  auto logical_filter = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalFilter>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_GREATER_THAN,
            logical_filter->GetPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(update_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalUpdate
  auto logical_update = operator_tree_->GetOp().As<optimizer::LogicalUpdate>();
  EXPECT_EQ(db_oid_, logical_update->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_update->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_update->GetTableOid());

  auto update_clause = logical_update->GetUpdateClauses()[0].Get();
  EXPECT_EQ("a1", update_clause->GetColumnName());
  auto constant = update_clause->GetUpdateValue().CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(constant->GetValue().Type(), type::TypeId::INTEGER);
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(constant->GetValue()), 999);

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalAggregateAndGroupBy
  auto logical_aggregate_and_group_by = operator_tree_->GetOp().As<optimizer::LogicalAggregateAndGroupBy>();
  auto column_expr =
      logical_aggregate_and_group_by->GetColumns()[0].CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_EQ("b2", column_expr->GetColumnName());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get->GetNamespaceOid());
  EXPECT_EQ(table_b_oid_, logical_get->GetTableOid());
  EXPECT_FALSE(logical_get->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementDistinctTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT DISTINCT B1 FROM B WHERE B1 <= 5";

  std::string ref =
      "{\"Op\":\"LogicalDistinct\",\"Children\":"
      "[{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalFilter
  auto logical_filter = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalFilter>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
            logical_filter->GetPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalLimit
  auto logical_limit = operator_tree_->GetOp().As<optimizer::LogicalLimit>();
  EXPECT_EQ(2, logical_limit->GetLimit());
  EXPECT_EQ(1, logical_limit->GetOffset());
  EXPECT_EQ(optimizer::OrderByOrderingType::ASC, logical_limit->GetSortDirections()[0]);
  EXPECT_EQ(
      "b2",
      logical_limit->GetSortExpressions()[0].CastManagedPointerTo<parser::ColumnValueExpression>()->GetColumnName());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalLeftJoin
  auto logical_left_join = operator_tree_->GetOp().As<optimizer::LogicalLeftJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_LESS_THAN,
            logical_left_join->GetJoinPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_left->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_right->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalRightJoin
  auto logical_right_join = operator_tree_->GetOp().As<optimizer::LogicalRightJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_GREATER_THAN,
            logical_right_join->GetJoinPredicates()[0].GetExpr().Get()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_left->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_right->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalInnerJoin
  auto logical_inner_join = operator_tree_->GetOp().As<optimizer::LogicalInnerJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL,
            logical_inner_join->GetJoinPredicates()[0].GetExpr().Get()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_left->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_right->GetNamespaceOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementOuterJoinTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");
  std::string select_sql = "SELECT * FROM A FULL OUTER JOIN B ON A.A1 = B.B1";

  std::string ref =
      "{\"Op\":\"LogicalOuterJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalOuterJoin
  auto logical_outer_join = operator_tree_->GetOp().As<optimizer::LogicalOuterJoin>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL,
            logical_outer_join->GetJoinPredicates()[0].GetExpr().Get()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_left->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[1]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_right->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementStarNestedSelectTest) {
  // Check if star expression is correctly processed
  OPTIMIZER_LOG_DEBUG("Checking STAR expression in nested select from.");

  std::string select_sql =
      "SELECT * FROM A LEFT OUTER JOIN (SELECT * FROM B INNER JOIN A ON B1 = A1) AS C ON C.B2 = a.A1";

  std::string ref =
      "{\"Op\":\"LogicalLeftJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalQueryDerivedGet\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]}]}]}";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementNestedColumnTest) {
  // Check if nested select columns are correctly processed
  OPTIMIZER_LOG_DEBUG("Checking nested select columns.");

  std::string select_sql = "SELECT A1, (SELECT B2 FROM B where B2 IS NULL LIMIT 1) FROM A";

  std::string ref = R"({"Op":"LogicalGet",})";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementDiffTableSameSchemaTest) {
  // Test select from different table instances from the same physical schema
  std::string select_sql = "SELECT * FROM A, A as AA where A.a1 = AA.a2";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SelectStatementSelectListAliasTest) {
  OPTIMIZER_LOG_DEBUG("Checking select_list and table alias binding");

  std::string select_sql = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalInnerJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalGet\",}]},{\"Op\":\"LogicalGet\",}]}]}";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, DeleteStatementWhereTest) {
  std::string delete_sql = "DELETE FROM b WHERE b1 = 1 AND b2 > 'str'";

  std::string ref =
      "{\"Op\":\"LogicalDelete\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}";

  auto parse_tree = parser_.BuildParseTree(delete_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalDelete
  auto logical_delete = operator_tree_->GetOp().As<optimizer::LogicalDelete>();
  EXPECT_EQ(db_oid_, logical_delete->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_delete->GetNamespaceOid());
  EXPECT_EQ(table_b_oid_, logical_delete->GetTableOid());

  // Test LogicalGet
  auto logical_get = operator_tree_->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get->GetNamespaceOid());
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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

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

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  auto default_namespace_oid = accessor_->GetDefaultNamespace();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);

  // Test LogicalFilter
  auto logical_filter = operator_tree_->GetOp().As<optimizer::LogicalFilter>();
  EXPECT_EQ(parser::ExpressionType::COMPARE_IN, logical_filter->GetPredicates()[0].GetExpr()->GetExpressionType());

  // Test LogicalGet
  auto logical_get_left = operator_tree_->GetChildren()[0]->GetChildren()[0]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_left->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_left->GetNamespaceOid());
  EXPECT_EQ(table_a_oid_, logical_get_left->GetTableOid());
  EXPECT_FALSE(logical_get_left->GetIsForUpdate());

  auto logical_get_right = operator_tree_->GetChildren()[0]->GetChildren()[1]->GetOp().As<optimizer::LogicalGet>();
  EXPECT_EQ(db_oid_, logical_get_right->GetDatabaseOid());
  EXPECT_EQ(default_namespace_oid, logical_get_right->GetNamespaceOid());
  EXPECT_EQ(table_b_oid_, logical_get_right->GetTableOid());
  EXPECT_FALSE(logical_get_right->GetIsForUpdate());
}

// NOLINTNEXTLINE
TEST_F(OperatorTransformerTest, SubqueryComplexTest) {
  OPTIMIZER_LOG_DEBUG("Parsing sql query");

  std::string select_sql =
      "SELECT A.a1 FROM A WHERE A.a1 IN (SELECT b1 FROM B WHERE b1 = 2 AND "
      "b2 > (SELECT a1 FROM A WHERE a2 > 0)) AND EXISTS (SELECT b1 FROM B WHERE B.b1 = A.a1)";

  std::string ref =
      "{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalMarkJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalSingleJoin\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}]}]},{\"Op\":\"LogicalFilter\",\"Children\":"
      "[{\"Op\":\"LogicalGet\",}]}]}]}";

  auto parse_tree = parser_.BuildParseTree(select_sql);
  auto statement = parse_tree.GetStatements()[0];
  binder_->BindNameToNode(statement, &parse_tree);
  accessor_ = binder_->GetCatalogAccessor();
  operator_transformer_ = new optimizer::QueryToOperatorTransformer(std::move(accessor_));
  operator_tree_ = operator_transformer_->ConvertToOpExpression(statement, &parse_tree);
  auto info = GenerateOperatorAudit(operator_tree_);

  EXPECT_EQ(ref, info);
}

}  // namespace terrier
