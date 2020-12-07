#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/default_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/type_cast_expression.h"
#include "parser/pg_trigger.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "test_util/test_harness.h"

namespace noisepage::parser {

class ParserTestBase : public TerrierTest {
 protected:
  /**
   * Initialization
   */
  void SetUp() override {
#if NOISEPAGE_USE_LOGGER
    parser_logger->set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::seconds(1));
#endif
  }

  void CheckTable(const std::unique_ptr<TableInfo> &table_info, const std::string &table_name) {
    EXPECT_EQ(table_info->GetTableName(), table_name);
  }
};

// NOLINTNEXTLINE
TEST_F(ParserTestBase, AnalyzeTest) {
  /**
   * We support:
   * ANALYZE table_name
   *
   * not supported:
   * ANALYZE VERBOSE ... : (rejected by parser)
   * ANALYZE table_name (column_name, ...) : (segfaults)
   */

  auto result = parser::PostgresParser::BuildParseTree("ANALYZE table_name;");
  auto analyze_stmt = result->GetStatement(0).CastManagedPointerTo<AnalyzeStatement>();
  EXPECT_EQ(analyze_stmt->GetType(), StatementType::ANALYZE);
  EXPECT_EQ(analyze_stmt->GetAnalyzeTable()->GetTableName(), "table_name");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, NOOPTest) {
  auto result = parser::PostgresParser::BuildParseTree(";");
  EXPECT_EQ(result->GetStatements().size(), 0);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CastTest) {
  auto result = parser::PostgresParser::BuildParseTree("SELECT CAST('100' AS INTEGER);");
  auto cast_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  EXPECT_EQ(cast_stmt->GetType(), StatementType::SELECT);
  EXPECT_EQ(cast_stmt->GetSelectColumns().at(0)->GetExpressionType(), ExpressionType::OPERATOR_CAST);
  EXPECT_EQ(cast_stmt->GetSelectColumns().at(0)->GetReturnValueType(), type::TypeId::INTEGER);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CopyTest) {
  auto result = parser::PostgresParser::BuildParseTree("COPY foo FROM STDIN WITH BINARY;");
  auto copy_stmt = result->GetStatement(0).CastManagedPointerTo<CopyStatement>();
  EXPECT_EQ(copy_stmt->GetType(), StatementType::COPY);
  EXPECT_EQ(copy_stmt->GetExternalFileFormat(), ExternalFileFormat::BINARY);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CreateFunctionTest) {
  {
    std::string query =
        "CREATE OR REPLACE FUNCTION increment ("
        " i DOUBLE"
        " )"
        " RETURNS DOUBLE AS $$ "
        " BEGIN RETURN i + 1; END; $$ "
        "LANGUAGE plpgsql;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto stmt = result->GetStatement(0).CastManagedPointerTo<CreateFunctionStatement>();
    auto func_params = stmt->GetFuncParameters();
    EXPECT_EQ(stmt->GetFuncName(), "increment");
    EXPECT_EQ(stmt->GetFuncReturnType()->GetDataType(), BaseFunctionParameter::DataType::DOUBLE);
    EXPECT_EQ(func_params[0]->GetParamName(), "i");
    EXPECT_EQ(func_params[0]->GetDataType(), BaseFunctionParameter::DataType::DOUBLE);
  }

  {
    std::string query =
        "CREATE FUNCTION increment1 ("
        " i DOUBLE, j DOUBLE"
        " )"
        " RETURNS DOUBLE AS $$ "
        " BEGIN RETURN i + j; END; $$ "
        "LANGUAGE plpgsql;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto stmt = result->GetStatement(0).CastManagedPointerTo<CreateFunctionStatement>();
    auto func_params = stmt->GetFuncParameters();
    EXPECT_EQ(stmt->GetFuncName(), "increment1");
    EXPECT_EQ(stmt->GetFuncReturnType()->GetDataType(), BaseFunctionParameter::DataType::DOUBLE);
    EXPECT_EQ(func_params[0]->GetParamName(), "i");
    EXPECT_EQ(func_params[0]->GetDataType(), BaseFunctionParameter::DataType::DOUBLE);
    EXPECT_EQ(func_params[1]->GetParamName(), "j");
    EXPECT_EQ(func_params[1]->GetDataType(), BaseFunctionParameter::DataType::DOUBLE);
  }

  {
    std::string query =
        "CREATE OR REPLACE FUNCTION increment2 ("
        " i INT, j INT"
        " )"
        " RETURNS INT AS $$ "
        "BEGIN RETURN i + 1; END; $$ "
        "LANGUAGE plpgsql;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto stmt = result->GetStatement(0).CastManagedPointerTo<CreateFunctionStatement>();
    auto func_params = stmt->GetFuncParameters();
    EXPECT_EQ(stmt->GetFuncName(), "increment2");
    EXPECT_EQ(stmt->GetFuncReturnType()->GetDataType(), BaseFunctionParameter::DataType::INT);
    EXPECT_EQ(func_params[0]->GetParamName(), "i");
    EXPECT_EQ(func_params[0]->GetDataType(), BaseFunctionParameter::DataType::INT);
    EXPECT_EQ(func_params[1]->GetParamName(), "j");
    EXPECT_EQ(func_params[1]->GetDataType(), BaseFunctionParameter::DataType::INT);
  }

  {
    std::string query =
        "CREATE OR REPLACE FUNCTION return_varchar ("
        " i VARCHAR"
        " )"
        " RETURNS VARCHAR AS $$ "
        "BEGIN RETURN 'foo'; END; $$ "
        "LANGUAGE plpgsql;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto stmt = result->GetStatement(0).CastManagedPointerTo<CreateFunctionStatement>();
    auto func_params = stmt->GetFuncParameters();
    EXPECT_EQ(stmt->GetFuncName(), "return_varchar");
    EXPECT_EQ(stmt->GetFuncReturnType()->GetDataType(), BaseFunctionParameter::DataType::VARCHAR);
    EXPECT_EQ(func_params[0]->GetParamName(), "i");
    EXPECT_EQ(func_params[0]->GetDataType(), BaseFunctionParameter::DataType::VARCHAR);
  }

  {
    std::string query =
        "CREATE OR REPLACE FUNCTION return_text ("
        " i TEXT"
        " )"
        " RETURNS TEXT AS $$ "
        "BEGIN RETURN 'foo'; END; $$ "
        "LANGUAGE plpgsql;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto stmt = result->GetStatement(0).CastManagedPointerTo<CreateFunctionStatement>();
    auto func_params = stmt->GetFuncParameters();
    EXPECT_EQ(stmt->GetFuncName(), "return_text");
    EXPECT_EQ(stmt->GetFuncReturnType()->GetDataType(), BaseFunctionParameter::DataType::TEXT);
    EXPECT_EQ(func_params[0]->GetParamName(), "i");
    EXPECT_EQ(func_params[0]->GetDataType(), BaseFunctionParameter::DataType::TEXT);
  }

  {
    std::string query =
        "CREATE OR REPLACE FUNCTION return_bool ("
        " i BOOL"
        " )"
        " RETURNS BOOL AS $$ "
        "BEGIN RETURN false; END; $$ "
        "LANGUAGE plpgsql;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto stmt = result->GetStatement(0).CastManagedPointerTo<CreateFunctionStatement>();
    auto func_params = stmt->GetFuncParameters();
    EXPECT_EQ(stmt->GetFuncName(), "return_bool");
    EXPECT_EQ(stmt->GetFuncReturnType()->GetDataType(), BaseFunctionParameter::DataType::BOOL);
    EXPECT_EQ(func_params[0]->GetParamName(), "i");
    EXPECT_EQ(func_params[0]->GetDataType(), BaseFunctionParameter::DataType::BOOL);
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CreateIndexTest) {
  std::string query = "CREATE INDEX IDX_ORDER ON oorder ((O_W_ID - 2), (O + W + O));";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  EXPECT_EQ(create_stmt->GetCreateType(), CreateStatement::kIndex);
  EXPECT_EQ(create_stmt->GetIndexName(), "idx_order");
  EXPECT_EQ(create_stmt->GetTableName(), "oorder");
  EXPECT_EQ(create_stmt->GetIndexAttributes().size(), 2);
  auto ia1 = create_stmt->GetIndexAttributes()[0].GetExpression();
  EXPECT_EQ(ia1->GetExpressionType(), ExpressionType::OPERATOR_MINUS);
  auto ia1l = ia1->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(ia1l->GetColumnName(), "o_w_id");
  auto ia1r = ia1->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ(ia1r->Peek<int64_t>(), 2);
  auto ia2 = create_stmt->GetIndexAttributes()[1].GetExpression();
  EXPECT_EQ(ia2->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
  auto ia2l = ia2->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(ia2l->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
  auto ia2ll = ia2l->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  auto ia2lr = ia2l->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  auto ia2r = ia2->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(ia2ll->GetColumnName(), "o");
  EXPECT_EQ(ia2lr->GetColumnName(), "w");
  EXPECT_EQ(ia2r->GetColumnName(), "o");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CreateTableTest) {
  std::string query =
      "CREATE TABLE Foo ("
      "id INT NOT NULL UNIQUE, "
      "b VARCHAR(255), "
      "c INT8, "
      "d INT2, "
      "e TIMESTAMP, "
      "f BOOL, "
      "g BPCHAR, "
      "h DOUBLE, "
      "i REAL, "
      "j NUMERIC, "
      "k TEXT, "
      "l TINYINT, "
      "m VARBINARY, "
      "n DATE, "
      "PRIMARY KEY (id),"
      "FOREIGN KEY (c_id) REFERENCES country (cid));";

  auto result = parser::PostgresParser::BuildParseTree(query);

  query = "CREATE TABLE Foo (id BAZ, PRIMARY KEY (id));";
  EXPECT_THROW(parser::PostgresParser::BuildParseTree(query), ParserException);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CreateViewTest) {
  auto result = parser::PostgresParser::BuildParseTree("CREATE VIEW foo AS SELECT * FROM bar WHERE baz = 1;");
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  EXPECT_EQ(create_stmt->GetViewName(), "foo");
  EXPECT_NE(create_stmt->GetViewQuery(), nullptr);
  auto view_query = create_stmt->GetViewQuery();
  EXPECT_EQ(view_query->GetSelectTable()->GetTableName(), "bar");
  EXPECT_EQ(view_query->GetSelectColumns().size(), 1);
  EXPECT_NE(view_query->GetSelectCondition(), nullptr);
  EXPECT_EQ(view_query->GetSelectCondition()->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(view_query->GetSelectCondition()->GetChildrenSize(), 2);
  auto left_child = view_query->GetSelectCondition()->GetChild(0);
  EXPECT_EQ(left_child->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(left_child.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "baz");
  auto right_child = view_query->GetSelectCondition()->GetChild(1);
  EXPECT_EQ(right_child->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(right_child.CastManagedPointerTo<ConstantValueExpression>()->Peek<int64_t>(), 1);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, DropDBTest) {
  auto result = parser::PostgresParser::BuildParseTree("DROP DATABASE test_db;");
  EXPECT_EQ(result->GetStatements().size(), 1);

  auto drop_stmt = result->GetStatement(0).CastManagedPointerTo<DropStatement>();
  EXPECT_EQ(drop_stmt->GetDropType(), DropStatement::DropType::kDatabase);
  EXPECT_EQ(drop_stmt->GetDatabaseName(), "test_db");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, DropIndexTest) {
  auto result = parser::PostgresParser::BuildParseTree("DROP INDEX foo;");
  EXPECT_EQ(result->GetStatements().size(), 1);

  auto drop_stmt = result->GetStatement(0).CastManagedPointerTo<DropStatement>();
  EXPECT_EQ(drop_stmt->GetDropType(), DropStatement::DropType::kIndex);
  EXPECT_EQ(drop_stmt->GetIndexName(), "foo");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, DropSchemaTest) {
  auto result = parser::PostgresParser::BuildParseTree("DROP SCHEMA IF EXISTS foo CASCADE;");
  EXPECT_EQ(result->GetStatements().size(), 1);

  auto drop_stmt = result->GetStatement(0).CastManagedPointerTo<DropStatement>();
  EXPECT_EQ(drop_stmt->GetDropType(), DropStatement::DropType::kSchema);
  EXPECT_EQ(drop_stmt->GetNamespaceName(), "foo");
  EXPECT_TRUE(drop_stmt->IsCascade());
  EXPECT_TRUE(drop_stmt->IsIfExists());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, DropTableTest) {
  auto result = parser::PostgresParser::BuildParseTree("DROP TABLE test_db;");
  EXPECT_EQ(result->GetStatements().size(), 1);

  auto drop_stmt = result->GetStatement(0).CastManagedPointerTo<DropStatement>();
  EXPECT_EQ(drop_stmt->GetDropType(), DropStatement::DropType::kTable);
  EXPECT_EQ(drop_stmt->GetTableName(), "test_db");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, ExecuteTest) {
  auto result = parser::PostgresParser::BuildParseTree("EXECUTE prepared_statement_name;");
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::EXECUTE);

  result = parser::PostgresParser::BuildParseTree("EXECUTE prepared_statement_name(1, 2.0)");
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::EXECUTE);

  result = parser::PostgresParser::BuildParseTree("EXECUTE prepared_statement_name(1, 'arg_2', 3.0)");
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::EXECUTE);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, ExplainTest) {
  auto result = parser::PostgresParser::BuildParseTree("EXPLAIN SELECT * FROM foo;");
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::EXPLAIN);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, GarbageTest) {
  EXPECT_THROW(parser::PostgresParser::BuildParseTree("blarglesnarf"), ParserException);
  EXPECT_THROW(parser::PostgresParser::BuildParseTree("SELECT;"), ParserException);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, InsertTest) {
  auto result = parser::PostgresParser::BuildParseTree("INSERT INTO foo VALUES (1, 2, 3), (4, 5, 6);");
  auto insert_stmt = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
  EXPECT_EQ(insert_stmt->GetInsertionTable()->GetTableName(), "foo");
  EXPECT_EQ(insert_stmt->GetInsertColumns()->size(), 0);

  result = parser::PostgresParser::BuildParseTree("INSERT INTO foo (id,bar,entry) VALUES (DEFAULT, 2, 3);");
  insert_stmt = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
  EXPECT_EQ(insert_stmt->GetInsertionTable()->GetTableName(), "foo");
  EXPECT_EQ(insert_stmt->GetInsertColumns()->size(), 3);
  EXPECT_EQ((*insert_stmt->GetValues())[0][0]->GetExpressionType(), ExpressionType::VALUE_DEFAULT);

  result = parser::PostgresParser::BuildParseTree("INSERT INTO foo VALUES (1, 1+1, 1+3);");
  insert_stmt = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
  EXPECT_EQ(insert_stmt->GetInsertionTable()->GetTableName(), "foo");
  EXPECT_EQ(insert_stmt->GetInsertColumns()->size(), 0);
  EXPECT_EQ((*insert_stmt->GetValues())[0][1]->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, PrepareTest) {
  {
    auto result = parser::PostgresParser::BuildParseTree("PREPARE insert_plan AS INSERT INTO table_name VALUES($1);");
    EXPECT_EQ(result->GetStatements().size(), 1);
    EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::PREPARE);
    auto prepare_stmt = result->GetStatement(0).CastManagedPointerTo<PrepareStatement>();
    EXPECT_EQ(prepare_stmt->GetName(), "insert_plan");
    // TODO(pakhtar)
    // - check table name == table_name
    // - check value_idx == 0
  }

  {
    auto result =
        parser::PostgresParser::BuildParseTree("PREPARE insert_plan (INT) AS INSERT INTO table_name VALUES($1);");
    EXPECT_EQ(result->GetStatements().size(), 1);
    EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::PREPARE);
    auto prepare_stmt = result->GetStatement(0).CastManagedPointerTo<PrepareStatement>();
    EXPECT_EQ(prepare_stmt->GetName(), "insert_plan");
    // TODO(pakhtar)
    // - check table name == table_name
    // - check value_idx == 0
    // - can we check the type?
  }

  {
    auto result = parser::PostgresParser::BuildParseTree(
        "PREPARE select_stmt_plan (INT) AS SELECT column_name FROM table_name WHERE id=$1;");
    EXPECT_EQ(result->GetStatements().size(), 1);
    EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::PREPARE);
    auto prepare_stmt = result->GetStatement(0).CastManagedPointerTo<PrepareStatement>();
    EXPECT_EQ(prepare_stmt->GetName(), "select_stmt_plan");
    // TODO(pakhtar)
    // - assert "column_name"
    // - assert "table_name"
    // - assert value_idx == 0
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, SelectTest) {
  auto result = parser::PostgresParser::BuildParseTree("SELECT * FROM foo;");

  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::SELECT);

  auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  EXPECT_EQ(select_stmt->GetSelectTable()->GetTableName(), "foo");
  // CheckTable(select_stmt->from_->table_info_, std::string("foo"));
  EXPECT_EQ(select_stmt->GetSelectColumns()[0]->GetExpressionType(), ExpressionType::TABLE_STAR);

  auto result2 = parser::PostgresParser::BuildParseTree("SELECT id FROM foo LIMIT 1 OFFSET 1;");
  EXPECT_EQ(result2->GetStatement(0)->GetType(), StatementType::SELECT);
  auto select_stmt_2 = result2->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  EXPECT_EQ(select_stmt_2->GetSelectLimit()->GetLimit(), 1);
  EXPECT_EQ(select_stmt_2->GetSelectLimit()->GetOffset(), 1);

  EXPECT_NE(*select_stmt, *select_stmt_2);
  EXPECT_NE(select_stmt->Hash(), select_stmt_2->Hash());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, SelectUnionTest) {
  auto result = parser::PostgresParser::BuildParseTree("SELECT * FROM foo UNION SELECT * FROM bar;");
  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::SELECT);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, SetTest) {
  auto result = parser::PostgresParser::BuildParseTree("SET var_name TO 1;");
  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::VARIABLE_SET);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, SubqueryTest) {
  auto result =
      parser::PostgresParser::BuildParseTree("SELECT * FROM foo WHERE id IN (SELECT id FROM foo WHERE x > 400)");
  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::SELECT);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, TruncateTest) {
  auto result = parser::PostgresParser::BuildParseTree("TRUNCATE TABLE test_db;");
  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::DELETE);

  auto delete_stmt = result->GetStatement(0).CastManagedPointerTo<DeleteStatement>();
  EXPECT_EQ(delete_stmt->GetDeletionTable()->GetTableName(), "test_db");
  EXPECT_EQ(delete_stmt->GetDeleteCondition(), nullptr);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, UpdateTest) {
  auto result = parser::PostgresParser::BuildParseTree("UPDATE students SET grade = 1.0;");

  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::UPDATE);

  auto update_stmt = result->GetStatement(0).CastManagedPointerTo<UpdateStatement>();
  EXPECT_EQ(update_stmt->GetUpdateTable()->GetTableName(), "students");
  // check expression here
  EXPECT_EQ(update_stmt->GetUpdateCondition(), nullptr);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OperatorTest) {
  {
    std::string query = "SELECT 10+10 AS Addition;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
  }

  {
    std::string query = "SELECT 15-721 AS Subtraction;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_MINUS);
  }

  {
    std::string query = "SELECT 5*7 AS Multiplication;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_MULTIPLY);
  }

  {
    std::string query = "SELECT 1/2 AS Division;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_DIVIDE);
  }

  {
    std::string query = "SELECT 15||213 AS Concatenation;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_CONCAT);
  }

  {
    std::string query = "SELECT 4%2 AS Mod;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_MOD);
  }

  {
    std::string query = "SELECT CAST('100' AS INTEGER) AS Casting;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectColumns().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_CAST);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::INTEGER);
  }

  {
    std::string query = "SELECT * FROM foo WHERE NOT id = 1;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_NOT);
  }

  {
    std::string query = "SELECT * FROM foo WHERE id IS NULL;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_IS_NULL);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }

  {
    // Coverage for NullNodeTransform
    std::string query = "SELECT * FROM foo WHERE 0 IS NULL;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_IS_NULL);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
    EXPECT_EQ(expr->GetChild(0)->GetExpressionType(), ExpressionType::VALUE_CONSTANT);

    query = "SELECT * FROM foo WHERE 0*1 IS NULL;";
    result = parser::PostgresParser::BuildParseTree(query);
    select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_IS_NULL);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);

    query = "SELECT * FROM foo WHERE ? IS NULL;";
    result = parser::PostgresParser::BuildParseTree(query);
    select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_IS_NULL);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
    EXPECT_EQ(expr->GetChild(0)->GetExpressionType(), ExpressionType::VALUE_PARAMETER);
  }

  {
    std::string query = "SELECT * FROM foo WHERE EXISTS (SELECT * from bar);";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::OPERATOR_EXISTS);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, CompareTest) {
  {
    std::string query = "SELECT * FROM foo WHERE id < 10;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COMPARE_LESS_THAN);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }

  {
    std::string query = "SELECT * FROM foo WHERE id <= 10;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }

  {
    std::string query = "SELECT * FROM foo WHERE id >= 10;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }

  {
    std::string query = "SELECT * FROM foo WHERE str ~~ '%test%';";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COMPARE_LIKE);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }

  {
    std::string query = "SELECT * FROM foo WHERE str !~~ '%test%';";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COMPARE_NOT_LIKE);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }

  {
    std::string query = "SELECT * FROM foo WHERE str IS DISTINCT FROM 'test';";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto expr = select_stmt->GetSelectCondition();
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COMPARE_IS_DISTINCT_FROM);
    EXPECT_EQ(expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  }
}

/*
 * All the converted old tests from postgresparser_test.cpp are below.
 * Notable differences:
 * 1. successfully building the parse tree = the statement is valid, no more is_valid checks
 */

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldBasicTest) {
  std::string query = "SELECT * FROM foo;";

  auto result = parser::PostgresParser::BuildParseTree(query);
  EXPECT_EQ(1, result->GetStatements().size());
  EXPECT_EQ(StatementType::SELECT, result->GetStatement(0)->GetType());

  // cast result to derived class pointers
  auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  EXPECT_EQ("foo", statement->GetSelectTable()->GetTableName());
  EXPECT_EQ(ExpressionType::TABLE_STAR, statement->GetSelectColumns()[0]->GetExpressionType());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldAggTest) {
  std::string query;

  {
    query = "SELECT COUNT(*) FROM foo;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    EXPECT_EQ(1, result->GetStatements().size());
    EXPECT_EQ(StatementType::SELECT, result->GetStatement(0)->GetType());

    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    EXPECT_EQ("foo", statement->GetSelectTable()->GetTableName());
    EXPECT_EQ(ExpressionType::AGGREGATE_COUNT, statement->GetSelectColumns()[0]->GetExpressionType());
  }

  {
    query = "SELECT COUNT(DISTINCT id) FROM foo;";
    auto result = parser::PostgresParser::BuildParseTree(query);

    EXPECT_EQ(1, result->GetStatements().size());
    EXPECT_EQ(StatementType::SELECT, result->GetStatement(0)->GetType());

    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    EXPECT_EQ("foo", statement->GetSelectTable()->GetTableName());
    EXPECT_EQ(ExpressionType::AGGREGATE_COUNT, statement->GetSelectColumns()[0]->GetExpressionType());

    auto agg_expression = statement->GetSelectColumns()[0].CastManagedPointerTo<AggregateExpression>();
    EXPECT_TRUE(agg_expression->IsDistinct());
    auto child_expr = statement->GetSelectColumns()[0]->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
    EXPECT_EQ("id", child_expr->GetColumnName());
  }

  {
    query = "SELECT MAX(*) FROM foo;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    EXPECT_EQ(1, result->GetStatements().size());
    EXPECT_EQ(StatementType::SELECT, result->GetStatement(0)->GetType());

    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    EXPECT_EQ("foo", statement->GetSelectTable()->GetTableName());
    EXPECT_EQ(ExpressionType::AGGREGATE_MAX, statement->GetSelectColumns()[0]->GetExpressionType());
  }

  {
    query = "SELECT MIN(*) FROM foo;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    EXPECT_EQ(1, result->GetStatements().size());
    EXPECT_EQ(StatementType::SELECT, result->GetStatement(0)->GetType());

    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    EXPECT_EQ("foo", statement->GetSelectTable()->GetTableName());
    EXPECT_EQ(ExpressionType::AGGREGATE_MIN, statement->GetSelectColumns()[0]->GetExpressionType());
  }

  {
    query = "SELECT AVG(*) FROM foo;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    EXPECT_EQ(1, result->GetStatements().size());
    EXPECT_EQ(StatementType::SELECT, result->GetStatement(0)->GetType());

    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    EXPECT_EQ("foo", statement->GetSelectTable()->GetTableName());
    EXPECT_EQ(ExpressionType::AGGREGATE_AVG, statement->GetSelectColumns()[0]->GetExpressionType());
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldGroupByTest) {
  // Select with group by clause
  std::string query = "SELECT * FROM foo GROUP BY id, name HAVING id > 10;";
  auto result = parser::PostgresParser::BuildParseTree(query);

  auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  auto columns = statement->GetSelectGroupBy()->GetColumns();

  EXPECT_EQ(2, columns.size());
  // Assume the parsed column order is the same as in the query
  EXPECT_EQ("id", columns[0].CastManagedPointerTo<ColumnValueExpression>()->GetColumnName());
  EXPECT_EQ("name", columns[1].CastManagedPointerTo<ColumnValueExpression>()->GetColumnName());

  auto having = statement->GetSelectGroupBy()->GetHaving();
  EXPECT_EQ(ExpressionType::COMPARE_GREATER_THAN, having->GetExpressionType());
  EXPECT_EQ(2, having->GetChildrenSize());

  auto name_exp = having->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  auto value_exp = having->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();

  EXPECT_EQ("id", name_exp->GetColumnName());
  EXPECT_EQ(type::TypeId::INTEGER, value_exp->GetReturnValueType());
  EXPECT_EQ(10, value_exp->Peek<int64_t>());

  auto result2 = parser::PostgresParser::BuildParseTree(query);
  auto statement_2 = result2->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  EXPECT_TRUE(*statement == *statement_2);
  EXPECT_EQ(statement->Hash(), statement_2->Hash());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldOrderByTest) {
  {
    std::string query = "SELECT * FROM foo ORDER BY id;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();

    auto order_by = select_stmt->GetSelectOrderBy();
    EXPECT_NE(order_by, nullptr);

    EXPECT_EQ(order_by->GetOrderByTypes().size(), 1);
    EXPECT_EQ(order_by->GetOrderByExpressions().size(), 1);
    EXPECT_EQ(order_by->GetOrderByTypes().at(0), OrderType::kOrderAsc);
    auto expr = order_by->GetOrderByExpressions().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "id");
  }

  {
    std::string query = "SELECT * FROM foo ORDER BY id ASC;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto order_by = select_stmt->GetSelectOrderBy();
    EXPECT_NE(order_by, nullptr);

    EXPECT_EQ(order_by->GetOrderByTypes().size(), 1);
    EXPECT_EQ(order_by->GetOrderByExpressions().size(), 1);
    EXPECT_EQ(order_by->GetOrderByTypes().at(0), OrderType::kOrderAsc);
    auto expr = order_by->GetOrderByExpressions().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "id");
  }

  {
    std::string query = "SELECT * FROM foo ORDER BY id DESC;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto order_by = select_stmt->GetSelectOrderBy();
    EXPECT_NE(order_by, nullptr);

    EXPECT_EQ(order_by->GetOrderByTypes().size(), 1);
    EXPECT_EQ(order_by->GetOrderByExpressions().size(), 1);
    EXPECT_EQ(order_by->GetOrderByTypes().at(0), OrderType::kOrderDesc);
    auto expr = order_by->GetOrderByExpressions().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "id");
  }

  {
    std::string query = "SELECT * FROM foo ORDER BY id, name;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto order_by = select_stmt->GetSelectOrderBy();
    EXPECT_NE(order_by, nullptr);

    EXPECT_EQ(order_by->GetOrderByTypes().size(), 2);
    EXPECT_EQ(order_by->GetOrderByExpressions().size(), 2);
    EXPECT_EQ(order_by->GetOrderByTypes().at(0), OrderType::kOrderAsc);
    EXPECT_EQ(order_by->GetOrderByTypes().at(1), OrderType::kOrderAsc);
    auto expr = order_by->GetOrderByExpressions().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "id");
    expr = order_by->GetOrderByExpressions().at(1);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "name");
  }

  {
    std::string query = "SELECT * FROM foo ORDER BY id, name DESC;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto sql_stmt = result->GetStatement(0);
    EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
    auto select_stmt = sql_stmt.CastManagedPointerTo<SelectStatement>();
    auto order_by = select_stmt->GetSelectOrderBy();
    EXPECT_NE(order_by, nullptr);

    EXPECT_EQ(order_by->GetOrderByTypes().size(), 2);
    EXPECT_EQ(order_by->GetOrderByExpressions().size(), 2);
    EXPECT_EQ(order_by->GetOrderByTypes().at(0), OrderType::kOrderAsc);
    EXPECT_EQ(order_by->GetOrderByTypes().at(1), OrderType::kOrderDesc);
    auto expr = order_by->GetOrderByExpressions().at(0);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "id");
    expr = order_by->GetOrderByExpressions().at(1);
    EXPECT_EQ(expr->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    EXPECT_EQ(expr.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "name");
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldConstTest) {
  // Select constants
  std::string query = "SELECT 'str', 1, 3.14 FROM foo;";

  auto result = parser::PostgresParser::BuildParseTree(query);
  auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  auto select_columns = statement->GetSelectColumns();
  EXPECT_EQ(3, select_columns.size());

  std::vector<type::TypeId> types = {type::TypeId::VARCHAR, type::TypeId::INTEGER, type::TypeId::REAL};

  for (size_t i = 0; i < select_columns.size(); i++) {
    auto column = select_columns[i];
    auto correct_type = types[i];

    EXPECT_EQ(ExpressionType::VALUE_CONSTANT, column->GetExpressionType());
    auto const_expression = column.CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ(correct_type, const_expression->GetReturnValueType());
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldJoinTest) {
  std::string query;

  {
    query = "SELECT * FROM foo JOIN bar ON foo.id=bar.id JOIN baz ON foo.id2=baz.id2;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto join_table = select_stmt->GetSelectTable();
    EXPECT_EQ(join_table->GetTableReferenceType(), TableReferenceType::JOIN);
    EXPECT_EQ(join_table->GetJoin()->GetJoinType(), JoinType::INNER);

    auto join_cond = join_table->GetJoin()->GetJoinCondition();
    EXPECT_EQ(join_cond->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(join_cond->GetChild(0)->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    auto jcl = join_cond->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
    EXPECT_EQ(jcl->GetTableName(), "foo");
    EXPECT_EQ(jcl->GetColumnName(), "id2");
    auto jcr = join_cond->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
    EXPECT_EQ(jcr->GetTableName(), "baz");
    EXPECT_EQ(jcr->GetColumnName(), "id2");

    auto l_join = join_table->GetJoin()->GetLeftTable();
    EXPECT_EQ(l_join->GetTableReferenceType(), TableReferenceType::JOIN);
    auto ll_join = l_join->GetJoin()->GetLeftTable();
    EXPECT_EQ(ll_join->GetTableName(), "foo");
    auto lr_join = l_join->GetJoin()->GetRightTable();
    EXPECT_EQ(lr_join->GetTableName(), "bar");

    auto r_table = join_table->GetJoin()->GetRightTable();
    EXPECT_EQ(r_table->GetTableReferenceType(), TableReferenceType::NAME);
    EXPECT_EQ(r_table->GetTableName(), "baz");
  }

  {
    query = "SELECT * FROM foo INNER JOIN bar ON foo.id=bar.id AND foo.val > bar.val;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto join_table = select_stmt->GetSelectTable();
    EXPECT_EQ(join_table->GetTableReferenceType(), TableReferenceType::JOIN);
    EXPECT_EQ(join_table->GetJoin()->GetJoinType(), JoinType::INNER);
  }

  {
    query = "SELECT * FROM foo LEFT JOIN bar ON foo.id=bar.id AND foo.val > bar.val;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto join_table = select_stmt->GetSelectTable();
    EXPECT_EQ(join_table->GetTableReferenceType(), TableReferenceType::JOIN);
    EXPECT_EQ(join_table->GetJoin()->GetJoinType(), JoinType::LEFT);
  }

  {
    query = "SELECT * FROM foo RIGHT JOIN bar ON foo.id=bar.id AND foo.val > bar.val;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto join_table = select_stmt->GetSelectTable();
    EXPECT_EQ(join_table->GetTableReferenceType(), TableReferenceType::JOIN);
    EXPECT_EQ(join_table->GetJoin()->GetJoinType(), JoinType::RIGHT);
  }

  {
    query = "SELECT * FROM foo FULL OUTER JOIN bar ON foo.id=bar.id AND foo.val > bar.val;";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto join_table = select_stmt->GetSelectTable();
    EXPECT_EQ(join_table->GetTableReferenceType(), TableReferenceType::JOIN);
    EXPECT_EQ(join_table->GetJoin()->GetJoinType(), JoinType::OUTER);
  }

  {
    // test case from SQLite
    query = "SELECT * FROM tab0 AS cor0 CROSS JOIN tab0 AS cor1 WHERE NULL IS NOT NULL;";
    EXPECT_THROW(parser::PostgresParser::BuildParseTree(query), ParserException);
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldNestedQueryTest) {
  // Select with nested query
  std::string query = "SELECT * FROM (SELECT * FROM foo) as t;";
  auto result = parser::PostgresParser::BuildParseTree(query);

  EXPECT_EQ(1, result->GetStatements().size());
  auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();

  EXPECT_EQ("t", statement->GetSelectTable()->GetAlias());
  auto nested_statement = statement->GetSelectTable()->GetSelect();
  EXPECT_EQ("foo", nested_statement->GetSelectTable()->GetTableName());
  EXPECT_EQ(ExpressionType::TABLE_STAR, nested_statement->GetSelectColumns()[0]->GetExpressionType());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldMultiTableTest) {
  // Select from multiple tables
  std::string query = "SELECT foo.name as name_new FROM (SELECT * FROM bar) as b, foo, bar WHERE foo.id = b.id;";
  auto result = parser::PostgresParser::BuildParseTree(query);
  EXPECT_EQ(1, result->GetStatements().size());
  auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();

  auto select_expression = statement->GetSelectColumns()[0].CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ("foo", select_expression->GetTableName());
  EXPECT_EQ("name", select_expression->GetColumnName());
  EXPECT_EQ("name_new", select_expression->GetAlias());

  auto from = statement->GetSelectTable();
  EXPECT_EQ(TableReferenceType::CROSS_PRODUCT, from->GetTableReferenceType());
  EXPECT_EQ(3, from->GetList().size());

  auto list = from->GetList();
  EXPECT_EQ("b", list[0]->GetAlias());
  EXPECT_EQ("bar", list[0]->GetSelect()->GetSelectTable()->GetTableName());

  EXPECT_EQ("foo", list[1]->GetTableName());
  EXPECT_EQ("bar", list[2]->GetTableName());

  auto where_expression = statement->GetSelectCondition();
  EXPECT_EQ(ExpressionType::COMPARE_EQUAL, where_expression->GetExpressionType());
  EXPECT_EQ(2, where_expression->GetChildrenSize());

  auto child_0 = where_expression->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  auto child_1 = where_expression->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ("foo", child_0->GetTableName());
  EXPECT_EQ("id", child_0->GetColumnName());
  EXPECT_EQ("b", child_1->GetTableName());
  EXPECT_EQ("id", child_1->GetColumnName());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldColumnUpdateTest) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.emplace_back("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE, C_DELIVERY_CNT = C_DELIVERY_CNT WHERE C_W_ID = 2");

  for (const auto &query : queries) {
    auto result = parser::PostgresParser::BuildParseTree(query);

    EXPECT_EQ(result->GetStatements().size(), 1);
    auto sql_stmt = result->GetStatement(0);

    EXPECT_EQ(sql_stmt->GetType(), StatementType::UPDATE);
    auto update_stmt = sql_stmt.CastManagedPointerTo<UpdateStatement>();
    auto table = update_stmt->GetUpdateTable();
    auto updates = update_stmt->GetUpdateClauses();
    auto where_clause = update_stmt->GetUpdateCondition();

    EXPECT_NE(table, nullptr);
    EXPECT_EQ(table->GetTableName(), "customer");

    EXPECT_EQ(updates.size(), 2);
    EXPECT_EQ(updates[0]->GetColumnName(), "c_balance");
    EXPECT_EQ(updates[0]->GetUpdateValue()->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    auto column_value_0 = updates[0]->GetUpdateValue().CastManagedPointerTo<ColumnValueExpression>();
    EXPECT_EQ(column_value_0->GetColumnName(), "c_balance");

    EXPECT_EQ(updates[1]->GetColumnName(), "c_delivery_cnt");
    EXPECT_EQ(updates[1]->GetUpdateValue()->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    auto column_value_1 = updates[1]->GetUpdateValue().CastManagedPointerTo<ColumnValueExpression>();
    EXPECT_EQ(column_value_1->GetColumnName(), "c_delivery_cnt");

    EXPECT_NE(where_clause, nullptr);
    EXPECT_EQ(where_clause->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
    auto left_child = where_clause->GetChild(0);
    auto right_child = where_clause->GetChild(1);
    EXPECT_EQ(left_child->GetExpressionType(), ExpressionType::COLUMN_VALUE);
    auto left_tuple = left_child.CastManagedPointerTo<ColumnValueExpression>();
    EXPECT_EQ(left_tuple->GetColumnName(), "c_w_id");

    EXPECT_EQ(right_child->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
    auto right_const = right_child.CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ(right_const->GetReturnValueType(), type::TypeId::INTEGER);
    EXPECT_EQ(right_const->Peek<int64_t>(), 2);
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldExpressionUpdateTest) {
  std::string query = "UPDATE STOCK SET S_QUANTITY = 48.0 , S_YTD = S_YTD + 1 WHERE S_I_ID = 68999 AND S_W_ID = 4";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto update_stmt = result->GetStatement(0).CastManagedPointerTo<UpdateStatement>();
  EXPECT_EQ(update_stmt->GetUpdateTable()->GetTableName(), "stock");

  // Test First Set Condition
  auto upd0 = update_stmt->GetUpdateClauses().at(0);
  EXPECT_EQ(upd0->GetColumnName(), "s_quantity");
  auto constant = upd0->GetUpdateValue().CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ(constant->GetReturnValueType(), type::TypeId::REAL);
  ASSERT_DOUBLE_EQ(constant->Peek<double>(), 48.0);

  // Test Second Set Condition
  auto upd1 = update_stmt->GetUpdateClauses().at(1);
  EXPECT_EQ(upd1->GetColumnName(), "s_ytd");
  auto op_expr = upd1->GetUpdateValue().CastManagedPointerTo<OperatorExpression>();
  EXPECT_EQ(op_expr->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
  auto child1 = op_expr->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(child1->GetColumnName(), "s_ytd");
  auto child2 = op_expr->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ(child2->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(child2->Peek<int64_t>(), 1);

  // Test Where clause
  auto where = update_stmt->GetUpdateCondition().CastManagedPointerTo<OperatorExpression>();
  EXPECT_EQ(where->GetExpressionType(), ExpressionType::CONJUNCTION_AND);

  auto cond1 = where->GetChild(0).CastManagedPointerTo<OperatorExpression>();
  EXPECT_EQ(cond1->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  auto column = cond1->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(column->GetColumnName(), "s_i_id");
  constant = cond1->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ(constant->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(constant->Peek<int64_t>(), 68999);

  auto cond2 = where->GetChild(1).CastManagedPointerTo<OperatorExpression>();
  EXPECT_EQ(cond2->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  column = cond2->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(column->GetColumnName(), "s_w_id");
  constant = cond2->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ(constant->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(constant->Peek<int64_t>(), 4);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldStringUpdateTest) {
  // Select with complicated where, tests both BoolExpr and AExpr
  std::string query =
      "UPDATE ORDER_LINE SET OL_DELIVERY_D = '2016-11-15 15:07:37' WHERE OL_O_ID = 2101 AND OL_D_ID = 2";

  auto result = parser::PostgresParser::BuildParseTree(query);
  auto sql_stmt = result->GetStatement(0);

  // Check root type
  EXPECT_EQ(sql_stmt->GetType(), StatementType::UPDATE);
  auto update = sql_stmt.CastManagedPointerTo<UpdateStatement>();

  // Check table name
  auto table_ref = update->GetUpdateTable();
  EXPECT_EQ(table_ref->GetTableName(), "order_line");

  // Check where expression
  auto where = update->GetUpdateCondition();
  EXPECT_EQ(where->GetExpressionType(), ExpressionType::CONJUNCTION_AND);
  EXPECT_EQ(where->GetChildrenSize(), 2);

  auto child0 = where->GetChild(0);
  auto child1 = where->GetChild(1);
  EXPECT_EQ(child0->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(child1->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(child0->GetChildrenSize(), 2);
  EXPECT_EQ(child1->GetChildrenSize(), 2);

  auto child00 = child0->GetChild(0);
  auto child10 = child1->GetChild(0);
  EXPECT_EQ(child00->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(child10->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(child00.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "ol_o_id");
  EXPECT_EQ(child10.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "ol_d_id");

  auto child01 = child0->GetChild(1);
  auto child11 = child1->GetChild(1);
  EXPECT_EQ(child01->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(child11->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(child01.CastManagedPointerTo<ConstantValueExpression>()->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(child01.CastManagedPointerTo<ConstantValueExpression>()->Peek<int64_t>(), 2101);
  EXPECT_EQ(child11.CastManagedPointerTo<ConstantValueExpression>()->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(child11.CastManagedPointerTo<ConstantValueExpression>()->Peek<int64_t>(), 2);

  // Check update clause
  auto update_clause = update->GetUpdateClauses()[0];
  EXPECT_EQ(update_clause->GetColumnName(), "ol_delivery_d");
  auto value = update_clause->GetUpdateValue();
  EXPECT_EQ(value->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  auto value_expr = value.CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ("2016-11-15 15:07:37", value_expr->Peek<std::string_view>());
  EXPECT_EQ(type::TypeId::VARCHAR, value_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldDeleteTest) {
  // Simple delete
  std::string query = "DELETE FROM foo;";
  auto result = parser::PostgresParser::BuildParseTree(query);

  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::DELETE);
  auto delstmt = result->GetStatement(0).CastManagedPointerTo<DeleteStatement>();
  EXPECT_EQ(delstmt->GetDeletionTable()->GetTableName(), "foo");
  EXPECT_EQ(delstmt->GetDeleteCondition(), nullptr);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldDeleteTestWithPredicate) {
  // Delete with a predicate
  std::string query = "DELETE FROM foo WHERE id=3;";
  auto result = parser::PostgresParser::BuildParseTree(query);

  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::DELETE);
  auto delstmt = result->GetStatement(0).CastManagedPointerTo<DeleteStatement>();
  EXPECT_EQ(delstmt->GetDeletionTable()->GetTableName(), "foo");
  EXPECT_NE(delstmt->GetDeleteCondition(), nullptr);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldInsertTest) {
  // Insert multiple tuples into the table
  std::string query = "INSERT INTO foo VALUES (NULL, 2, 3), (4, 5, 6);";
  auto result = parser::PostgresParser::BuildParseTree(query);

  EXPECT_EQ(1, result->GetStatements().size());
  EXPECT_TRUE(result->GetStatement(0)->GetType() == StatementType::INSERT);
  auto insert_stmt = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
  EXPECT_EQ("foo", insert_stmt->GetInsertionTable()->GetTableName());
  // 2 tuples
  EXPECT_EQ(2, insert_stmt->GetValues()->size());

  // First item of first tuple is NULL
  auto constant = insert_stmt->GetValues()->at(0).at(0).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_TRUE(constant->IsNull());

  // Second item of second tuple == 5
  constant = insert_stmt->GetValues()->at(1).at(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_EQ(constant->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(constant->Peek<int64_t>(), 5);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCreateTest) {
  std::string query =
      "CREATE TABLE Persons ("
      "id INT NOT NULL UNIQUE, "
      "age INT PRIMARY KEY, "
      "name VARCHAR(255), "
      "c_id INT,"
      "PRIMARY KEY (id),"
      "FOREIGN KEY (c_id) REFERENCES country (cid));";

  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  // Check column definition
  EXPECT_EQ(create_stmt->GetColumns().size(), 4);
  // Check First column
  auto column = create_stmt->GetColumns()[0];
  EXPECT_FALSE(column->IsNullable());
  EXPECT_TRUE(column->IsUnique());
  EXPECT_TRUE(column->IsPrimaryKey());
  EXPECT_EQ(column->GetColumnName(), "id");
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::INT);
  // Check Second column
  column = create_stmt->GetColumns()[1];
  EXPECT_TRUE(column->IsNullable());
  EXPECT_TRUE(column->IsPrimaryKey());
  // Check Third column
  column = create_stmt->GetColumns()[2];
  EXPECT_FALSE(column->IsPrimaryKey());
  EXPECT_EQ(column->GetTypeModifier(), 255);

  // Check Foreign Key Constraint
  column = create_stmt->GetForeignKeys()[0];
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::FOREIGN);
  EXPECT_EQ(column->GetForeignKeySources()[0], "c_id");
  EXPECT_EQ(column->GetForeignKeySinks()[0], "cid");
  EXPECT_EQ(column->GetForeignKeySinkTableName(), "country");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldTransactionTest) {
  std::string query = "BEGIN TRANSACTION;";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto transac_stmt = result->GetStatement(0).CastManagedPointerTo<TransactionStatement>();
  EXPECT_EQ(transac_stmt->GetTransactionType(), TransactionStatement::kBegin);

  query = "BEGIN;";
  result = parser::PostgresParser::BuildParseTree(query);
  transac_stmt = result->GetStatement(0).CastManagedPointerTo<TransactionStatement>();
  EXPECT_EQ(transac_stmt->GetTransactionType(), TransactionStatement::kBegin);

  query = "COMMIT TRANSACTION;";
  result = parser::PostgresParser::BuildParseTree(query);
  transac_stmt = result->GetStatement(0).CastManagedPointerTo<TransactionStatement>();
  EXPECT_EQ(transac_stmt->GetTransactionType(), TransactionStatement::kCommit);

  query = "ROLLBACK;";
  result = parser::PostgresParser::BuildParseTree(query);
  transac_stmt = result->GetStatement(0).CastManagedPointerTo<TransactionStatement>();
  EXPECT_EQ(transac_stmt->GetTransactionType(), TransactionStatement::kRollback);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCreateIndexTest) {
  std::string query = "CREATE UNIQUE INDEX IDX_ORDER ON oorder (O_W_ID, O_D_ID);";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  // Check attributes
  EXPECT_EQ(create_stmt->GetCreateType(), CreateStatement::kIndex);
  EXPECT_TRUE(create_stmt->IsUniqueIndex());
  EXPECT_EQ(create_stmt->GetIndexName(), "idx_order");
  EXPECT_EQ(create_stmt->GetTableName(), "oorder");
  EXPECT_EQ(create_stmt->GetIndexAttributes()[0].GetName(), "o_w_id");
  EXPECT_EQ(create_stmt->GetIndexAttributes()[1].GetName(), "o_d_id");

  query = "CREATE INDEX ii ON t USING HASH (col);";
  result = parser::PostgresParser::BuildParseTree(query);
  create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  // Check attributes
  EXPECT_EQ(create_stmt->GetCreateType(), CreateStatement::kIndex);
  EXPECT_EQ(create_stmt->GetIndexType(), IndexType::HASH);
  EXPECT_EQ(create_stmt->GetIndexName(), "ii");
  EXPECT_EQ(create_stmt->GetTableName(), "t");

  query = "CREATE INDEX ii ON t (col);";
  result = parser::PostgresParser::BuildParseTree(query);
  create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  // Check attributes
  EXPECT_EQ(create_stmt->GetCreateType(), CreateStatement::kIndex);
  EXPECT_EQ(create_stmt->GetIndexType(), IndexType::BWTREE);
  EXPECT_EQ(create_stmt->GetIndexName(), "ii");
  EXPECT_EQ(create_stmt->GetTableName(), "t");

  query = "CREATE INDEX ii ON t USING GIN (col);";
  EXPECT_THROW(parser::PostgresParser::BuildParseTree(query), NotImplementedException);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldInsertIntoSelectTest) {
  // insert into a table with select sub-query
  std::string query = "INSERT INTO foo select * from bar where id = 5;";
  auto result = parser::PostgresParser::BuildParseTree(query);

  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_TRUE(result->GetStatement(0)->GetType() == StatementType::INSERT);
  auto insert_stmt = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
  EXPECT_EQ(insert_stmt->GetInsertionTable()->GetTableName(), "foo");
  EXPECT_EQ(insert_stmt->GetValues(), nullptr);
  EXPECT_EQ(insert_stmt->GetSelect()->GetType(), StatementType::SELECT);
  EXPECT_EQ(insert_stmt->GetSelect()->GetSelectTable()->GetTableName(), "bar");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCreateDbTest) {
  std::string query = "CREATE DATABASE tt";
  auto result = parser::PostgresParser::BuildParseTree(query);

  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();
  EXPECT_EQ(create_stmt->GetCreateType(), CreateStatement::CreateType::kDatabase);
  EXPECT_EQ(create_stmt->GetDatabaseName(), "tt");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCreateSchemaTest) {
  std::string query = "CREATE SCHEMA tt";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();
  EXPECT_EQ("tt", create_stmt->GetNamespaceName());

  // Test default schema name
  query = "CREATE SCHEMA AUTHORIZATION joe";
  result = parser::PostgresParser::BuildParseTree(query);
  create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();
  EXPECT_EQ("joe", create_stmt->GetNamespaceName());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCreateViewTest) {
  std::string query = "CREATE VIEW comedies AS SELECT * FROM films WHERE kind = 'Comedy';";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  // Check attributes
  EXPECT_EQ(create_stmt->GetViewName(), "comedies");
  EXPECT_NE(create_stmt->GetViewQuery(), nullptr);
  auto view_query = create_stmt->GetViewQuery();
  EXPECT_EQ(view_query->GetSelectTable()->GetTableName(), "films");
  EXPECT_EQ(view_query->GetSelectColumns().size(), 1);
  EXPECT_NE(view_query->GetSelectCondition(), nullptr);
  EXPECT_EQ(view_query->GetSelectCondition()->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(view_query->GetSelectCondition()->GetChildrenSize(), 2);

  auto left_child = view_query->GetSelectCondition()->GetChild(0);
  EXPECT_EQ(left_child->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(left_child.CastManagedPointerTo<ColumnValueExpression>()->GetColumnName(), "kind");

  auto right_child = view_query->GetSelectCondition()->GetChild(1);
  EXPECT_EQ(right_child->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ("Comedy", right_child.CastManagedPointerTo<ConstantValueExpression>()->Peek<std::string_view>());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldDistinctFromTest) {
  std::string query = "SELECT id, value FROM foo WHERE id IS DISTINCT FROM value;";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  auto where_expr = statement->GetSelectCondition();
  EXPECT_EQ(ExpressionType::COMPARE_IS_DISTINCT_FROM, where_expr->GetExpressionType());
  EXPECT_EQ(type::TypeId::BOOLEAN, where_expr->GetReturnValueType());

  auto child0 = where_expr->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ("id", child0->GetColumnName());
  auto child1 = where_expr->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ("value", child1->GetColumnName());
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldConstraintTest) {
  std::string query =
      "CREATE TABLE table1 ("
      "a int DEFAULT 1+2,"
      "b int DEFAULT 1 REFERENCES table2 (bb) ON UPDATE CASCADE,"
      "c varchar(32) REFERENCES table3 (cc) MATCH FULL ON DELETE SET NULL,"
      "d int CHECK (d+1 > 0),"
      "FOREIGN KEY (d) REFERENCES table4 (dd) MATCH SIMPLE ON UPDATE SET "
      "DEFAULT"
      ");";

  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  // Check column definition
  EXPECT_EQ(create_stmt->GetColumns().size(), 4);

  // Check First column
  auto column = create_stmt->GetColumns()[0];
  EXPECT_EQ(column->GetColumnName(), "a");
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::INT);
  EXPECT_NE(column->GetDefaultExpression(), nullptr);
  auto default_expr = column->GetDefaultExpression().CastManagedPointerTo<OperatorExpression>();
  EXPECT_NE(default_expr, nullptr);
  EXPECT_EQ(default_expr->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
  EXPECT_EQ(default_expr->GetChildrenSize(), 2);

  auto child0 = default_expr->GetChild(0).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(child0, nullptr);
  EXPECT_EQ(child0->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(child0->Peek<int64_t>(), 1);

  auto child1 = default_expr->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(child1, nullptr);
  EXPECT_EQ(child1->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(child1->Peek<int64_t>(), 2);

  // Check Second column
  column = create_stmt->GetColumns()[1];
  EXPECT_EQ(column->GetColumnName(), "b");
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::INT);

  // Check Third column
  column = create_stmt->GetColumns()[2];
  EXPECT_EQ(column->GetColumnName(), "c");
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::VARCHAR);

  // Check Fourth column
  column = create_stmt->GetColumns()[3];
  EXPECT_EQ(column->GetColumnName(), "d");
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::INT);
  EXPECT_NE(column->GetCheckExpression(), nullptr);
  EXPECT_EQ(column->GetCheckExpression()->GetExpressionType(), ExpressionType::COMPARE_GREATER_THAN);
  EXPECT_EQ(column->GetCheckExpression()->GetChildrenSize(), 2);

  auto check_child1 = column->GetCheckExpression()->GetChild(0).CastManagedPointerTo<OperatorExpression>();
  EXPECT_NE(check_child1, nullptr);
  EXPECT_EQ(check_child1->GetExpressionType(), ExpressionType::OPERATOR_PLUS);
  EXPECT_EQ(check_child1->GetChildrenSize(), 2);
  auto plus_child1 = check_child1->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_NE(plus_child1, nullptr);
  EXPECT_EQ(plus_child1->GetColumnName(), "d");
  auto plus_child2 = check_child1->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(plus_child2, nullptr);
  EXPECT_EQ(plus_child2->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(plus_child2->Peek<int64_t>(), 1);

  auto check_child2 = column->GetCheckExpression()->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(check_child2, nullptr);
  EXPECT_EQ(check_child2->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(check_child2->Peek<int64_t>(), 0);

  // Check the foreign key constraint
  column = create_stmt->GetForeignKeys()[0];
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::FOREIGN);
  EXPECT_EQ(column->GetForeignKeySinks().size(), 1);
  EXPECT_EQ(column->GetForeignKeySinks()[0], "bb");
  EXPECT_EQ(column->GetForeignKeySinkTableName(), "table2");
  EXPECT_EQ(column->GetForeignKeyUpdateAction(), FKConstrActionType::CASCADE);
  EXPECT_EQ(column->GetForeignKeyDeleteAction(), FKConstrActionType::NOACTION);
  EXPECT_EQ(column->GetForeignKeyMatchType(), FKConstrMatchType::SIMPLE);

  column = create_stmt->GetForeignKeys()[1];
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::FOREIGN);
  EXPECT_EQ(column->GetForeignKeySinks().size(), 1);
  EXPECT_EQ(column->GetForeignKeySinks()[0], "cc");
  EXPECT_EQ(column->GetForeignKeySinkTableName(), "table3");
  EXPECT_EQ(column->GetForeignKeyUpdateAction(), FKConstrActionType::NOACTION);
  EXPECT_EQ(column->GetForeignKeyDeleteAction(), FKConstrActionType::SETNULL);
  EXPECT_EQ(column->GetForeignKeyMatchType(), FKConstrMatchType::FULL);

  column = create_stmt->GetForeignKeys()[2];
  EXPECT_EQ(column->GetColumnType(), ColumnDefinition::DataType::FOREIGN);
  EXPECT_EQ(column->GetForeignKeySources().size(), 1);
  EXPECT_EQ(column->GetForeignKeySources()[0], "d");
  EXPECT_EQ(column->GetForeignKeySinks().size(), 1);
  EXPECT_EQ(column->GetForeignKeySinks()[0], "dd");
  EXPECT_EQ(column->GetForeignKeySinkTableName(), "table4");
  EXPECT_EQ(column->GetForeignKeyUpdateAction(), FKConstrActionType::SETDEFAULT);
  EXPECT_EQ(column->GetForeignKeyDeleteAction(), FKConstrActionType::NOACTION);
  EXPECT_EQ(column->GetForeignKeyMatchType(), FKConstrMatchType::SIMPLE);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldDataTypeTest) {
  std::string query =
      "CREATE TABLE table1 ("
      "a text,"
      "b varchar(1024),"
      "c varbinary(32)"
      ");";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto create_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  EXPECT_EQ(create_stmt->GetColumns().size(), 3);

  // Check First column
  auto column = create_stmt->GetColumns()[0];
  EXPECT_EQ(column->GetColumnName(), "a");
  EXPECT_EQ(column->GetValueType(), type::TypeId::VARCHAR);
  // TODO(WAN): we got an equivalent of this?
  // EXPECT_EQ(peloton::type::PELOTON_TEXT_MAX_LEN, column->varlen);

  // Check Second column
  column = create_stmt->GetColumns()[1];
  EXPECT_EQ(column->GetColumnName(), "b");
  EXPECT_EQ(column->GetValueType(), type::TypeId::VARCHAR);
  EXPECT_EQ(column->GetTypeModifier(), 1024);

  // Check Third column
  column = create_stmt->GetColumns()[2];
  EXPECT_EQ(column->GetColumnName(), "c");
  EXPECT_EQ(column->GetValueType(), type::TypeId::VARBINARY);
  EXPECT_EQ(column->GetTypeModifier(), 32);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCreateTriggerTest) {
  std::string query =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF balance ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update(update_date);";
  auto result = parser::PostgresParser::BuildParseTree(query);

  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::CREATE);
  auto create_trigger_stmt = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();

  EXPECT_EQ(create_trigger_stmt->GetCreateType(), CreateStatement::CreateType::kTrigger);
  EXPECT_EQ(create_trigger_stmt->GetTriggerName(), "check_update");
  EXPECT_EQ(create_trigger_stmt->GetTableName(), "accounts");

  auto func_name = create_trigger_stmt->GetTriggerFuncNames();
  EXPECT_EQ(func_name.size(), 1);
  EXPECT_EQ(func_name[0], "check_account_update");

  auto func_args = create_trigger_stmt->GetTriggerArgs().at(0);
  EXPECT_EQ(func_args, "update_date");
  EXPECT_EQ(create_trigger_stmt->GetTriggerArgs().size(), 1);

  auto columns = create_trigger_stmt->GetTriggerColumns();
  EXPECT_EQ(columns.size(), 1);
  EXPECT_EQ(columns[0], "balance");

  auto when = create_trigger_stmt->GetTriggerWhen();
  EXPECT_NE(when, nullptr);
  EXPECT_EQ(when->GetExpressionType(), ExpressionType::COMPARE_NOT_EQUAL);
  EXPECT_EQ(when->GetChildrenSize(), 2);

  auto left = when->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  auto right = when->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(left->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(left->GetTableName(), "old");
  EXPECT_EQ(left->GetColumnName(), "balance");
  EXPECT_EQ(right->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(right->GetTableName(), "new");
  EXPECT_EQ(right->GetColumnName(), "balance");

  EXPECT_TRUE(TRIGGER_FOR_ROW(create_trigger_stmt->GetTriggerType()));

  EXPECT_TRUE(TRIGGER_FOR_BEFORE(create_trigger_stmt->GetTriggerType()));
  EXPECT_FALSE(TRIGGER_FOR_AFTER(create_trigger_stmt->GetTriggerType()));
  EXPECT_FALSE(TRIGGER_FOR_INSTEAD(create_trigger_stmt->GetTriggerType()));

  EXPECT_TRUE(TRIGGER_FOR_UPDATE(create_trigger_stmt->GetTriggerType()));
  EXPECT_FALSE(TRIGGER_FOR_INSERT(create_trigger_stmt->GetTriggerType()));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(create_trigger_stmt->GetTriggerType()));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(create_trigger_stmt->GetTriggerType()));
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldDropTriggerTest) {
  std::string query = "DROP TRIGGER if_dist_exists ON noisepage.films;";
  auto result = parser::PostgresParser::BuildParseTree(query);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::DROP);
  auto drop_trigger_stmt = result->GetStatement(0).CastManagedPointerTo<DropStatement>();

  EXPECT_EQ(drop_trigger_stmt->GetDropType(), DropStatement::DropType::kTrigger);
  EXPECT_EQ(drop_trigger_stmt->GetTriggerName(), "if_dist_exists");
  EXPECT_EQ(drop_trigger_stmt->GetTableName(), "films");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldFuncCallTest) {
  std::string query = "SELECT add(1,a), chr(99) FROM TEST WHERE FUN(b) > 2";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();

  // Check ADD(1,a)
  auto fun_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<FunctionExpression>();
  EXPECT_NE(fun_expr, nullptr);
  EXPECT_EQ(fun_expr->GetFuncName(), "add");
  EXPECT_EQ(fun_expr->GetChildrenSize(), 2);

  auto const_expr = fun_expr->GetChild(0).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(const_expr, nullptr);
  EXPECT_EQ(const_expr->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(const_expr->Peek<int64_t>(), 1);

  auto tv_expr = fun_expr->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_NE(tv_expr, nullptr);
  EXPECT_EQ(tv_expr->GetColumnName(), "a");

  // Check chr(99)
  fun_expr = select_stmt->GetSelectColumns()[1].CastManagedPointerTo<FunctionExpression>();
  EXPECT_NE(fun_expr, nullptr);
  EXPECT_EQ(fun_expr->GetFuncName(), "chr");
  EXPECT_EQ(fun_expr->GetChildrenSize(), 1);

  // Check FUN(b) > 2
  auto op_expr = select_stmt->GetSelectCondition().CastManagedPointerTo<OperatorExpression>();
  EXPECT_NE(op_expr, nullptr);
  EXPECT_EQ(op_expr->GetExpressionType(), ExpressionType::COMPARE_GREATER_THAN);

  fun_expr = op_expr->GetChild(0).CastManagedPointerTo<FunctionExpression>();
  EXPECT_NE(fun_expr, nullptr);
  EXPECT_EQ(fun_expr->GetFuncName(), "fun");
  EXPECT_EQ(fun_expr->GetChildrenSize(), 1);
  tv_expr = fun_expr->GetChild(0).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_NE(tv_expr, nullptr);
  EXPECT_EQ(tv_expr->GetColumnName(), "b");

  const_expr = op_expr->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(const_expr, nullptr);
  EXPECT_EQ(const_expr->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(const_expr->Peek<int64_t>(), 2);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldUDFFuncCallTest) {
  std::string query = "SELECT increment(1,b) FROM TEST;";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();

  auto fun_expr = select_stmt->GetSelectColumns()[0].CastManagedPointerTo<FunctionExpression>();
  EXPECT_NE(fun_expr, nullptr);
  EXPECT_EQ(fun_expr->GetFuncName(), "increment");
  EXPECT_EQ(fun_expr->GetChildrenSize(), 2);

  auto const_expr = fun_expr->GetChild(0).CastManagedPointerTo<ConstantValueExpression>();
  EXPECT_NE(const_expr, nullptr);
  EXPECT_EQ(const_expr->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(const_expr->Peek<int64_t>(), 1);

  auto tv_expr = fun_expr->GetChild(1).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_NE(tv_expr, nullptr);
  EXPECT_EQ(tv_expr->GetColumnName(), "b");
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldCaseTest) {
  std::string query = "SELECT id, case when id=100 then 1 else 0 end from tbl;";
  auto result = parser::PostgresParser::BuildParseTree(query);
  auto select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  auto select_args = select_stmt->GetSelectColumns();
  EXPECT_EQ(select_args.at(0)->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(select_args.at(1)->GetExpressionType(), ExpressionType::OPERATOR_CASE_EXPR);

  query = "SELECT id, case id when 100 then 1 when 200 then 2 end from tbl;";
  result = parser::PostgresParser::BuildParseTree(query);
  select_stmt = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
  select_args = select_stmt->GetSelectColumns();
  EXPECT_EQ(select_args.at(0)->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(select_args.at(1)->GetExpressionType(), ExpressionType::OPERATOR_CASE_EXPR);
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldDateTypeTest) {
  // Only checks valid date queries
  std::string query;

  {
    query = "INSERT INTO test_table VALUES (1, 2, '2017-01-01'::DATE);";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto statement = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
    auto values = *(statement->GetValues());
    auto cast_expr = values[0][2].CastManagedPointerTo<TypeCastExpression>();
    EXPECT_EQ(type::TypeId::DATE, cast_expr->GetReturnValueType());

    auto const_expr = cast_expr->GetChild(0).CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ("2017-01-01", const_expr->Peek<std::string_view>());
  }

  {
    query = "CREATE TABLE students (name TEXT, graduation DATE)";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto statement = result->GetStatement(0).CastManagedPointerTo<CreateStatement>();
    auto values = statement->GetColumns();
    auto date_column = values[1];
    EXPECT_EQ(ColumnDefinition::DataType::DATE, date_column->GetColumnType());
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldTypeCastTest) {
  std::vector<std::string> queries;
  queries.emplace_back("INSERT INTO test_table VALUES (1, 2, '2017'::INTEGER);");
  queries.emplace_back("INSERT INTO test_table VALUES (1, 2, '2017'::FLOAT);");
  queries.emplace_back("INSERT INTO test_table VALUES (1, 2, '2017'::DECIMAL);");
  queries.emplace_back("INSERT INTO test_table VALUES (1, 2, '2017'::TEXT);");
  queries.emplace_back("INSERT INTO test_table VALUES (1, 2, '2017'::VARCHAR);");

  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::REAL, type::TypeId::REAL,
                                     type::TypeId::VARCHAR, type::TypeId::VARCHAR};

  for (size_t i = 0; i < queries.size(); i++) {
    std::string query = queries[i];
    type::TypeId correct_type = types[i];

    auto result = parser::PostgresParser::BuildParseTree(query);
    auto statement = result->GetStatement(0).CastManagedPointerTo<InsertStatement>();
    auto values = *(statement->GetValues());
    auto cast_expr = values[0][2].CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ(correct_type, cast_expr->GetReturnValueType());
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTestBase, OldTypeCastInExpressionTest) {
  std::string query;
  {
    query = "SELECT * FROM a WHERE d <= date '2018-04-04';";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto where_expr = statement->GetSelectCondition();
    auto cast_expr = where_expr->GetChild(1).CastManagedPointerTo<TypeCastExpression>();
    EXPECT_EQ(type::TypeId::DATE, cast_expr->GetReturnValueType());

    auto const_expr = cast_expr->GetChild(0).CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ("2018-04-04", const_expr->Peek<std::string_view>());
  }

  {
    query = "SELECT '12345'::INTEGER - 12";
    auto result = parser::PostgresParser::BuildParseTree(query);
    auto statement = result->GetStatement(0).CastManagedPointerTo<SelectStatement>();
    auto column = statement->GetSelectColumns()[0];
    EXPECT_EQ(ExpressionType::OPERATOR_MINUS, column->GetExpressionType());

    auto left_child = column->GetChild(0).CastManagedPointerTo<TypeCastExpression>();
    EXPECT_EQ(type::TypeId::INTEGER, left_child->GetReturnValueType());

    auto value_expr = left_child->GetChild(0).CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ("12345", value_expr->Peek<std::string_view>());

    auto right_child = column->GetChild(1).CastManagedPointerTo<ConstantValueExpression>();
    EXPECT_EQ(12, right_child->Peek<int64_t>());
  }
}

}  // namespace noisepage::parser
