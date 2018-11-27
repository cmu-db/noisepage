#include <memory>
#include <utility>
#include <vector>
#include <tuple>
#include <string>

#include "gtest/gtest.h"

#include "sql/expression/sql_abstract_expression.h"
#include "sql/expression/sql_aggregate_expression.h"
#include "sql/expression/sql_case_expression.h"
#include "sql/expression/sql_comparison_expression.h"
#include "sql/expression/sql_function_expression.h"
#include "sql/expression/sql_operator_expression.h"
#include "sql/expression/sql_parameter_value_expression.h"
#include "sql/expression/sql_star_expression.h"
#include "sql/expression/sql_subquery_expression.h"
#include "sql/expression/sql_tuple_value_expression.h"
#include "sql/parameter.h"

namespace terrier::sql::expression {

// NOLINTNEXTLINE
TEST(SqlExpressionTests, BasicTest) {
  // Buiild a SQL tuple value expression
  std::string column_name = "column name";
  std::string table_name = "table name";
  std::tuple<db_oid_t, table_oid_t, col_oid_t> oid = std::make_tuple((db_oid_t) 1, (table_oid_t) 1, (col_oid_t) 1);
  auto sql_tuple_expression_builder = new SqlTupleValueExpression::Builder();
  sql_tuple_expression_builder->SetColName(column_name);
  sql_tuple_expression_builder->SetTableName(table_name);
  sql_tuple_expression_builder->SetOid(oid);

  auto sql_tuple_expression = sql_tuple_expression_builder->Build();

  EXPECT_TRUE(sql_tuple_expression->GetColumnName() == column_name);
  EXPECT_TRUE(sql_tuple_expression->GetTableName() == table_name);
  EXPECT_TRUE(sql_tuple_expression->GetOid() == oid);

  delete sql_tuple_expression_builder;
}

}  // namespace terrier::sql::expression
