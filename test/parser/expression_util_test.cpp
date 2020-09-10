#include "parser/expression_util.h"

#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/expression/operator_expression.h"
#include "type/type_id.h"

using ::testing::ElementsAre;

namespace terrier::parser::expression {

// NOLINTNEXTLINE
TEST(ExpressionUtilTest, GetColumnOidsTest) {
  // Test a single ColumnValueExpression
  auto tve =
      std::make_unique<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3));
  std::set<catalog::col_oid_t> oids;
  ExpressionUtil::GetColumnOids(&oids, common::ManagedPointer<AbstractExpression>(tve.get()));
  EXPECT_THAT(oids, ElementsAre(catalog::col_oid_t(3)));

  // Test an expression with 2 children
  auto child1 =
      std::make_unique<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(4));
  auto child2 =
      std::make_unique<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(5));
  std::vector<std::unique_ptr<AbstractExpression>> children1;
  children1.emplace_back(std::move(child1));
  children1.emplace_back(std::move(child2));
  auto op_expr_1 =
      std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children1));
  oids.clear();
  ExpressionUtil::GetColumnOids(&oids, common::ManagedPointer<AbstractExpression>(op_expr_1.get()));
  EXPECT_THAT(oids, ElementsAre(catalog::col_oid_t(4), catalog::col_oid_t(5)));

  // Test an expression with depth 2 (4 children)
  auto child3 =
      std::make_unique<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(6));
  auto child4 =
      std::make_unique<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(7));
  std::vector<std::unique_ptr<AbstractExpression>> children2;
  children2.emplace_back(std::move(child3));
  children2.emplace_back(std::move(child4));
  auto op_expr_2 =
      std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children2));

  std::vector<std::unique_ptr<AbstractExpression>> children3;
  children3.emplace_back(std::move(op_expr_1));
  children3.emplace_back(std::move(op_expr_2));
  auto op_expr_3 =
      std::make_unique<OperatorExpression>(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children3));
  oids.clear();
  ExpressionUtil::GetColumnOids(&oids, common::ManagedPointer<AbstractExpression>(op_expr_3.get()));
  EXPECT_THAT(oids,
              ElementsAre(catalog::col_oid_t(4), catalog::col_oid_t(5), catalog::col_oid_t(6), catalog::col_oid_t(7)));
}

}  // namespace terrier::parser::expression
