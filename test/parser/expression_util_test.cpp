#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/expression/expression_util.h"
#include "parser/expression/operator_expression.h"
#include "type/type_id.h"

using ::testing::ElementsAre;

namespace terrier::parser::expression {

// NOLINTNEXTLINE
TEST(ExpressionUtilTest, GetColumnOidsTest) {
  // Test a single ColumnValueExpression
  auto tve =
      std::make_shared<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3));
  std::set<catalog::col_oid_t> oids;
  ExpressionUtil::GetColumnOids(&oids, common::ManagedPointer<AbstractExpression>(tve.get()));
  EXPECT_THAT(oids, ElementsAre(catalog::col_oid_t(3)));

  // Test an expression with 2 children
  auto child1 =
      std::make_shared<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(4));
  auto child2 =
      std::make_shared<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(5));
  auto children1 = std::vector<std::shared_ptr<AbstractExpression>>{child1, child2};
  auto op_expr_1 =
      std::make_shared<OperatorExpression>(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children1));
  oids.clear();
  ExpressionUtil::GetColumnOids(&oids, common::ManagedPointer<AbstractExpression>(op_expr_1.get()));
  EXPECT_THAT(oids, ElementsAre(catalog::col_oid_t(4), catalog::col_oid_t(5)));

  // Test an expression with depth 2 (4 children)
  auto child3 =
      std::make_shared<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(6));
  auto child4 =
      std::make_shared<ColumnValueExpression>(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(7));
  auto children2 = std::vector<std::shared_ptr<AbstractExpression>>{child3, child4};
  auto op_expr_2 =
      std::make_shared<OperatorExpression>(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children2));

  auto children3 = std::vector<std::shared_ptr<AbstractExpression>>{op_expr_1, op_expr_2};
  auto op_expr_3 =
      std::make_shared<OperatorExpression>(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children3));
  oids.clear();
  ExpressionUtil::GetColumnOids(&oids, common::ManagedPointer<AbstractExpression>(op_expr_3.get()));
  EXPECT_THAT(oids,
              ElementsAre(catalog::col_oid_t(4), catalog::col_oid_t(5), catalog::col_oid_t(6), catalog::col_oid_t(7)));
}

}  // namespace terrier::parser::expression
