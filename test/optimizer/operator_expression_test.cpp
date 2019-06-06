#include "optimizer/operator_expression.h"
#include <memory>
#include <utility>
#include <vector>
#include "optimizer/logical_operators.h"
#include "optimizer/physical_operators.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {

// NOLINTNEXTLINE
TEST(OperatorExpressionTests, BasicOperatorExpressionTest) {
  auto ope1 = new OperatorExpression(TableFreeScan::make(), std::vector<OperatorExpression *>());
  auto ope2 = new OperatorExpression(LogicalDistinct::make(), std::vector<OperatorExpression *>());
  auto ope3 = new OperatorExpression(TableFreeScan::make(), std::vector<OperatorExpression *>());

  auto ope4 = new OperatorExpression(LogicalDistinct::make(), std::vector<OperatorExpression *>{ope1, ope2});
  auto ope5 = new OperatorExpression(LogicalDistinct::make(), std::vector<OperatorExpression *>{ope3});
  auto ope6 = new OperatorExpression(LogicalDistinct::make(), std::vector<OperatorExpression *>());

  auto ope7 = new OperatorExpression(TableFreeScan::make(), std::vector<OperatorExpression *>{ope4, ope5, ope6});

  EXPECT_TRUE(TableFreeScan::make() == ope7->GetOp());
  EXPECT_TRUE(LogicalDistinct::make() == ope6->GetOp());
  EXPECT_EQ(ope1->GetChildren(), std::vector<OperatorExpression *>());
  EXPECT_EQ(ope4->GetChildren(), (std::vector<OperatorExpression *>{ope1, ope2}));
  EXPECT_EQ(ope7->GetChildren(), (std::vector<OperatorExpression *>{ope4, ope5, ope6}));

  delete ope7;
}

}  // namespace terrier::optimizer
