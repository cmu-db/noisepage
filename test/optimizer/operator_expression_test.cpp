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
  auto ope1 = new OperatorExpression(TableFreeScan::Make(), std::vector<OperatorExpression *>());
  auto ope2 = new OperatorExpression(LogicalDistinct::Make(), std::vector<OperatorExpression *>());
  auto ope3 = new OperatorExpression(TableFreeScan::Make(), std::vector<OperatorExpression *>());

  auto ope4 = new OperatorExpression(LogicalDistinct::Make(), std::vector<OperatorExpression *>{ope1, ope2});
  Operator logical_ext_file_get_1 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_2 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  auto ope5 = new OperatorExpression(std::move(logical_ext_file_get_1), std::vector<OperatorExpression *>{ope3});
  auto ope6 = new OperatorExpression(LogicalDistinct::Make(), std::vector<OperatorExpression *>());

  auto ope7 = new OperatorExpression(TableFreeScan::Make(), std::vector<OperatorExpression *>{ope4, ope5, ope6});

  EXPECT_TRUE(TableFreeScan::Make() == ope7->GetOp());
  EXPECT_TRUE(LogicalDistinct::Make() == ope6->GetOp());
  EXPECT_TRUE(logical_ext_file_get_2 == ope5->GetOp());
  EXPECT_EQ(ope1->GetChildren(), std::vector<OperatorExpression *>());
  EXPECT_EQ(ope4->GetChildren(), (std::vector<OperatorExpression *>{ope1, ope2}));
  EXPECT_EQ(ope7->GetChildren(), (std::vector<OperatorExpression *>{ope4, ope5, ope6}));

  delete ope7;
}

}  // namespace terrier::optimizer
