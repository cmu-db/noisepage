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
  auto ope1 =
      std::make_unique<OperatorExpression>(TableFreeScan::Make(), std::vector<std::unique_ptr<OperatorExpression>>());
  auto ope2 =
      std::make_unique<OperatorExpression>(LogicalDistinct::Make(), std::vector<std::unique_ptr<OperatorExpression>>());
  auto ope3 =
      std::make_unique<OperatorExpression>(TableFreeScan::Make(), std::vector<std::unique_ptr<OperatorExpression>>());

  auto ope1_ref = common::ManagedPointer<OperatorExpression>(ope1);
  auto ope2_ref = common::ManagedPointer<OperatorExpression>(ope2);

  auto ope4 =
      std::make_unique<OperatorExpression>(LogicalDistinct::Make(), std::vector<std::unique_ptr<OperatorExpression>>());
  ope4->PushChild(std::move(ope1));
  ope4->PushChild(std::move(ope2));
  Operator logical_ext_file_get_1 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_2 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  auto ope5 = std::make_unique<OperatorExpression>(std::move(logical_ext_file_get_1),
                                                   std::vector<std::unique_ptr<OperatorExpression>>());
  auto ope4_ref = common::ManagedPointer<OperatorExpression>(ope4);
  auto ope5_ref = common::ManagedPointer<OperatorExpression>(ope5);
  ope5->PushChild(std::move(ope3));
  auto ope6 =
      std::make_unique<OperatorExpression>(LogicalDistinct::Make(), std::vector<std::unique_ptr<OperatorExpression>>());
  auto ope6_ref = common::ManagedPointer<OperatorExpression>(ope6);

  auto ope7 =
      std::make_unique<OperatorExpression>(TableFreeScan::Make(), std::vector<std::unique_ptr<OperatorExpression>>());
  ope7->PushChild(std::move(ope4));
  ope7->PushChild(std::move(ope5));
  ope7->PushChild(std::move(ope6));

  EXPECT_TRUE(TableFreeScan::Make() == ope7->GetOp());
  EXPECT_TRUE(LogicalDistinct::Make() == ope6_ref->GetOp());
  EXPECT_TRUE(logical_ext_file_get_2 == ope5_ref->GetOp());
  EXPECT_EQ(ope1_ref->GetChildren(), std::vector<common::ManagedPointer<OperatorExpression>>());
  EXPECT_EQ(ope4_ref->GetChildren(), (std::vector<common::ManagedPointer<OperatorExpression>>{ope1_ref, ope2_ref}));
  EXPECT_EQ(ope7->GetChildren(),
            (std::vector<common::ManagedPointer<OperatorExpression>>{ope4_ref, ope5_ref, ope6_ref}));
}

}  // namespace terrier::optimizer
