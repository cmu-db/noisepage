#include "optimizer/expression_node_contents.h"

#include <memory>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression_defs.h"
#include "transaction/transaction_context.h"

namespace noisepage::optimizer {

common::ManagedPointer<parser::AbstractExpression> ExpressionNodeContents::CopyWithChildren(
    std::vector<std::unique_ptr<parser::AbstractExpression>> children) {
  common::ManagedPointer<parser::AbstractExpression> result;
  auto type = GetExpType();
  switch (type) {
    case parser::ExpressionType::COMPARE_EQUAL:
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
    case parser::ExpressionType::COMPARE_LESS_THAN:
    case parser::ExpressionType::COMPARE_GREATER_THAN:
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
    case parser::ExpressionType::COMPARE_LIKE:
    case parser::ExpressionType::COMPARE_NOT_LIKE:
    case parser::ExpressionType::COMPARE_IN:
    case parser::ExpressionType::COMPARE_IS_DISTINCT_FROM: {
      // Create new expression with 2 new children of the same type
      result = common::ManagedPointer<parser::AbstractExpression>(
          new parser::ComparisonExpression(type, std::move(children)));
      break;
    }

    case parser::ExpressionType::CONJUNCTION_AND:
    case parser::ExpressionType::CONJUNCTION_OR: {
      // Create new expression with the new children
      result = common::ManagedPointer<parser::AbstractExpression>(
          new parser::ConjunctionExpression(type, std::move(children)));
      break;
    }

    case parser::ExpressionType::OPERATOR_PLUS:
    case parser::ExpressionType::OPERATOR_MINUS:
    case parser::ExpressionType::OPERATOR_MULTIPLY:
    case parser::ExpressionType::OPERATOR_DIVIDE:
    case parser::ExpressionType::OPERATOR_CONCAT:
    case parser::ExpressionType::OPERATOR_MOD:
    case parser::ExpressionType::OPERATOR_NOT:
    case parser::ExpressionType::OPERATOR_IS_NULL:
    case parser::ExpressionType::OPERATOR_IS_NOT_NULL:
    case parser::ExpressionType::OPERATOR_EXISTS: {
      // Create new expression, preserving return value type
      type::TypeId ret = expr_->GetReturnValueType();
      result = common::ManagedPointer<parser::AbstractExpression>(
          new parser::OperatorExpression(type, ret, std::move(children)));
      break;
    }

    case parser::ExpressionType::STAR:
    case parser::ExpressionType::VALUE_CONSTANT:
    case parser::ExpressionType::VALUE_PARAMETER:
    case parser::ExpressionType::VALUE_TUPLE: {
      result = common::ManagedPointer<parser::AbstractExpression>(expr_->Copy().release());
      break;
    }

    case parser::ExpressionType::AGGREGATE_COUNT:
    case parser::ExpressionType::AGGREGATE_SUM:
    case parser::ExpressionType::AGGREGATE_MIN:
    case parser::ExpressionType::AGGREGATE_MAX:
    case parser::ExpressionType::AGGREGATE_AVG: {
      // Unfortunately, the aggregate expression (also applies to function) may
      // already have extra state information created due to the binder.
      // Under noisepage's desgn, we decide to just copy() the node and then
      // install the child.
      auto expr_copy = expr_->Copy();
      if (children.size() == 1) {
        // If we updated the child, install the child
        expr_copy->SetChild(0, common::ManagedPointer<parser::AbstractExpression>(children[0]));
      }
      result = common::ManagedPointer<parser::AbstractExpression>(expr_copy.release());
      break;
    }

    case parser::ExpressionType::FUNCTION: {
      // We should not be modifying the # of children of a function expression
      auto expr_copy = expr_->Copy();
      size_t num_children = children.size();
      for (size_t i = 0; i < num_children; i++) {
        expr_copy->SetChild(i, common::ManagedPointer<parser::AbstractExpression>(children[i]));
      }
      result = common::ManagedPointer<parser::AbstractExpression>(expr_copy.release());
      break;
    }

    // Rewriting for these 2 uses special matching patterns.
    // As such, when building as an output, we just copy directly.
    case parser::ExpressionType::ROW_SUBQUERY:
    case parser::ExpressionType::OPERATOR_CASE_EXPR: {
      result = common::ManagedPointer<parser::AbstractExpression>(expr_->Copy().release());
      break;
    }

    // These ExpressionTypes are never instantiated as a type
    case parser::ExpressionType::PLACEHOLDER:
    case parser::ExpressionType::COLUMN_REF:
    case parser::ExpressionType::FUNCTION_REF:
    case parser::ExpressionType::TABLE_REF:
    case parser::ExpressionType::VALUE_TUPLE_ADDRESS:
    case parser::ExpressionType::VALUE_NULL:
    case parser::ExpressionType::VALUE_VECTOR:
    case parser::ExpressionType::VALUE_SCALAR:
    case parser::ExpressionType::HASH_RANGE:
    case parser::ExpressionType::OPERATOR_CAST:
    default: {
      result = common::ManagedPointer<parser::AbstractExpression>(expr_->Copy().release());
      break;
    }
  }
  // Register the output with the transaction context
  parser::AbstractExpression *result_ptr = result.Get();
  if (txn_ != nullptr) {
    txn_->RegisterCommitAction([=]() { delete result_ptr; });
    txn_->RegisterAbortAction([=]() { delete result_ptr; });
  }
  return result;
}

void ExpressionNodeContents::Accept(common::ManagedPointer<OperatorVisitor> v) const { (void)v; }
}  // namespace noisepage::optimizer
