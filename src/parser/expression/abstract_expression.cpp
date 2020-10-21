#include "parser/expression/abstract_expression.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/default_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/table_star_expression.h"
#include "parser/expression/type_cast_expression.h"

namespace terrier::parser {

void AbstractExpression::SetMutableStateForCopy(const AbstractExpression &copy_expr) {
  SetExpressionName(copy_expr.GetExpressionName());
  SetReturnValueType(copy_expr.GetReturnValueType());
  SetDepth(copy_expr.GetDepth());
  has_subquery_ = copy_expr.HasSubquery();
  alias_ = copy_expr.alias_;
}

common::hash_t AbstractExpression::Hash() const {
  common::hash_t hash = common::HashUtil::Hash(expression_type_);
  for (const auto &child : children_) {
    hash = common::HashUtil::CombineHashes(hash, child->Hash());
  }
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(return_value_type_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(expression_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(alias_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(depth_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(has_subquery_)));

  return hash;
}

bool AbstractExpression::operator==(const AbstractExpression &rhs) const {
  if (expression_type_ != rhs.expression_type_) return false;
  if (alias_ != rhs.alias_) return false;
  if (expression_name_ != rhs.expression_name_) return false;
  if (depth_ != rhs.depth_) return false;
  if (has_subquery_ != rhs.has_subquery_) return false;
  if (children_.size() != rhs.children_.size()) return false;
  for (size_t i = 0; i < children_.size(); i++)
    if (*(children_[i]) != *(rhs.children_[i])) return false;
  return return_value_type_ == rhs.return_value_type_;
}

std::vector<common::ManagedPointer<AbstractExpression>> AbstractExpression::GetChildren() const {
  std::vector<common::ManagedPointer<AbstractExpression>> children;
  children.reserve(children_.size());
  for (const auto &child : children_) {
    children.emplace_back(common::ManagedPointer(child));
  }
  return children;
}

void AbstractExpression::SetChild(int index, common::ManagedPointer<AbstractExpression> expr) {
  if (index >= static_cast<int>(children_.size())) {
    children_.resize(index + 1);
  }
  auto new_child = expr->Copy();
  children_[index] = std::move(new_child);
}

nlohmann::json AbstractExpression::ToJson() const {
  nlohmann::json j;
  j["expression_type"] = expression_type_;
  j["expression_name"] = expression_name_;
  j["alias"] = alias_;
  j["depth"] = depth_;
  j["has_subquery"] = has_subquery_;
  j["return_value_type"] = return_value_type_;
  std::vector<nlohmann::json> children_json;
  children_json.reserve(children_.size());
  for (const auto &child : children_) {
    children_json.emplace_back(child->ToJson());
  }
  j["children"] = children_json;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> AbstractExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> result_exprs;

  expression_type_ = j.at("expression_type").get<ExpressionType>();
  expression_name_ = j.at("expression_name").get<std::string>();
  alias_ = j.at("alias").get<std::string>();
  return_value_type_ = j.at("return_value_type").get<type::TypeId>();
  depth_ = j.at("depth").get<int>();
  has_subquery_ = j.at("has_subquery").get<bool>();

  // Deserialize children
  std::vector<std::unique_ptr<AbstractExpression>> children;
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  children.reserve(children_json.size());
  for (const auto &child_json : children_json) {
    auto deserialized = DeserializeExpression(child_json);
    children.emplace_back(std::move(deserialized.result_));
    result_exprs.insert(result_exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                        std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  children_ = std::move(children);

  return result_exprs;
}

JSONDeserializeExprIntermediate DeserializeExpression(const nlohmann::json &j) {
  std::unique_ptr<AbstractExpression> expr;

  auto expression_type = j.at("expression_type").get<ExpressionType>();
  switch (expression_type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_AVG: {
      expr = std::make_unique<AggregateExpression>();
      break;
    }

    case ExpressionType::OPERATOR_CASE_EXPR: {
      expr = std::make_unique<CaseExpression>();
      break;
    }

    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOT_EQUAL:
    case ExpressionType::COMPARE_LESS_THAN:
    case ExpressionType::COMPARE_GREATER_THAN:
    case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
    case ExpressionType::COMPARE_LIKE:
    case ExpressionType::COMPARE_NOT_LIKE:
    case ExpressionType::COMPARE_IN:
    case ExpressionType::COMPARE_IS_DISTINCT_FROM: {
      expr = std::make_unique<ComparisonExpression>();
      break;
    }

    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR: {
      expr = std::make_unique<ConjunctionExpression>();
      break;
    }

    case ExpressionType::VALUE_CONSTANT: {
      expr = std::make_unique<ConstantValueExpression>();
      break;
    }

    case ExpressionType ::VALUE_DEFAULT: {
      expr = std::make_unique<DefaultValueExpression>();
      break;
    }
    case ExpressionType::FUNCTION: {
      expr = std::make_unique<FunctionExpression>();
      break;
    }

    case ExpressionType::OPERATOR_UNARY_MINUS:
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_CONCAT:
    case ExpressionType::OPERATOR_MOD:
    case ExpressionType::OPERATOR_NOT:
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL:
    case ExpressionType::OPERATOR_EXISTS: {
      expr = std::make_unique<OperatorExpression>();
      break;
    }

    case ExpressionType::VALUE_PARAMETER: {
      expr = std::make_unique<ParameterValueExpression>();
      break;
    }

    case ExpressionType::STAR: {
      expr = std::make_unique<StarExpression>();
      break;
    }

    case ExpressionType::TABLE_STAR: {
      expr = std::make_unique<TableStarExpression>();
      break;
    }

    case ExpressionType::ROW_SUBQUERY: {
      expr = std::make_unique<SubqueryExpression>();
      break;
    }

    case ExpressionType::VALUE_TUPLE: {
      expr = std::make_unique<DerivedValueExpression>();
      break;
    }

    case ExpressionType::COLUMN_VALUE: {
      expr = std::make_unique<ColumnValueExpression>();
      break;
    }

    case ExpressionType::OPERATOR_CAST: {
      expr = std::make_unique<TypeCastExpression>();
      break;
    }

    default:
      throw std::runtime_error("Unknown expression type during deserialization");
  }
  auto non_owned_exprs = expr->FromJson(j);
  return JSONDeserializeExprIntermediate{std::move(expr), std::move(non_owned_exprs)};
}

bool AbstractExpression::DeriveSubqueryFlag() {
  if (expression_type_ == ExpressionType::ROW_SUBQUERY) {
    has_subquery_ = true;
  } else {
    for (auto &child : children_) {
      if (child->DeriveSubqueryFlag()) {
        has_subquery_ = true;
        break;
      }
    }
  }
  return has_subquery_;
}

int AbstractExpression::DeriveDepth() {
  if (depth_ < 0) {
    for (auto &child : children_) {
      auto child_depth = child->DeriveDepth();
      if (child_depth >= 0 && (depth_ == -1 || child_depth < depth_)) depth_ = child_depth;
    }
  }
  return depth_;
}

void AbstractExpression::DeriveExpressionName() {
  // If alias exists, it will be used in TrafficCop
  if (!alias_.empty()) {
    expression_name_ = alias_;
    return;
  }
  // TODO(WAN): I don't understand why we need to derive an expression name at all. And aliases are known early.
}

DEFINE_JSON_BODY_DECLARATIONS(AbstractExpression);

}  // namespace terrier::parser
