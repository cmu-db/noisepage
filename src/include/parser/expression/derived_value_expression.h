#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * DerivedValueExpression represents a tuple of values that are derived from nested expressions
 *
 * Per Ling, this is only generated in the optimizer.
 */
class DerivedValueExpression : public AbstractExpression {
 public:
  /**
   * This constructor is called by the optimizer
   * TODO(WAN): does it make sense to make this a private constructor, and friend the optimizer? Ask William, Ling.
   * @param type type of the return value of the expression
   * @param tuple_idx index of the tuple
   * @param value_idx offset of the value in the tuple
   */
  DerivedValueExpression(type::TypeId type, int tuple_idx, int value_idx)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type, {}), tuple_idx_(tuple_idx), value_idx_(value_idx) {}

  /** Default constructor for deserialization. */
  DerivedValueExpression() = default;

  /**
   * Copies this DerivedValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    auto expr = std::make_unique<DerivedValueExpression>(GetReturnValueType(), GetTupleIdx(), GetValueIdx());
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  /**
   * Copies this DerivedValueExpression with new children
   * @param children New Children
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    TERRIER_ASSERT(children.empty(), "DerivedValueExpression should have no children");
    return Copy();
  }

  /** @return index of the tuple */
  int GetTupleIdx() const { return tuple_idx_; }

  /** @return offset of the value in the tuple */
  int GetValueIdx() const { return value_idx_; }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(tuple_idx_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(value_idx_));
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const DerivedValueExpression &>(rhs);
    if (GetTupleIdx() != other.GetTupleIdx()) return false;
    return GetValueIdx() == other.GetValueIdx();
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v,
              common::ManagedPointer<binder::BinderSherpa> sherpa) override {
    v->Visit(common::ManagedPointer(this), sherpa);
  }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["tuple_idx"] = tuple_idx_;
    j["value_idx"] = value_idx_;
    return j;
  }

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    tuple_idx_ = j.at("tuple_idx").get<int>();
    value_idx_ = j.at("value_idx").get<int>();
    return exprs;
  }

 private:
  /** Index of the tuple. */
  int tuple_idx_;
  /** Offset of the value in the tuple. */
  int value_idx_;
};

DEFINE_JSON_DECLARATIONS(DerivedValueExpression);

}  // namespace terrier::parser
