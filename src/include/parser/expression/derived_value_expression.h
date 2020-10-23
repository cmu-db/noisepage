#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
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
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Copies this DerivedValueExpression with new children
   * @param children New Children
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "DerivedValueExpression should have no children");
    return Copy();
  }

  /** @return index of the tuple */
  int GetTupleIdx() const { return tuple_idx_; }

  /** @return offset of the value in the tuple */
  int GetValueIdx() const { return value_idx_; }

  /** Hashes the expression **/
  common::hash_t Hash() const override;

  bool operator==(const AbstractExpression &rhs) const override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override;

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /** Index of the tuple. */
  int tuple_idx_;
  /** Offset of the value in the tuple. */
  int value_idx_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DerivedValueExpression);

}  // namespace noisepage::parser
