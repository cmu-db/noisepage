#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
/**
 * StarExpression represents a star in expressions like COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*).
   */
  StarExpression() : AbstractExpression(ExpressionType::STAR, type::TypeId::INTEGER, {}) { table_name_ = ""; }

  /**
   * Instantiates a new star expression with a table name, e.g. as in xxx.*
   */
  explicit StarExpression(std::string table_name)
      : AbstractExpression(ExpressionType::STAR, type::TypeId::INTEGER, {}) {
    table_name_ = std::move(table_name);
  }

  /**
   * Returns the table name associated with the star expression
   * @return table name
   */
  std::string GetTableName() { return table_name_; }

  /**
   * Copies this StarExpression
   * @returns this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;
  // TODO(Tianyu): This really should be a singleton object
  // ^WAN: jokes on you there's mutable state now and it can't be hahahaha

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "StarExpression should have 0 children");
    return Copy();
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

 private:
  std::string table_name_;
};

DEFINE_JSON_HEADER_DECLARATIONS(StarExpression);

}  // namespace noisepage::parser
