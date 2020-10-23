#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
/**
 * TableStarExpression represents a star in a SELECT list
 */
class TableStarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new table star expression used to indicate SELECT * FROM [tbls]
   */
  TableStarExpression() : AbstractExpression(ExpressionType::TABLE_STAR, type::TypeId::INVALID, {}) {}

  /**
   * Instantiates a new table star expression used to indicate SELECT t.* FROM [tbls]
   * @param tbl Table to select all columns from
   */
  explicit TableStarExpression(std::string tbl)
      : AbstractExpression(ExpressionType::TABLE_STAR, type::TypeId::INVALID, {}),
        target_table_specified_(true),
        target_table_(std::move(tbl)) {}

  /**
   * Copies this TableStarExpression
   * @returns this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "TableStarExpression should have 0 children");
    return Copy();
  }

  /** @return whether TableStarExpression has a target table specified */
  bool IsTargetTableSpecified() { return target_table_specified_; }

  /** @return target table specified by TableStarExpression */
  const std::string &GetTargetTable() { return target_table_; }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

 private:
  bool target_table_specified_ = false;
  std::string target_table_ = "";
};

DEFINE_JSON_HEADER_DECLARATIONS(TableStarExpression);

}  // namespace noisepage::parser
