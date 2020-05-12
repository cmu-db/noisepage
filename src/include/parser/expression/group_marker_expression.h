
#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "optimizer/group_expression.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

//===----------------------------------------------------------------------===//
// GroupMarkerExpression
//===----------------------------------------------------------------------===//

/**
 * When binding expressions to specific patterns, we allow for a "wildcard".
 * This GroupMarkerExpression serves to encapsulate and represent an expression
 * that was bound successfully to a "wildcard" pattern node.
 *
 * This class contains a single GroupID that can be used as a lookup into the
 * Memo class for the actual expression. In short, this GroupMarkerExpression
 * serves as an indirection wrapper pointing to the actual expression.
 */
class GroupMarkerExpression : public AbstractExpression {
 public:
  /**
   * Constructor.
   * @param group_id the Group ID that this marker represents
   */
  explicit GroupMarkerExpression(optimizer::group_id_t group_id)
      : AbstractExpression(ExpressionType::GROUP_MARKER, type::TypeId::INVALID, {}), group_id_(group_id) {}

  /**
   * Constructor with children.
   * @param group_id the Group ID that this marker represents
   * @param children the children to be passed to the node
   */
  GroupMarkerExpression(optimizer::group_id_t group_id, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::GROUP_MARKER, type::TypeId::INVALID, std::move(children)),
        group_id_(group_id) {}

  /**
   * @return this expression's Group ID
   */
  optimizer::group_id_t GetGroupID() { return group_id_; }

  std::unique_ptr<AbstractExpression> Copy() const override {
    return std::make_unique<GroupMarkerExpression>(group_id_);
  }

  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    return std::make_unique<GroupMarkerExpression>(group_id_, std::move(children));
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override {
    TERRIER_ASSERT(0, "Accept should not be called on a group marker expression");
  }

 protected:
  /**
   * The group ID for this group marker
   */
  optimizer::group_id_t group_id_;
};

}  // namespace terrier::parser
