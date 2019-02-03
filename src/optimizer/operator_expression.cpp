#include "optimizer/operator_expression.h"

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Operator Expression
//===--------------------------------------------------------------------===//
OperatorExpression::OperatorExpression(Operator op) : op(std::move(op)) {}

void OperatorExpression::PushChild(std::shared_ptr<OperatorExpression> op) { children.emplace_back(std::move(op)); }

void OperatorExpression::PopChild() { children.pop_back(); }

const std::vector<std::shared_ptr<OperatorExpression>> &OperatorExpression::Children() const { return children; }

const Operator &OperatorExpression::Op() const { return op; }

const std::string OperatorExpression::GetInfo() const {
  std::string info = "{";
  {
    info += "\"Op\":";
    info += "\"" + op.GetName() + "\",";
    if (!children.empty()) {
      info += "\"Children\":[";
      {
        bool is_first = true;
        for (const auto &child : children) {
          if (is_first) {
            is_first = false;
          } else {
            info += ",";
          }
          info += child->GetInfo();
        }
      }
      info += "]";
    }
  }
  info += '}';
  return info;
}

}  // namespace terrier::optimizer
