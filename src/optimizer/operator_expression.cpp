#include "optimizer/operator_expression.h"

#include <memory>
#include <string>
#include <vector>

namespace terrier::optimizer {

const std::string OperatorExpression::GetInfo() const {
  std::string info = "{";
  {
    info += "\"Op\":";
    info += "\"" + op_.GetName() + "\",";
    if (!children_.empty()) {
      info += "\"Children\":[";
      {
        bool is_first = true;
        for (const auto &child : children_) {
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
