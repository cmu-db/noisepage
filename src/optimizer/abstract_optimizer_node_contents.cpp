#include "optimizer/abstract_optimizer_node_contents.h"

#include "common/hash_util.h"

namespace terrier::optimizer {

hash_t AbstractOptimizerNodeContents::Hash() const {
  OpType op_type = GetOpType();
  parser::ExpressionType exp_type = GetExpType();
  return (op_type != OpType::UNDEFINED) ? common::HashUtil::Hash(op_type) : common::HashUtil::Hash(exp_type);
}

}  // namespace terrier::optimizer
