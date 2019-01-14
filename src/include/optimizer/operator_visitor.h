#pragma once

#include "optimizer/operators.h"

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Operator Visitor
//===--------------------------------------------------------------------===//

class OperatorVisitor {
 public:
  virtual ~OperatorVisitor() = default;

  // Physical operator
  virtual void Visit(const DummyScan *) {}
  virtual void Visit(const SeqScan *) {}
  virtual void Visit(const IndexScan *) {}
  virtual void Visit(const ExternalFileScan *) {}
  virtual void Visit(const QueryDerivedScan *) {}
  virtual void Visit(const OrderBy *) {}
  virtual void Visit(const Limit *) {}
  virtual void Visit(const InnerNLJoin *) {}
  virtual void Visit(const LeftNLJoin *) {}
  virtual void Visit(const RightNLJoin *) {}
  virtual void Visit(const OuterNLJoin *) {}
  virtual void Visit(const InnerHashJoin *) {}
  virtual void Visit(const LeftHashJoin *) {}
  virtual void Visit(const RightHashJoin *) {}
  virtual void Visit(const OuterHashJoin *) {}
  virtual void Visit(const Insert *) {}
  virtual void Visit(const InsertSelect *) {}
  virtual void Visit(const Delete *) {}
  virtual void Visit(const Update *) {}
  virtual void Visit(const HashGroupBy *) {}
  virtual void Visit(const SortGroupBy *) {}
  virtual void Visit(const Distinct *) {}
  virtual void Visit(const Aggregate *) {}
  virtual void Visit(const ExportExternalFile *) {}
};

}  // namespace terrier::optimizer