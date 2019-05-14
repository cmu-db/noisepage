#pragma once

#include "optimizer/operators.h"

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Operator Visitor
//===--------------------------------------------------------------------===//

class OperatorVisitor {
 public:
  virtual ~OperatorVisitor() = default;

  // Physical operators
  virtual void Visit(const TableFreeScan *table_free_scan) {}
  virtual void Visit(const SeqScan *seq_scan) {}
  virtual void Visit(const IndexScan *index_scan) {}
  virtual void Visit(const ExternalFileScan *ext_file_scan) {}
  virtual void Visit(const QueryDerivedScan *query_derived_scan) {}
  virtual void Visit(const OrderBy *order_by) {}
  virtual void Visit(const Limit *limit) {}
  virtual void Visit(const InnerNLJoin *inner_join) {}
  virtual void Visit(const LeftNLJoin *left_nl_join) {}
  virtual void Visit(const RightNLJoin *right_nl_join) {}
  virtual void Visit(const OuterNLJoin *outer_nl_join) {}
  virtual void Visit(const InnerHashJoin *inner_hash_join) {}
  virtual void Visit(const LeftHashJoin *left_hash_join) {}
  virtual void Visit(const RightHashJoin *right_hash_join) {}
  virtual void Visit(const OuterHashJoin *outer_hash_join) {}
  virtual void Visit(const Insert *insert) {}
  virtual void Visit(const InsertSelect *insert_select) {}
  virtual void Visit(const Delete *del) {}
  virtual void Visit(const Update *update) {}
  virtual void Visit(const HashGroupBy *hash_group_by) {}
  virtual void Visit(const SortGroupBy *sort_group_by) {}
  virtual void Visit(const Distinct *distinct) {}
  virtual void Visit(const Aggregate *aggregate) {}
  virtual void Visit(const ExportExternalFile *export_ext_file) {}
};

}  // namespace terrier::optimizer
