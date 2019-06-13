#include "common/managed_pointer.h"
#include "parser/expression_util.h"
#include "optimizer/child_property_deriver.h"
#include "optimizer/properties.h"
#include "optimizer/group_expression.h"
#include "optimizer/property_set.h"
#include "optimizer/memo.h"

namespace terrier {
namespace optimizer {

std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>>
ChildPropertyDeriver::GetProperties(GroupExpression *gexpr,
                                    PropertySet* requirements,
                                    Memo *memo) {
  requirements_ = requirements;
  output_.clear();
  memo_ = memo;
  gexpr_ = gexpr;
  gexpr->Op().Accept(this);
  return move(output_);
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const SeqScan *op) {
  // Seq Scan does not provide any property
  output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{}));
};

void ChildPropertyDeriver::Visit(const IndexScan *op) {
  auto provided_prop = new PropertySet();
  std::shared_ptr<catalog::TableCatalogEntry> target_table = op->table_;
  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      // Walk through all indices in the table, check if any of the index could
      // provide the sort property
      // TODO(boweic) : Now we only consider ascending sort property, since the
      // index catalog interface does not support descending flag
      auto sort_prop = prop->As<PropertySort>();
      auto sort_col_size = sort_prop->GetSortColumnSize();
      auto can_fulfill = true;
      for (size_t idx = 0; idx < sort_col_size; ++idx) {
        int i_idx = static_cast<int>(idx);
        bool isAsc = sort_prop->GetSortAscending(i_idx);
        parser::ExpressionType type = sort_prop->GetSortColumn(i_idx)->GetExpressionType();
        if (!isAsc || type != parser::ExpressionType::VALUE_TUPLE) {
          can_fulfill = false;
          break;
        }
      }

      if (!can_fulfill) {
        break;
      }

      for (auto &index : target_table->GetIndexCatalogEntries()) {
        auto key_oids = index.second->GetKeyAttrs();
        // If the sort column size is larger, then can't be fulfill by the index
        if (sort_col_size > key_oids.size()) {
          break;
        }

        auto can_fulfill = true;
        for (size_t idx = 0; idx < sort_col_size; ++idx) {
          if (std::get<2>(reinterpret_cast<expression::TupleValueExpression *>(
                              sort_prop->GetSortColumn(idx))
                              ->GetBoundOid()) != key_oids[idx]) {
            can_fulfill = false;
            break;
          }
        }

        if (can_fulfill) {
          delete provided_prop;
          provided_prop = requirements_->Copy();
        }
      }
    }
  }

  output_.push_back(std::make_pair(provided_prop, std::vector<PropertySet*>{}));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const ExternalFileScan *op) {
  // External file scans (like sequential scans) do not provide properties
  output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{}));
}

void ChildPropertyDeriver::Visit(const QueryDerivedScan *op) {
  auto output = requirements_->Copy();
  auto input = requirements_->Copy();
  output_.push_back(std::make_pair(output, std::vector<PropertySet*>{input}));
}

/**
 * Note:
 * Fulfill the entire projection property in the aggregation. Should
 * enumerate different combination of the aggregation functions and other
 * projection.
 */
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) {
  output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{new PropertySet()}));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) {
  // Child must provide sort for Groupby columns
  std::vector<bool> sort_ascending(op->columns.size(), true);
  std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_cols;
  for (auto &col : op->columns) {
    sort_cols.push_back(col.get());
  }

  auto sort_prop = new PropertySort(sort_cols, std::move(sort_ascending));
  auto prop_set = new PropertySet(std::vector<Property*>{sort_prop});
  output_.push_back(std::make_pair(prop_set, std::vector<PropertySet*>{prop_set->Copy()}));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Aggregate *op) {
  output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{new PropertySet()}));
}

void ChildPropertyDeriver::Visit(const Limit *op) {
  // Limit fulfill the internal sort property
  std::vector<PropertySet*> child_input_properties{new PropertySet()};
  auto provided_prop = new PropertySet();
  if (!op->sort_exprs.empty()) {
    provided_prop->AddProperty(new PropertySort(op->sort_exprs, op->sort_acsending));
  }

  output_.push_back(std::make_pair(provided_prop, std::move(child_input_properties)));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Distinct *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet*> child_input_properties{requirements_->Copy()};
  output_.push_back(std::make_pair(requirements_->Copy(), std::move(child_input_properties)));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const OrderBy *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) {
  DeriveForJoin();
}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) {
  DeriveForJoin();
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) {}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Insert *op) {
  std::vector<PropertySet*> child_input_properties{requirements_->Copy()};
  output_.push_back(std::make_pair(requirements_->Copy(), std::move(child_input_properties)));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InsertSelect *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet*> child_input_properties{requirements_->Copy()};
  output_.push_back(std::make_pair(requirements_->Copy(), std::move(child_input_properties)));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Update *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet*> child_input_properties{requirements_->Copy()};
  output_.push_back(std::make_pair(requirements_->Copy(), std::move(child_input_properties)));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Delete *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet*> child_input_properties{requirements_->Copy()};
  output_.push_back(std::make_pair(requirements_->Copy(), std::move(child_input_properties)));
};

void ChildPropertyDeriver::Visit(UNUSED_ATRIBUTE const DummyScan *op) {
  // Provide nothing
  output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{}));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const ExportExternalFile *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet*> child_input_properties{requirements_->Copy()};
  output_.push_back(std::make_pair(requirements_->Copy(), std::move(child_input_properties)));
}

void ChildPropertyDeriver::DeriveForJoin() {
  output_.push_back(std::make_pair(
    new PropertySet(),
    std::vector<PropertySet*>{new PropertySet(), new PropertySet()}
  ));

  // If there is sort property and all the sort columns are from the probe
  // table (currently right table), we can push down the sort property
  // TODO(wz2): this assumption should be changed or encapsulated elsewhere
  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      bool can_pass_down = true;

      auto sort_prop = prop->As<PropertySort>();
      size_t sort_col_size = sort_prop->GetSortColumnSize();
      Group *probe_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(1));
      for (size_t idx = 0; idx < sort_col_size; ++idx) {
        ExprSet tuples;
        parser::ExpressionUtil::GetTupleValueExprs(tuples, sort_prop->GetSortColumn(idx));
        for (auto &expr : tuples) {
          // Get the underlying AbstractExpression pointer
          auto abs_expr = expr.operator->();
          auto tv_expr = dynamic_cast<parser::TupleValueExpression *>(abs_expr);
          TERRIER_ASSERT(tv_expr, "Expected TupleValueExpression");

          // If a column is not in the prob table, we cannot fulfill the sort
          // property in the requirement
          if (!probe_group->GetTableAliases().count(tv_expr->GetTableName())) {
            can_pass_down = false;
            break;
          }
        }

        if (!can_pass_down) {
          break;
        }
      }

      if (can_pass_down) {
        output_.push_back(std::make_pair(
          requirements_->Copy(),
          std::vector<PropertySet*>{new PropertySet(), requirements_->Copy()}
        ));
      }
    }
  }
}

}  // namespace optimizer
}  // namespace terrier
