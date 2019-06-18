#include "common/managed_pointer.h"
#include "catalog/catalog_accessor.h"
#include "catalog/index_schema.h"
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
                                    Memo *memo,
                                    catalog::CatalogAccessor *accessor) {
  requirements_ = requirements;
  output_.clear();
  memo_ = memo;
  gexpr_ = gexpr;
  accessor_ = accessor;
  gexpr->Op().Accept(this);
  return move(output_);
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const SeqScan *op) {
  // Seq Scan does not provide any property
  output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{}));
};

void ChildPropertyDeriver::Visit(const IndexScan *op) {
  // Use GetIndexes() to get all indexes on table_alias
  auto tbl_id = accessor_->GetTableOid(op->GetNamespaceOID(), op->GetTableAlias());
  std::vector<catalog::index_oid_t> tbl_indexes = accessor_->GetIndexes(tbl_id);

  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      auto sort_prop = prop->As<PropertySort>();
      auto sort_col_size = sort_prop->GetSortColumnSize();

      // Check to see whether the sort columns can be satisfied with
      // an index. In addition, we only consider ascending sort for now.
      auto can_fulfill = true;
      for (size_t idx = 0; idx < sort_col_size; ++idx) {
        int i_idx = static_cast<int>(idx);

        // TODO(boweic): Only consider ascending sort columns
        bool isAsc = sort_prop->GetSortAscending(i_idx) == planner::OrderByOrderingType::ASC;
        parser::ExpressionType type = sort_prop->GetSortColumn(i_idx)->GetExpressionType();
        if (!isAsc || type != parser::ExpressionType::VALUE_TUPLE) {
          can_fulfill = false;
          break;
        }
      }

      if (!can_fulfill) {
        continue;
      }

      // Iterate through all the table indexes and check whether any
      // of the indexes can be used to satisfy the sort property.
      for (auto &index : tbl_indexes) {
        std::vector<catalog::indexkeycol_oid_t> key_oids;
        const catalog::IndexSchema &index_schema = accessor_->GetIndexSchema(index);
        for (auto &col : index_schema.GetColumns()) { key_oids.push_back(col.GetOid()); }

        // If the sort column size is larger, then can't be fulfill by the index
        if (sort_col_size > key_oids.size()) {
          continue;
        }

        auto can_fulfill = true;
        for (size_t idx = 0; idx < sort_col_size; ++idx) {
          auto *expr = sort_prop->GetSortColumn(idx).get();
          auto *tv_expr = dynamic_cast<parser::TupleValueExpression *>(expr);
          TERRIER_ASSERT(tv_expr, "SortColumn should be TupleValueExpression");

          // Comparison with underlying values directly
          // TODO(wz2): Revisit this after John replies about indexkeycol_oid_t v col_oid_t
          TERRIER_ASSERT(0, "Figure out how to properly compare columns...");
          if ((!std::get<2>(tv_expr->GetBoundOid())) != (!key_oids[idx])) {
            can_fulfill = false;
            break;
          }
        }

        if (can_fulfill) {
          auto prop = requirements_->Copy();
          output_.push_back(std::make_pair(prop, std::vector<PropertySet*>{}));
        }
      }
    }
  }

  if (output_.empty()) {
    // No index can be used, so output provides no properties
    output_.push_back(std::make_pair(new PropertySet(), std::vector<PropertySet*>{}));
  }
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
  // TODO(wz2): Similar to boweic's todo above; is descending support?
  std::vector<planner::OrderByOrderingType> sort_ascending(op->GetColumns().size(), planner::OrderByOrderingType::ASC);
  std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_cols;
  for (auto &col : op->GetColumns()) {
    sort_cols.push_back(col);
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
  if (!op->GetSortExpressions().empty()) {
    std::vector<common::ManagedPointer<parser::AbstractExpression>> exprs = op->GetSortExpressions();
    std::vector<planner::OrderByOrderingType> sorts = op->GetSortAscending();
    provided_prop->AddProperty(new PropertySort(exprs, sorts));
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

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const TableFreeScan *op) {
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
        parser::ExpressionUtil::GetTupleValueExprs(tuples, sort_prop->GetSortColumn(idx).get());
        for (auto &expr : tuples) {
          auto tv_expr = dynamic_cast<const parser::TupleValueExpression *>(expr);
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
