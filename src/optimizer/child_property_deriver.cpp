#include "optimizer/child_property_deriver.h"

#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/index_schema.h"
#include "common/managed_pointer.h"
#include "optimizer/group_expression.h"
#include "optimizer/index_util.h"
#include "optimizer/memo.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "parser/expression_util.h"

namespace noisepage::optimizer {

std::vector<std::pair<PropertySet *, std::vector<PropertySet *>>> ChildPropertyDeriver::GetProperties(
    catalog::CatalogAccessor *accessor, Memo *memo, PropertySet *requirements, GroupExpression *gexpr) {
  requirements_ = requirements;
  output_.clear();
  memo_ = memo;
  gexpr_ = gexpr;
  accessor_ = accessor;
  gexpr->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
  return move(output_);
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const SeqScan *op) {
  // Seq Scan does not provide any property
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(const IndexScan *op) {
  // Use GetIndexOids() to get all indexes on table_alias
  auto tbl_id = op->GetTableOID();
  std::vector<catalog::index_oid_t> tbl_indexes = accessor_->GetIndexOids(tbl_id);

  auto *property_set = new PropertySet();
  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      auto sort_prop = prop->As<PropertySort>();
      if (!IndexUtil::CheckSortProperty(sort_prop)) {
        continue;
      }

      auto idx_oid = op->GetIndexOID();
      if (IndexUtil::SatisfiesSortWithIndex(accessor_, sort_prop, tbl_id, idx_oid)) {
        property_set->AddProperty(prop->Copy());
      }
    }
  }

  output_.emplace_back(property_set, std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const ExternalFileScan *op) {
  // External file scans (like sequential scans) do not provide properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(const QueryDerivedScan *op) {
  auto output = requirements_->Copy();
  auto input = requirements_->Copy();
  output_.emplace_back(output, std::vector<PropertySet *>{input});
}

/**
 * Note:
 * Fulfill the entire projection property in the aggregation. Should
 * enumerate different combination of the aggregation functions and other
 * projection.
 */
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const HashGroupBy *op) {
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{new PropertySet()});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const SortGroupBy *op) {
  // Child must provide sort for Groupby columns
  std::vector<OrderByOrderingType> sort_ascending(op->GetColumns().size(), OrderByOrderingType::ASC);

  auto sort_prop = new PropertySort(op->GetColumns(), std::move(sort_ascending));
  auto prop_set = new PropertySet(std::vector<Property *>{sort_prop});
  output_.emplace_back(prop_set, std::vector<PropertySet *>{prop_set->Copy()});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Aggregate *op) {
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{new PropertySet()});
}

void ChildPropertyDeriver::Visit(const Limit *op) {
  // Limit fulfill the internal sort property
  std::vector<PropertySet *> child_input_properties{new PropertySet()};
  auto provided_prop = new PropertySet();
  if (!op->GetSortExpressions().empty()) {
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &exprs = op->GetSortExpressions();
    const std::vector<OrderByOrderingType> &sorts{op->GetSortAscending()};
    provided_prop->AddProperty(new PropertySort(exprs, sorts));
  }

  output_.emplace_back(provided_prop, std::move(child_input_properties));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const OrderBy *op) {}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InnerIndexJoin *op) {
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{new PropertySet()});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InnerNLJoin *op) { DeriveForJoin(); }
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InnerHashJoin *op) { DeriveForJoin(); }

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const LeftHashJoin *op) { DeriveForJoin(); }
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) {}
void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const LeftSemiHashJoin *op) { DeriveForJoin(); }

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Insert *op) {
  std::vector<PropertySet *> child_input_properties;
  output_.emplace_back(requirements_->Copy(), std::move(child_input_properties));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const InsertSelect *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet *> child_input_properties{requirements_->Copy()};
  output_.emplace_back(requirements_->Copy(), std::move(child_input_properties));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Update *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet *> child_input_properties{requirements_->Copy()};
  output_.emplace_back(requirements_->Copy(), std::move(child_input_properties));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const Delete *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet *> child_input_properties{requirements_->Copy()};
  output_.emplace_back(requirements_->Copy(), std::move(child_input_properties));
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const TableFreeScan *op) {
  // Provide nothing
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const ExportExternalFile *op) {
  // Let child fulfil all the required properties
  std::vector<PropertySet *> child_input_properties{requirements_->Copy()};
  output_.emplace_back(requirements_->Copy(), std::move(child_input_properties));
}

void ChildPropertyDeriver::DeriveForJoin() {
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{new PropertySet(), new PropertySet()});

  // If there is sort property and all the sort columns are from the probe
  // table (currently right table), we can push down the sort property
  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      bool can_pass_down = true;

      auto sort_prop = prop->As<PropertySort>();
      size_t sort_col_size = sort_prop->GetSortColumnSize();
      Group *probe_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(1));
      for (size_t idx = 0; idx < sort_col_size; ++idx) {
        ExprSet tuples;
        parser::ExpressionUtil::GetTupleValueExprs(&tuples, sort_prop->GetSortColumn(idx));
        for (auto &expr : tuples) {
          auto tv_expr = expr.CastManagedPointerTo<parser::ColumnValueExpression>();

          // If a column is not in the prob table, we cannot fulfill the sort
          // property in the requirement
          if (probe_group->GetTableAliases().count(tv_expr->GetTableName()) == 0U) {
            can_pass_down = false;
            break;
          }
        }

        if (!can_pass_down) {
          break;
        }
      }

      if (can_pass_down) {
        std::vector<PropertySet *> children{new PropertySet(), requirements_->Copy()};
        output_.emplace_back(requirements_->Copy(), std::move(children));
      }
    }
  }
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateDatabase *create_database) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateFunction *create_function) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateIndex *create_index) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateTable *create_table) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateNamespace *create_namespace) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateTrigger *create_trigger) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const CreateView *create_view) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const DropDatabase *drop_database) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const DropTable *drop_table) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const DropIndex *drop_index) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const DropNamespace *drop_namespace) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const DropTrigger *drop_trigger) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

void ChildPropertyDeriver::Visit(UNUSED_ATTRIBUTE const DropView *drop_view) {
  // Operator does not provide any properties
  output_.emplace_back(new PropertySet(), std::vector<PropertySet *>{});
}

}  // namespace noisepage::optimizer
