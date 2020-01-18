#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/index_schema.h"
#include "optimizer/properties.h"
#include "parser/expression_util.h"
#include "type/transient_value_factory.h"

namespace terrier::optimizer {

/**
 * Collection of helper functions related to working with Indexes
 * within the scope of the optimizer.
 */
class IndexUtil {
 public:
  /**
   * Checks whether a Sort property can be satisfied with any index.
   * This function does not determine whether an index CAN or CANNOT
   * be used. This function only verifies that the preconditions
   * are met before actually searching for a usable index.
   *
   * @param prop PropertySort to evaluate
   * @returns TRUE if should search for index
   */
  static bool CheckSortProperty(const PropertySort *prop) {
    auto sort_col_size = prop->GetSortColumnSize();
    for (size_t idx = 0; idx < sort_col_size; idx++) {
      // TODO(wz2): Consider descending when catalog/index support
      auto is_asc = prop->GetSortAscending(static_cast<int>(idx)) == optimizer::OrderByOrderingType::ASC;
      auto is_base = IsBaseColumn(prop->GetSortColumn(static_cast<int>(idx)));
      if (!is_asc || !is_base) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks whether a given index can be used to satisfy a property.
   * For an index to fulfill the sort property, the columns sorted
   * on must be in the same order and in the same direction.
   *
   * @param accessor CatalogAccessor
   * @param prop PropertySort to satisfy
   * @param tbl_oid OID of the table that the index is built on
   * @param idx_oid OID of index to use to satisfy
   * @returns TRUE if the specified index can fulfill sort property
   */
  static bool SatisfiesSortWithIndex(catalog::CatalogAccessor *accessor, const PropertySort *prop,
                                     catalog::table_oid_t tbl_oid, catalog::index_oid_t idx_oid) {
    auto &index_schema = accessor->GetIndexSchema(idx_oid);
    if (!SatisfiesBaseColumnRequirement(index_schema)) {
      return false;
    }

    std::vector<catalog::col_oid_t> mapped_cols;
    if (!ConvertIndexKeyOidToColOid(accessor, tbl_oid, index_schema, &mapped_cols)) {
      // Unable to translate indexkeycol_oid_t -> col_oid_t
      // Translation uses the IndexSchema::Column expression
      return false;
    }

    auto sort_col_size = prop->GetSortColumnSize();
    if (sort_col_size > mapped_cols.size()) {
      // Sort(a,b,c,d) cannot be satisfied with Index(a,b,c)
      return false;
    }

    for (size_t idx = 0; idx < sort_col_size; idx++) {
      // Compare col_oid_t directly due to "Base Column" requirement
      auto tv_expr = prop->GetSortColumn(idx).CastManagedPointerTo<parser::ColumnValueExpression>();

      // Sort(a,b,c) cannot be fulfilled by Index(a,c,b)
      auto col_match = tv_expr->GetColumnOid() == mapped_cols[idx];

      // TODO(wz2): need catalog flag for column sort direction
      // Sort(a ASC) cannot be fulfilled by Index(a DESC)
      auto dir_match = true;
      if (!col_match || !dir_match) {
        return false;
      }
    }

    return true;
  }

  /*
  static void PopulateMetadata(const std::vector<AnnotatedExpression> &predicates, IndexUtilMetadata *metadata) {
    // List of column OIDs that predicates are built against
    std::vector<catalog::col_oid_t> key_column_id_list;

    // List of expression comparison type (i.e =, >, ...)
    std::vector<parser::ExpressionType> expr_type_list;

    // List of values compared against
    std::vector<type::TransientValue> value_list;

    for (auto &pred : predicates) {
      auto expr = pred.GetExpr();
      if (expr->GetChildrenSize() != 2) {
        continue;
      }

      // Fetch column reference and value
      auto expr_type = expr->GetExpressionType();
      common::ManagedPointer<parser::AbstractExpression> tv_expr;
      common::ManagedPointer<parser::AbstractExpression> value_expr;
      if (expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto r_type = expr->GetChild(1)->GetExpressionType();
        if (r_type == parser::ExpressionType::VALUE_CONSTANT || r_type == parser::ExpressionType::VALUE_PARAMETER) {
          tv_expr = expr->GetChild(0);
          value_expr = expr->GetChild(1);
        }
      } else if (expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto l_type = expr->GetChild(0)->GetExpressionType();
        if (l_type == parser::ExpressionType::VALUE_CONSTANT || l_type == parser::ExpressionType::VALUE_PARAMETER) {
          tv_expr = expr->GetChild(1);
          value_expr = expr->GetChild(0);
          expr_type = parser::ExpressionUtil::ReverseComparisonExpressionType(expr_type);
        }
      }

      // If found valid tv_expr and value_expr, update col_id_list, expr_type_list and val_list
      if (tv_expr != nullptr) {
        // Get the column's col_oid_t from catalog
        auto col_expr = tv_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
        auto col_oid = col_expr->GetColumnOid();
        TERRIER_ASSERT(col_oid != catalog::col_oid_t(-1), "ColumnValueExpression at scan should be bound");
        key_column_id_list.push_back(col_oid);

        // Update expr_type_list
        expr_type_list.push_back(expr_type);

        if (value_expr->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT) {
          auto cve = value_expr.CastManagedPointerTo<parser::ConstantValueExpression>();
          type::TransientValue value = cve->GetValue();
          value_list.emplace_back(std::move(value));
        } else {
          auto poe = value_expr.CastManagedPointerTo<parser::ParameterValueExpression>();
          value_list.push_back(type::TransientValueFactory::GetParameterOffset(poe->GetValueIdx()));
        }
      }
    }

    metadata->SetPredicateColumnIds(std::move(key_column_id_list));
    metadata->SetPredicateExprTypes(std::move(expr_type_list));
    metadata->SetPredicateValues(std::move(value_list));
  }
  */

  /**
   * Checks whether a set of predicates can be satisfied with an index
   * @param accessor CatalogAccessor
   * @param tbl_oid OID of the table
   * @param index_oid OID of an index to check
   * @param preds_metadata IndexUtilMetadata from PopulateMetadata on predicates
   * @param output_metadata Output IndexUtilMetadata for creating IndexScan
   * @returns Whether index can be used
   */
  static bool SatisfiesPredicateWithIndex(catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid,
                                          catalog::index_oid_t index_oid,
                                          const std::vector<AnnotatedExpression> &predicates) {
    auto &index_schema = accessor->GetIndexSchema(index_oid);
    if (!SatisfiesBaseColumnRequirement(index_schema)) {
      return false;
    }

    std::vector<catalog::col_oid_t> mapped_cols;
    if (!ConvertIndexKeyOidToColOid(accessor, tbl_oid, index_schema, &mapped_cols)) {
      // Unable to translate indexkeycol_oid_t -> col_oid_t
      // Translation uses the IndexSchema::Column expression
      return false;
    }

    std::unordered_set<catalog::col_oid_t> index_cols;
    for (auto &id : mapped_cols) {
      index_cols.insert(id);
    }

    // TODO: check whether index can be used

    return false;
  }

 private:
  /**
   * Retrieves the catalog::col_oid_t equivalent for the index
   * @requires SatisfiesBaseColumnRequirement(schema)
   * @param accessor CatalogAccessor to use
   * @param tbl_oid Table the index belongs to
   * @param schema Schema
   * @param col_oids Vector to place col_oid_t translations
   * @returns TRUE if conversion successful
   */
  static bool ConvertIndexKeyOidToColOid(catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid,
                                         const catalog::IndexSchema &schema,
                                         std::vector<catalog::col_oid_t> *col_oids) {
    TERRIER_ASSERT(SatisfiesBaseColumnRequirement(schema), "GetIndexColOid() pre-cond not satisfied");
    auto &tbl_schema = accessor->GetSchema(tbl_oid);
    if (tbl_schema.GetColumns().size() < schema.GetColumns().size()) {
      return false;
    }

    std::unordered_map<std::string, catalog::col_oid_t> schema_col;
    for (auto &column : tbl_schema.GetColumns()) {
      schema_col[column.Name()] = column.Oid();
    }

    for (auto &column : schema.GetColumns()) {
      if (column.StoredExpression()->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto tv_expr = column.StoredExpression().CastManagedPointerTo<const parser::ColumnValueExpression>();
        if (tv_expr->GetColumnOid() != catalog::INVALID_COLUMN_OID) {
          // IndexSchema's expression's col_oid is bound
          col_oids->push_back(tv_expr->GetColumnOid());
          continue;
        }

        auto it = schema_col.find(tv_expr->GetColumnName());
        TERRIER_ASSERT(it != schema_col.end(), "Inconsistency between IndexSchema and table schema");
        col_oids->push_back(it->second);
      }
    }

    return true;
  }

  /**
   * Checks whether a Index satisfies the "base column" requirement.
   * The base column requirement (as defined from Peloton) is where
   * the index is built only on base table columns.
   * @param schema IndexSchema to evaluate
   * @returns TRUE if the "base column" requirement is met
   */
  static bool SatisfiesBaseColumnRequirement(const catalog::IndexSchema &schema) {
    for (auto &column : schema.GetColumns()) {
      auto recast = const_cast<parser::AbstractExpression *>(column.StoredExpression().Get());
      if (!IsBaseColumn(common::ManagedPointer(recast))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks whether a given expression is a "base column".
   * A base column, as used and defined by Peloton, is where expr is a ColumnValueExpression
   * @param expr AbstractExpression to evaluate
   * @returns TRUE if base column, false otherwise
   */
  static bool IsBaseColumn(common::ManagedPointer<parser::AbstractExpression> expr) {
    return (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE);
  }
};

}  // namespace terrier::optimizer
