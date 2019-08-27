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
 * Struct defines information for IndexUtil functions that are
 * concerned with handling index-predicates.
 *
 * TODO(wz2): Support complicated predicates on non-base indexes
 * Support requires modifying PopulateMetadata/SatisfiesPredicateWithIndex.
 * In addition, also need to modify IndexScan / IndexScanPlanNode.
 * For now, based on Peloton, supports only (col comparator value)
 * on base-column indexes.
 */
struct IndexUtilMetadata {
 public:
  friend class IndexUtil;

  /**
   * Returns the predicate col_oid_t vector
   * @returns vector of predicates col_oid_t
   */
  std::vector<catalog::col_oid_t> &GetPredicateColumnIds() { return predicate_column_ids_; }

  /**
   * Returns the predicate ExpressionType vector
   * @returns vector of predicates ExpressionType
   */
  std::vector<parser::ExpressionType> &GetPredicateExprTypes() { return predicate_expr_types_; }

  /**
   * Returns the predicate TransientValue vector
   * @returns vector of predicates TransientValue
   */
  std::vector<type::TransientValue> &GetPredicateValues() { return predicate_values_; }

 private:
  /**
   * Sets the predicate_column_ids_ vector
   * @param col_ids Vector of catalog::col_oid_t for predicates
   */
  void SetPredicateColumnIds(std::vector<catalog::col_oid_t> &&col_ids) { predicate_column_ids_ = col_ids; }

  /**
   * Sets the predicate_expr_types_ vector
   * @param expr_types Vector of parser::ExpressionType for predicates
   */
  void SetPredicateExprTypes(std::vector<parser::ExpressionType> &&expr_types) { predicate_expr_types_ = expr_types; }

  /**
   * Sets the predicate_values_ vector
   * @param values Vector of type::TransientValue for predicates
   */
  void SetPredicateValues(std::vector<type::TransientValue> &&values) { predicate_values_ = std::move(values); }

  /**
   * Vector of predicate col_oid_t
   */
  std::vector<catalog::col_oid_t> predicate_column_ids_;

  /**
   * Vector of predicate ExpressionType
   */
  std::vector<parser::ExpressionType> predicate_expr_types_;

  /**
   * Vector of predicates values
   */
  std::vector<type::TransientValue> predicate_values_;
};

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
      auto isAsc = prop->GetSortAscending(static_cast<int>(idx)) == planner::OrderByOrderingType::ASC;
      auto isBase = IsBaseColumn(prop->GetSortColumn(static_cast<int>(idx)).get());
      if (!isAsc || !isBase) {
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
   * Requires CheckSortProperty(prop)
   * @param prop PropertySort to satisfy
   * @param tbl_oid OID of the table that the index is built on
   * @param idx_oid OID of index to use to satisfy
   * @param accessor CatalogAccessor
   * @returns TRUE if the specified index can fulfill sort property
   */
  static bool SatisfiesSortWithIndex(const PropertySort *prop, catalog::table_oid_t tbl_oid,
                                     catalog::index_oid_t idx_oid, catalog::CatalogAccessor *accessor) {
    // For now, this logic only considers indexes where the columns
    // all satisfy the "base column" property.
    TERRIER_ASSERT(CheckSortProperty(prop), "pre-cond not satisfied");

    auto &index_schema = accessor->GetIndexSchema(idx_oid);
    if (!SatisfiesBaseColumnRequirement(index_schema)) {
      return false;
    }

    // Index is not queryable
    if (!index_schema.Valid()) {
      return false;
    }

    std::vector<catalog::col_oid_t> mapped_cols;
    if (!GetIndexColOid(tbl_oid, index_schema, accessor, &mapped_cols)) {
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
      auto *expr = prop->GetSortColumn(idx).get();
      auto *tv_expr = dynamic_cast<const parser::ColumnValueExpression *>(expr);
      TERRIER_ASSERT(tv_expr, "ColumnValueExpression expected");

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

  /**
   * Populates metadata using information from predicates
   * @param predicates Predicates to populate metadata with
   * @param metadata IndexUtilMetadata
   */
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
      const parser::AbstractExpression *tv_expr = nullptr;
      const parser::AbstractExpression *value_expr = nullptr;
      if (expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto r_type = expr->GetChild(1)->GetExpressionType();
        if (r_type == parser::ExpressionType::VALUE_CONSTANT ||
            r_type == parser::ExpressionType::VALUE_PARAMETER) {
          tv_expr = expr->GetChild(0).get();
          value_expr = expr->GetChild(1).get();
        }
      } else if (expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto l_type = expr->GetChild(0)->GetExpressionType();
        if (l_type == parser::ExpressionType::VALUE_CONSTANT ||
            l_type == parser::ExpressionType::VALUE_PARAMETER) {
          tv_expr = expr->GetChild(1).get();
          value_expr = expr->GetChild(0).get();

          // TODO([Execution Engine]): Does ExpressionType have to be flipped?
          expr_type = parser::ExpressionUtil::ReverseComparisonExpressionType(expr_type);
        }
      }

      // If found valid tv_expr and value_expr, update col_id_list, expr_type_list and val_list
      if (tv_expr != nullptr) {
        // Get the column's col_oid_t from catalog
        auto col_expr = dynamic_cast<const parser::ColumnValueExpression *>(tv_expr);
        TERRIER_ASSERT(col_expr, "ColumnValueExpression expected");

        auto col_oid = col_expr->GetColumnOid();
        TERRIER_ASSERT(col_oid != catalog::col_oid_t(-1), "ColumnValueExpression at scan should be bound");
        key_column_id_list.push_back(col_oid);

        // Update expr_type_list
        expr_type_list.push_back(expr_type);

        if (value_expr->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT) {
          auto cve = dynamic_cast<const parser::ConstantValueExpression *>(value_expr);
          TERRIER_ASSERT(cve, "ConstantValueExpression expected");

          // Update value_list
          type::TransientValue value = cve->GetValue();
          value_list.emplace_back(std::move(value));
        } else {
          auto poe = dynamic_cast<const parser::ParameterValueExpression *>(value_expr);
          TERRIER_ASSERT(poe, "ParameterValueExpression expected");

          // Update value_list
          value_list.push_back(type::TransientValueFactory::GetParameterOffset(poe->GetValueIdx()));
        }
      }
    }

    metadata->SetPredicateColumnIds(std::move(key_column_id_list));
    metadata->SetPredicateExprTypes(std::move(expr_type_list));
    metadata->SetPredicateValues(std::move(value_list));
  }

  /**
   * Checks whether a set of predicates can be satisfied with an index
   * @param tbl_oid OID of the table
   * @param index_oid OID of an index to check
   * @param preds_metadata IndexUtilMetadata from PopulateMetadata on predicates
   * @param output_metadata Output IndexUtilMetadata for creating IndexScan
   * @param accessor CatalogAccessor
   * @returns Whether index can be used
   */
  static bool SatisfiesPredicateWithIndex(catalog::table_oid_t tbl_oid, catalog::index_oid_t index_oid,
                                          IndexUtilMetadata *preds_metadata, IndexUtilMetadata *output_metadata,
                                          catalog::CatalogAccessor *accessor) {
    auto &index_schema = accessor->GetIndexSchema(index_oid);
    if (!SatisfiesBaseColumnRequirement(index_schema)) {
      return false;
    }

    if (!index_schema.Valid()) {
      return false;
    }

    std::vector<catalog::col_oid_t> mapped_cols;
    if (!GetIndexColOid(tbl_oid, index_schema, accessor, &mapped_cols)) {
      // Unable to translate indexkeycol_oid_t -> col_oid_t
      // Translation uses the IndexSchema::Column expression
      return false;
    }

    std::unordered_set<catalog::col_oid_t> index_cols;
    for (auto &id : mapped_cols) {
      index_cols.insert(id);
    }

    std::vector<catalog::col_oid_t> output_col_list;
    std::vector<parser::ExpressionType> output_expr_list;
    std::vector<type::TransientValue> output_val_list;

    // From predicate maetadata
    auto &input_col_list = preds_metadata->GetPredicateColumnIds();
    auto &input_expr_list = preds_metadata->GetPredicateExprTypes();
    auto &input_val_list = preds_metadata->GetPredicateValues();
    TERRIER_ASSERT(input_col_list.size() == input_expr_list.size() && input_col_list.size() == input_val_list.size(),
                   "Predicate metadata should all be equal length vectors");

    for (size_t offset = 0; offset < input_col_list.size(); offset++) {
      auto col_id = input_col_list[offset];
      if (index_cols.find(col_id) != index_cols.end()) {
        output_col_list.push_back(col_id);
        output_expr_list.push_back(input_expr_list[offset]);

        type::TransientValue val = input_val_list[offset];
        output_val_list.emplace_back(std::move(val));
      }
    }

    bool is_empty = output_col_list.empty();
    output_metadata->SetPredicateColumnIds(std::move(output_col_list));
    output_metadata->SetPredicateExprTypes(std::move(output_expr_list));
    output_metadata->SetPredicateValues(std::move(output_val_list));
    return !is_empty;
  }

 private:
  /**
   * Checks whether a Index satisfies the "base column" requirement.
   * The base column requirement (as defined from Peloton) is where
   * the index is built only on base table columns.
   * @param schema IndexSchema to evaluate
   * @returns TRUE if the "base column" requirement is met
   */
  static bool SatisfiesBaseColumnRequirement(const catalog::IndexSchema &schema) {
    for (auto &column : schema.GetColumns()) {
      if (!IsBaseColumn(column.StoredExpression().get())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Retrieves the catalog::col_oid_t equivalent for the index
   * @requires SatisfiesBaseColumnRequirement(schema)
   * @param tbl_oid Table the index belongs to
   * @param schema Schema
   * @param accessor CatalogAccessor to use
   * @param col_oids Vector to place col_oid_t translations
   * @returns TRUE if conversion successful
   */
  static bool GetIndexColOid(catalog::table_oid_t tbl_oid, const catalog::IndexSchema &schema,
                             catalog::CatalogAccessor *accessor, std::vector<catalog::col_oid_t> *col_oids) {
    TERRIER_ASSERT(SatisfiesBaseColumnRequirement(schema), "GetIndexColOid() pre-cond not satisfied");
    catalog::Schema tbl_schema = accessor->GetSchema(tbl_oid);
    if (tbl_schema.GetColumns().size() < schema.GetColumns().size()) {
      return false;
    }

    std::unordered_map<std::string, catalog::col_oid_t> schema_col;
    for (auto &column : tbl_schema.GetColumns()) {
      schema_col[column.Name()] = column.Oid();
    }

    for (auto &column : schema.GetColumns()) {
      auto *expr = column.StoredExpression().get();
      if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto *tv_expr = dynamic_cast<const parser::ColumnValueExpression *>(expr);
        TERRIER_ASSERT(tv_expr, "ColumnValueExpression expected");

        if (tv_expr->GetColumnOid() != catalog::INVALID_COLUMN_OID) {
          // IndexSchema's expression's col_oid is bound
          col_oids->push_back(tv_expr->GetColumnOid());
          continue;
        }

        auto it = schema_col.find(tv_expr->GetColumnName());
        if (it == schema_col.end()) {
          return false;  // column not found???
        }

        col_oids->push_back(it->second);
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
  static bool IsBaseColumn(const parser::AbstractExpression *expr) {
    return (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE);
  }
};

}  // namespace terrier::optimizer
