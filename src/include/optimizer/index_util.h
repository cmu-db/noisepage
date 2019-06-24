#pragma once

#include <vector>
#include <unordered_map>

#include "catalog/catalog_accessor.h"
#include "catalog/index_schema.h"
#include "optimizer/properties.h"

// Collection of helper functions related to Indexes...

namespace terrier::optimizer {

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
      // TODO(boweic): Only consider ascending sort columns (catalog...)
      // TODO(wz2): Re-evaluate isBase for complex index selection
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
   * @requires CheckSortProperty(prop)
   * @param prop PropertySort to satisfy
   * @param tbl_oid OID of the table that the index is built on
   * @param idx_oid OID of index to use to satisfy
   * @param accessor CatalogAccessor
   * @returns TRUE if the specified index can fulfill sort property
   */
  static bool SatisfiesSortWithIndex(const PropertySort *prop,
                                     catalog::table_oid_t tbl_oid,
                                     catalog::index_oid_t idx_oid,
                                     catalog::CatalogAccessor *accessor) {
    TERRIER_ASSERT(CheckSortProperty(prop), "pre-cond not satisfied");

    // TODO(wz2): Future consider more elaborate indexes
    // For now, this logic only considers indexes where the columns
    // all satisfy the "base column" property.
    auto &index_schema = accessor->GetIndexSchema(idx_oid);
    if (!SatisfiesBaseColumnRequirement(index_schema)) {
      return false;
    }

    std::vector<catalog::col_oid_t> mapped_cols;
    if (!GetIndexColOid(tbl_oid, index_schema, accessor, mapped_cols)) {
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
      auto *tv_expr = dynamic_cast<const parser::TupleValueExpression *>(expr);
      TERRIER_ASSERT(tv_expr, "TupleValueExpression expected");

      // Sort(a,b,c) cannot be fulfilled by Index(a,c,b)
      auto col_match = std::get<2>(tv_expr->GetBoundOid()) == mapped_cols[idx];
      // TODO(wz2): need catalog flag for column sort direction
      // Sort(a ASC) cannot be fulfilled by Index(a DESC)
      auto dir_match = true;
      if (!col_match || !dir_match) {
        return false;
      }
    }

    return true;
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
    for (auto column : schema.GetColumns()) {
      if (!IsBaseColumn(column.GetExpression())) {
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
  static bool GetIndexColOid(catalog::table_oid_t tbl_oid,
                             const catalog::IndexSchema &schema,
                             catalog::CatalogAccessor *accessor,
                             std::vector<catalog::col_oid_t> &col_oids) {
    TERRIER_ASSERT(SatisfiesBaseColumnRequirement(schema),  "GetIndexColOid() pre-cond not satisfied");
    catalog::Schema tbl_schema = accessor->GetSchema(tbl_oid);
    if (tbl_schema.GetColumns().size() < schema.GetColumns().size()) {
      return false;
    }

    std::unordered_map<std::string, catalog::col_oid_t> schema_col;
    for (auto &column : tbl_schema.GetColumns()) {
      schema_col[column.GetName()] = column.GetOid();
    }

    for (auto &column : schema.GetColumns()) {
      auto *expr = column.GetExpression();
      if (expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE) {
        auto *tv_expr = dynamic_cast<const parser::TupleValueExpression*>(expr);
        TERRIER_ASSERT(tv_expr, "TupleValueExpression expected");

        if (accessor->GetTableOid(tv_expr->GetTableName()) != tbl_oid) {
          return false; // table not match???
        }

        auto it = schema_col.find(tv_expr->GetColumnName());
        if (it == schema_col.end()) {
          return false; // column not found???
        }

        col_oids.push_back(it->second);
      }
    }

    return true;
  }

  /**
   * Checks whether a given expression is a "base column".
   * A base column, as used and defined by Peloton, is where expr is a TupleValueExpression
   * @param expr AbstractExpression to evaluate
   * @returns TRUE if base column, false otherwise
   */
  static bool IsBaseColumn(const parser::AbstractExpression *expr) {
    return (expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE);
  }

};

}
