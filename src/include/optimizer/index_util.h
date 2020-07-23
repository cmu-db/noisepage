#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "optimizer/properties.h"

namespace terrier::catalog {
class CatalogAccessor;
class IndexSchema;
}  // namespace terrier::catalog

namespace terrier::parser {
class AbstractExpression;
}  // namespace terrier::parser

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
   * TODO(dpatra): once we add support for descending columns in the index fix this
   *
   * @param prop PropertySort to evaluate
   * @returns TRUE if should search for index
   */
  static bool CheckSortProperty(const PropertySort *prop) {
    auto sort_col_size = prop->GetSortColumnSize();
    for (size_t idx = 0; idx < sort_col_size; idx++) {
      // TODO(wz2): Consider descending when catalog/index support
      auto is_asc = prop->GetSortAscending(static_cast<int>(idx)) == catalog::OrderByOrderingType::ASC;
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
                                     catalog::table_oid_t tbl_oid, catalog::index_oid_t idx_oid,
                                     std::unordered_map<catalog::indexkeycol_oid_t,
                                                        std::vector<planner::IndexExpression>> *bounds = nullptr);

  /**
   * Checks whether a set of predicates can be satisfied with an index
   * @param accessor CatalogAccessor
   * @param tbl_oid OID of the table
   * @param index_oid OID of an index to check
   * @param predicates List of predicates
   * @param allow_cves Allow CVEs
   * @param scan_type IndexScanType to utilize
   * @param bounds Relevant bounds for the index scan
   * @returns Whether index can be used
   */
  static bool SatisfiesPredicateWithIndex(
      catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid, catalog::index_oid_t index_oid,
      const std::vector<AnnotatedExpression> &predicates, bool allow_cves, planner::IndexScanType *scan_type,
      std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> *bounds);

 private:
  /**
   * Check whether predicate can take part in index computation
   * @param schema Index Schema
   * @param tbl_oid Table OID
   * @param lookup map from col_oid_t to indexkeycol_oid_t
   * @param mapped_cols col_oid_t from index schema's indexkeycol_oid_t
   * @param predicates Set of predicates to attempt to satisfy
   * @param allow_cves Allow ColumnValueExpressions
   * @param idx_scan_type IndexScanType to utilize
   * @param bounds Relevant bounds for the index scan
   * @returns Whether predicate can be utilized
   */
  static bool CheckPredicates(
      const catalog::IndexSchema &schema, catalog::table_oid_t tbl_oid,
      const std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> &lookup,
      const std::unordered_set<catalog::col_oid_t> &mapped_cols, const std::vector<AnnotatedExpression> &predicates,
      bool allow_cves, planner::IndexScanType *idx_scan_type,
      std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> *bounds);

  /**
   * Retrieves the catalog::col_oid_t equivalent for the index
   * @requires SatisfiesBaseColumnRequirement(index_schema)
   * @param accessor CatalogAccessor to use
   * @param tbl_oid Table the index belongs to
   * @param index_schema Schema
   * @param key_map Mapping from col_oid_t to indexkeycol_oid_t
   * @param col_oids Vector to place col_oid_t translations
   * @returns TRUE if conversion successful
   */
  static bool ConvertIndexKeyOidToColOid(catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid,
                                         const catalog::IndexSchema &index_schema,
                                         std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> *key_map,
                                         std::vector<catalog::col_oid_t> *col_oids);

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
