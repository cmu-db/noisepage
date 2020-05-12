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
    std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> lookup;
    if (!ConvertIndexKeyOidToColOid(accessor, tbl_oid, index_schema, &lookup, &mapped_cols)) {
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
      std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> *bounds) {
    auto &index_schema = accessor->GetIndexSchema(index_oid);
    if (!SatisfiesBaseColumnRequirement(index_schema)) {
      return false;
    }

    std::vector<catalog::col_oid_t> mapped_cols;
    std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> lookup;
    if (!ConvertIndexKeyOidToColOid(accessor, tbl_oid, index_schema, &lookup, &mapped_cols)) {
      // Unable to translate indexkeycol_oid_t -> col_oid_t
      // Translation uses the IndexSchema::Column expression
      return false;
    }

    std::unordered_set<catalog::col_oid_t> mapped_set;
    for (auto col : mapped_cols) mapped_set.insert(col);

    return CheckPredicates(index_schema, tbl_oid, lookup, mapped_set, predicates, allow_cves, scan_type, bounds);
  }

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
      std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> *bounds) {
    // TODO(wz2): Eventually consider supporting concatenating/shrinking ranges
    // Right now, this implementation only allows at most 1 range for an indexed column.
    // To concatenate/shrink ranges, we would need to be able to compare TransientValues.
    std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> open_highs;  // <index, low start>
    std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> open_lows;   // <index, high end>
    for (const auto &pred : predicates) {
      auto expr = pred.GetExpr();
      if (expr->HasSubquery()) return false;

      auto type = expr->GetExpressionType();
      switch (type) {
        case parser::ExpressionType::COMPARE_EQUAL:
        case parser::ExpressionType::COMPARE_NOT_EQUAL:
        case parser::ExpressionType::COMPARE_LESS_THAN:
        case parser::ExpressionType::COMPARE_GREATER_THAN:
        case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
        case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
          // TODO(wz2): Support more complex/predicates on indexes

          // Currently supports [column] (=/!=/>/>=/</<=) [value/parameter]
          // [column] = [column] will force a seq scan
          // [value] = [value] will force a seq scan (rewriter should fix this)
          auto ltype = expr->GetChild(0)->GetExpressionType();
          auto rtype = expr->GetChild(1)->GetExpressionType();

          common::ManagedPointer<parser::ColumnValueExpression> tv_expr;
          common::ManagedPointer<parser::AbstractExpression> idx_expr;
          if (ltype == parser::ExpressionType::COLUMN_VALUE &&
              (rtype == parser::ExpressionType::VALUE_CONSTANT || rtype == parser::ExpressionType::VALUE_PARAMETER)) {
            tv_expr = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
            idx_expr = expr->GetChild(1);
          } else if (rtype == parser::ExpressionType::COLUMN_VALUE &&
                     (ltype == parser::ExpressionType::VALUE_CONSTANT ||
                      ltype == parser::ExpressionType::VALUE_PARAMETER)) {
            tv_expr = expr->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
            idx_expr = expr->GetChild(0);
            type = parser::ExpressionUtil::ReverseComparisonExpressionType(type);
          } else if (allow_cves &&
                     (ltype == parser::ExpressionType::COLUMN_VALUE && rtype == parser::ExpressionType::COLUMN_VALUE)) {
            auto lexpr = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
            auto rexpr = expr->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
            if (lexpr->GetTableOid() == tbl_oid) {
              tv_expr = lexpr;
              idx_expr = expr->GetChild(1);
            } else {
              tv_expr = rexpr;
              idx_expr = expr->GetChild(0);
            }
          } else {
            // By derivation, all of these predicates should be CONJUNCTIVE_AND
            // so, we let the scan_predicate() handle evaluating the truthfulness.
            continue;
          }

          auto col_oid = tv_expr->GetColumnOid();
          if (mapped_cols.find(col_oid) != mapped_cols.end()) {
            auto idxkey = lookup.find(col_oid)->second;
            if (type == parser::ExpressionType::COMPARE_EQUAL) {
              // Exact is simulated as open high of idx_expr and open low of idx_expr
              open_highs[idxkey] = idx_expr;
              open_lows[idxkey] = idx_expr;
            } else if (type == parser::ExpressionType::COMPARE_LESS_THAN ||
                       type == parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO) {
              open_lows[idxkey] = idx_expr;
            } else if (type == parser::ExpressionType::COMPARE_GREATER_THAN ||
                       type == parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO) {
              open_highs[idxkey] = idx_expr;
            }
          }
          break;
        }
        default:
          // If a predicate can enlarge the result set, then (for now), reject.
          return false;
      }
    }

    // No predicate can actually be used
    if (open_highs.empty() && open_lows.empty()) return false;

    // Check predicate open/close ordering
    planner::IndexScanType scan_type = planner::IndexScanType::AscendingClosed;
    if (open_highs.size() == open_lows.size() && open_highs.size() == schema.GetColumns().size()) {
      // Generally on multi-column indexes, exact would result in comparing against unspecified attribute.
      // Only try to do an exact key lookup if potentially all attributes are specified.
      scan_type = planner::IndexScanType::Exact;
    }

    for (auto &col : schema.GetColumns()) {
      auto oid = col.Oid();
      if (open_highs.find(oid) == open_highs.end() && open_lows.find(oid) == open_lows.end()) {
        // Index predicate ordering is busted
        break;
      }

      if (open_highs.find(oid) != open_highs.end() && open_lows.find(oid) != open_lows.end()) {
        bounds->insert(std::make_pair(oid, std::vector<planner::IndexExpression>{open_highs[oid], open_lows[oid]}));

        // A range is defined but we are doing exact scans, so make ascending closed
        // If already doing ascending closed, ascending open then it would be a matter of
        // picking the right low/high key at the plan_generator stage of processing.
        if (open_highs[oid] != open_lows[oid] && scan_type == planner::IndexScanType::Exact)
          scan_type = planner::IndexScanType::AscendingClosed;
      } else if (open_highs.find(oid) != open_highs.end()) {
        if (scan_type == planner::IndexScanType::Exact || scan_type == planner::IndexScanType::AscendingClosed ||
            scan_type == planner::IndexScanType::AscendingOpenHigh) {
          scan_type = planner::IndexScanType::AscendingOpenHigh;
        } else {
          // OpenHigh scan is not compatible with an OpenLow scan
          // Revert to a sequential scan
          break;
        }

        bounds->insert(std::make_pair(
            oid, std::vector<planner::IndexExpression>{open_highs[oid], planner::IndexExpression(nullptr)}));
      } else if (open_lows.find(oid) != open_lows.end()) {
        if (scan_type == planner::IndexScanType::Exact || scan_type == planner::IndexScanType::AscendingClosed ||
            scan_type == planner::IndexScanType::AscendingOpenLow) {
          scan_type = planner::IndexScanType::AscendingOpenLow;
        } else {
          // OpenLow scan is not compatible with an OpenHigh scan
          // Revert to a sequential scan
          break;
        }

        bounds->insert(std::make_pair(
            oid, std::vector<planner::IndexExpression>{planner::IndexExpression(nullptr), open_lows[oid]}));
      }
    }

    *idx_scan_type = scan_type;
    return !bounds->empty();
  }

  /**
   * Retrieves the catalog::col_oid_t equivalent for the index
   * @requires SatisfiesBaseColumnRequirement(schema)
   * @param accessor CatalogAccessor to use
   * @param tbl_oid Table the index belongs to
   * @param schema Schema
   * @param key_map Mapping from col_oid_t to indexkeycol_oid_t
   * @param col_oids Vector to place col_oid_t translations
   * @returns TRUE if conversion successful
   */
  static bool ConvertIndexKeyOidToColOid(catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid,
                                         const catalog::IndexSchema &schema,
                                         std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> *key_map,
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
          key_map->insert(std::make_pair(tv_expr->GetColumnOid(), column.Oid()));
          continue;
        }

        auto it = schema_col.find(tv_expr->GetColumnName());
        TERRIER_ASSERT(it != schema_col.end(), "Inconsistency between IndexSchema and table schema");
        col_oids->push_back(it->second);
        key_map->insert(std::make_pair(it->second, column.Oid()));
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
