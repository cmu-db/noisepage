#include "optimizer/index_util.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/index_schema.h"
#include "optimizer/properties.h"
#include "parser/expression_util.h"

namespace noisepage::optimizer {

bool IndexUtil::SatisfiesSortWithIndex(catalog::CatalogAccessor *accessor, const PropertySort *prop,
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

bool IndexUtil::SatisfiesPredicateWithIndex(
    catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid, const std::string &tbl_alias,
    catalog::index_oid_t index_oid, const std::vector<AnnotatedExpression> &predicates, bool allow_cves,
    planner::IndexScanType *scan_type,
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

  return CheckPredicates(index_schema, tbl_oid, tbl_alias, lookup, mapped_set, predicates, allow_cves, scan_type,
                         bounds);
}

bool IndexUtil::CheckPredicates(
    const catalog::IndexSchema &schema, catalog::table_oid_t tbl_oid, const std::string &tbl_alias,
    const std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> &lookup,
    const std::unordered_set<catalog::col_oid_t> &mapped_cols, const std::vector<AnnotatedExpression> &predicates,
    bool allow_cves, planner::IndexScanType *idx_scan_type,
    std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> *bounds) {
  // TODO(wz2): Eventually consider supporting concatenating/shrinking ranges
  // Right now, this implementation only allows at most 1 range for an indexed column.
  // To concatenate/shrink ranges, we would need to be able to compare TransientValues.
  std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> open_highs;  // <index, low start>
  std::unordered_map<catalog::indexkeycol_oid_t, planner::IndexExpression> open_lows;   // <index, high end>
  bool left_side = true;
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
          if (lexpr->GetTableOid() == tbl_oid &&
              (rexpr->GetTableOid() != tbl_oid || lexpr->GetTableName() == tbl_alias)) {
            tv_expr = lexpr;
            idx_expr = expr->GetChild(1);
            left_side = true;
          } else {
            tv_expr = rexpr;
            idx_expr = expr->GetChild(0);
            left_side = false;
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
            if (left_side) {
              open_lows[idxkey] = idx_expr;
            } else {
              open_highs[idxkey] = idx_expr;
            }
          } else if (type == parser::ExpressionType::COMPARE_GREATER_THAN ||
                     type == parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO) {
            if (left_side) {
              open_highs[idxkey] = idx_expr;
            } else {
              open_lows[idxkey] = idx_expr;
            }
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

  if (schema.Type() == storage::index::IndexType::HASHMAP && scan_type != planner::IndexScanType::Exact) {
    // This is a range-based scan, but this is a hashmap so it cannot satisfy the predicate.
    //
    // TODO(John): Ideally this check should be based off of lookups in the catalog.  However, we do not
    // support dynamically defined index types nor do we have `pg_op*` catalog tables to store the necessary
    // data.  For now, this check is sufficient for what the optimizer is doing.
    return false;
  }

  *idx_scan_type = scan_type;
  return !bounds->empty();
}

bool IndexUtil::ConvertIndexKeyOidToColOid(catalog::CatalogAccessor *accessor, catalog::table_oid_t tbl_oid,
                                           const catalog::IndexSchema &schema,
                                           std::unordered_map<catalog::col_oid_t, catalog::indexkeycol_oid_t> *key_map,
                                           std::vector<catalog::col_oid_t> *col_oids) {
  NOISEPAGE_ASSERT(SatisfiesBaseColumnRequirement(schema), "GetIndexColOid() pre-cond not satisfied");
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
      NOISEPAGE_ASSERT(it != schema_col.end(), "Inconsistency between IndexSchema and table schema");
      col_oids->push_back(it->second);
      key_map->insert(std::make_pair(it->second, column.Oid()));
    }
  }

  return true;
}

}  // namespace noisepage::optimizer
