#include "optimizer/util.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "optimizer/optimizer_defs.h"
#include "parser/expression_util.h"

namespace noisepage::optimizer {

void OptimizerUtil::ExtractEquiJoinKeys(const std::vector<AnnotatedExpression> &join_predicates,
                                        std::vector<common::ManagedPointer<parser::AbstractExpression>> *left_keys,
                                        std::vector<common::ManagedPointer<parser::AbstractExpression>> *right_keys,
                                        const std::unordered_set<std::string> &left_alias,
                                        const std::unordered_set<std::string> &right_alias) {
  for (auto &expr_unit : join_predicates) {
    auto expr = expr_unit.GetExpr();
    if (expr->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL) {
      auto l_expr = expr->GetChild(0);
      auto r_expr = expr->GetChild(1);
      NOISEPAGE_ASSERT(l_expr->GetExpressionType() != parser::ExpressionType::VALUE_TUPLE &&
                           r_expr->GetExpressionType() != parser::ExpressionType::VALUE_TUPLE,
                       "DerivedValue should not exist here");

      // equi-join between two ColumnValueExpressions
      if (l_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE &&
          r_expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
        auto l_tv_expr = l_expr.CastManagedPointerTo<parser::ColumnValueExpression>();
        auto r_tv_expr = r_expr.CastManagedPointerTo<parser::ColumnValueExpression>();

        // Assign keys based on left and right join tables
        if (left_alias.find(l_tv_expr->GetTableName()) != left_alias.end() &&
            right_alias.find(r_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys->emplace_back(l_expr);
          right_keys->emplace_back(r_expr);
        } else if (left_alias.find(r_tv_expr->GetTableName()) != left_alias.end() &&
                   right_alias.find(l_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys->emplace_back(r_expr);
          right_keys->emplace_back(l_expr);
        }
      }
    }
  }
}

std::vector<parser::AbstractExpression *> OptimizerUtil::GenerateTableColumnValueExprs(
    catalog::CatalogAccessor *accessor, const std::string &alias, catalog::db_oid_t db_oid,
    catalog::table_oid_t tbl_oid) {
  // @note(boweic): we seems to provide all columns here, in case where there are
  // a lot of attributes and we're only visiting a few this is not efficient
  auto &schema = accessor->GetSchema(tbl_oid);
  auto &columns = schema.GetColumns();
  std::vector<parser::AbstractExpression *> exprs;
  for (auto &column : columns) {
    auto col_oid = column.Oid();
    auto *col_expr = new parser::ColumnValueExpression(alias, column.Name());
    col_expr->SetReturnValueType(column.Type());
    col_expr->SetDatabaseOID(db_oid);
    col_expr->SetTableOID(tbl_oid);
    col_expr->SetColumnOID(col_oid);

    col_expr->DeriveExpressionName();
    col_expr->DeriveReturnValueType();
    exprs.push_back(col_expr);
  }

  return exprs;
}

}  // namespace noisepage::optimizer
