#include "binder/binder_context.h"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/table_star_expression.h"
#include "parser/postgresparser.h"
#include "parser/table_ref.h"

namespace noisepage::binder {

void BinderContext::AddRegularTable(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                    common::ManagedPointer<parser::TableRef> table_ref, const catalog::db_oid_t db_id) {
  if (!(table_ref->GetDatabaseName().empty())) {
    const auto db_oid = accessor->GetDatabaseOid(table_ref->GetDatabaseName());
    if (db_oid == catalog::INVALID_DATABASE_OID)
      throw BINDER_EXCEPTION("Database does not exist", common::ErrorCode::ERRCODE_UNDEFINED_DATABASE);
    if (db_oid != db_id)
      throw BINDER_EXCEPTION("cross-database references are not implemented: ",
                             common::ErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED);
  }

  AddRegularTable(accessor, db_id, table_ref->GetNamespaceName(), table_ref->GetTableName(), table_ref->GetAlias());
}

void BinderContext::AddRegularTable(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                    const catalog::db_oid_t db_id, const std::string &namespace_name,
                                    const std::string &table_name, const std::string &table_alias) {
  catalog::table_oid_t table_id;
  if (!namespace_name.empty()) {
    auto namespace_id = accessor->GetNamespaceOid(namespace_name);
    if (namespace_id == catalog::INVALID_NAMESPACE_OID) {
      throw BINDER_EXCEPTION(fmt::format("Unknown namespace name \"{}\"", namespace_name),
                             common::ErrorCode::ERRCODE_UNDEFINED_SCHEMA);
    }

    table_id = accessor->GetTableOid(namespace_id, table_name);
    if (table_id == catalog::INVALID_TABLE_OID) {
      throw BINDER_EXCEPTION(fmt::format("relation \"{}\" does not exist", table_name),
                             common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
    }
  } else {
    table_id = accessor->GetTableOid(table_name);
    if (table_id == catalog::INVALID_TABLE_OID) {
      throw BINDER_EXCEPTION(fmt::format("relation \"{}\" does not exist", table_name),
                             common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
    }
  }

  // TODO(Matt): deep copy of schema, should not be done
  auto schema = accessor->GetSchema(table_id);

  if (nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(fmt::format("Duplicate alias \"{}\"", table_alias),
                           common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }

  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(fmt::format("Duplicate alias \"{}\"", table_alias),
                           common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }
  regular_table_alias_map_[table_alias] = std::make_tuple(db_id, table_id, schema);
  regular_table_alias_list_.push_back(table_alias);
}

void BinderContext::AddNewTable(const std::string &new_table_name,
                                const std::vector<common::ManagedPointer<parser::ColumnDefinition>> &new_columns) {
  if (regular_table_alias_map_.find(new_table_name) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(new_table_name) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(fmt::format("Duplicate alias \"{}\"", new_table_name),
                           common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }

  std::unordered_map<std::string, type::TypeId> column_alias_map;

  for (auto &col : new_columns) {
    column_alias_map[col->GetColumnName()] = col->GetValueType();
  }
  nested_table_alias_map_[new_table_name] = column_alias_map;
}

void BinderContext::AddNestedTable(const std::string &table_alias,
                                   const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list) {
  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(fmt::format("Duplicate alias \"{}\"", table_alias),
                           common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }

  std::unordered_map<std::string, type::TypeId> column_alias_map;
  for (auto &expr : select_list) {
    std::string alias;
    if (!expr->GetAlias().empty()) {
      alias = expr->GetAlias();
    } else if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto tv_expr = reinterpret_cast<parser::ColumnValueExpression *>(expr.Get());
      alias = tv_expr->GetColumnName();
    } else {
      continue;
    }

    std::transform(alias.begin(), alias.end(), alias.begin(), ::tolower);
    column_alias_map[alias] = expr->GetReturnValueType();
  }
  nested_table_alias_map_[table_alias] = column_alias_map;
}

bool BinderContext::ColumnInSchema(const catalog::Schema &schema, const std::string &col_name) {
  try {
    const auto &column_object UNUSED_ATTRIBUTE = schema.GetColumn(col_name);
  } catch (const std::out_of_range &oor) {
    return false;
  }
  return true;
}

void BinderContext::SetColumnPosTuple(const std::string &col_name,
                                      std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> tuple,
                                      common::ManagedPointer<parser::ColumnValueExpression> expr) {
  auto column_object = std::get<2>(tuple).GetColumn(col_name);
  expr->SetDatabaseOID(std::get<0>(tuple));
  expr->SetTableOID(std::get<1>(tuple));
  expr->SetColumnOID(column_object.Oid());
  expr->SetColumnName(col_name);
  expr->SetReturnValueType(column_object.Type());
}

bool BinderContext::SetColumnPosTuple(common::ManagedPointer<parser::ColumnValueExpression> expr) {
  auto col_name = expr->GetColumnName();
  std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);

  bool find_matched = false;
  auto current_context = common::ManagedPointer(this);
  while (current_context != nullptr) {
    // Check regular table
    for (auto &entry : current_context->regular_table_alias_map_) {
      bool get_matched = ColumnInSchema(std::get<2>(entry.second), col_name);
      if (get_matched) {
        if (!find_matched) {
          // First match
          find_matched = true;
          SetColumnPosTuple(col_name, entry.second, expr);
          expr->SetTableName(entry.first);
        } else {
          throw BINDER_EXCEPTION(fmt::format("Ambiguous column name \"{}\"", col_name),
                                 common::ErrorCode::ERRCODE_AMBIGUOUS_COLUMN);
        }
      }
    }
    // Check nested table
    for (auto &entry : current_context->nested_table_alias_map_) {
      bool get_match = entry.second.find(col_name) != entry.second.end();
      if (get_match) {
        if (!find_matched) {
          // First match
          find_matched = true;
          expr->SetTableName(entry.first);
          expr->SetReturnValueType(entry.second[col_name]);
          expr->SetColumnName(col_name);
        } else {
          throw BINDER_EXCEPTION(fmt::format("Ambiguous column name \"{}\"", col_name),
                                 common::ErrorCode::ERRCODE_AMBIGUOUS_COLUMN);
        }
      }
    }
    if (find_matched) {
      expr->SetDepth(current_context->depth_);
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::GetRegularTableObj(
    const std::string &alias, common::ManagedPointer<parser::ColumnValueExpression> expr,
    common::ManagedPointer<std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema>> tuple) {
  auto current_context = common::ManagedPointer(this);
  while (current_context != nullptr) {
    auto iter = current_context->regular_table_alias_map_.find(alias);
    if (iter != current_context->regular_table_alias_map_.end()) {
      *tuple = iter->second;
      expr->SetDepth(current_context->depth_);
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::CheckNestedTableColumn(const std::string &alias, const std::string &col_name,
                                           common::ManagedPointer<parser::ColumnValueExpression> expr) {
  auto current_context = common::ManagedPointer(this);
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(alias);
    if (iter != current_context->nested_table_alias_map_.end()) {
      auto col_iter = iter->second.find(col_name);
      if (col_iter == iter->second.end()) {
        throw BINDER_EXCEPTION(fmt::format("Cannot find column \"{}\"", col_name),
                               common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
      }
      expr->SetReturnValueType(col_iter->second);
      expr->SetDepth(current_context->depth_);
      expr->SetColumnName(col_name);
      expr->SetTableName(alias);
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

void BinderContext::GenerateAllColumnExpressions(
    common::ManagedPointer<parser::TableStarExpression> table_star,
    common::ManagedPointer<parser::ParseResult> parse_result,
    common::ManagedPointer<std::vector<common::ManagedPointer<parser::AbstractExpression>>> exprs) {
  bool target_specified = table_star->IsTargetTableSpecified();
  bool target_found = false;
  for (auto &entry : regular_table_alias_list_) {
    // If a target is specified, check that the entry matches the target table
    if (target_specified && entry != table_star->GetTargetTable()) {
      continue;
    }

    target_found = true;
    auto table_data = regular_table_alias_map_[entry];
    auto &schema = std::get<2>(table_data);
    auto col_cnt = schema.GetColumns().size();
    for (uint32_t i = 0; i < col_cnt; i++) {
      const auto &col_obj = schema.GetColumn(i);
      auto tv_expr = new parser::ColumnValueExpression(std::string(entry), std::string(col_obj.Name()));
      tv_expr->SetReturnValueType(col_obj.Type());
      tv_expr->DeriveExpressionName();
      tv_expr->SetDatabaseOID(std::get<0>(table_data));
      tv_expr->SetTableOID(std::get<1>(table_data));
      tv_expr->SetColumnOID(col_obj.Oid());
      tv_expr->SetDepth(depth_);

      auto unique_tv_expr =
          std::unique_ptr<parser::AbstractExpression>(reinterpret_cast<parser::AbstractExpression *>(tv_expr));
      parse_result->AddExpression(std::move(unique_tv_expr));
      auto new_tv_expr = common::ManagedPointer(parse_result->GetExpressions().back());
      exprs->push_back(new_tv_expr);
    }
  }

  if (!target_specified) {
    // If a target is not specified, continue generating column value expressions
    for (auto &entry : nested_table_alias_map_) {
      auto &table_alias = entry.first;
      auto &cols = entry.second;
      for (auto &col_entry : cols) {
        auto tv_expr = new parser::ColumnValueExpression(std::string(table_alias), std::string(col_entry.first));
        tv_expr->SetReturnValueType(col_entry.second);
        tv_expr->DeriveExpressionName();
        tv_expr->SetDepth(depth_);

        auto unique_tv_expr =
            std::unique_ptr<parser::AbstractExpression>(reinterpret_cast<parser::AbstractExpression *>(tv_expr));
        parse_result->AddExpression(std::move(unique_tv_expr));
        auto new_tv_expr = common::ManagedPointer(parse_result->GetExpressions().back());
        // All derived columns do not have bound oids, thus keep them as INVALID_OIDs
        exprs->push_back(new_tv_expr);
      }
    }
  } else if (target_specified && !target_found) {
    // Case where a target is specified but not found
    throw BINDER_EXCEPTION(fmt::format("Invalid table reference {}", table_star->GetTargetTable()),
                           common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
  }
}

common::ManagedPointer<BinderContext::TableMetadata> BinderContext::GetTableMapping(const std::string &table_name) {
  if (regular_table_alias_map_.find(table_name) == regular_table_alias_map_.end()) {
    return nullptr;
  }
  return common::ManagedPointer(&regular_table_alias_map_[table_name]);
}

}  // namespace noisepage::binder
