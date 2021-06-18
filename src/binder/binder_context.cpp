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
#include "parser/expression/constant_value_expression.h"
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

  AddRegularTable(accessor, db_id, table_ref->GetNamespaceName(), table_ref->GetTableName(),
                  table_ref->GetAlias().GetName());
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

  std::unordered_map<parser::AliasType, type::TypeId> column_alias_map;

  for (auto &col : new_columns) {
    column_alias_map[parser::AliasType(col->GetColumnName())] = col->GetValueType();
  }
  nested_table_alias_map_[new_table_name] = column_alias_map;
}

void BinderContext::AddNestedTable(const std::string &table_alias,
                                   const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list,
                                   const std::vector<parser::AliasType> &col_aliases) {
  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(fmt::format("Duplicate alias \"{}\"", table_alias),
                           common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }

  std::unordered_map<parser::AliasType, type::TypeId> column_alias_map{};
  auto cols = col_aliases.size();
  for (std::size_t i = 0; i < select_list.size(); i++) {
    auto &expr = select_list[i];
    parser::AliasType alias;
    if (i < cols) {
      alias = col_aliases[i];
    } else if (!expr->GetAliasName().empty()) {
      alias = expr->GetAlias();
    } else if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto tv_expr = reinterpret_cast<parser::ColumnValueExpression *>(expr.Get());
      alias = parser::AliasType(tv_expr->GetColumnName());
    } else {
      continue;
    }

    column_alias_map[alias] = expr->GetReturnValueType();
  }
  nested_table_alias_map_[table_alias] = column_alias_map;
}

void BinderContext::AddCTETable(const std::string &table_name,
                                const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list,
                                const std::vector<parser::AliasType> &col_aliases) {
  if (nested_table_alias_map_.find(table_name) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION("Duplicate CTE table definition", common::ErrorCode::ERRCODE_DUPLICATE_TABLE);
  }
  std::unordered_map<parser::AliasType, type::TypeId> nested_column_mappings{};
  for (std::size_t i = 0; i < col_aliases.size(); i++) {
    NOISEPAGE_ASSERT(select_list[i]->GetReturnValueType() != type::TypeId::INVALID, "CTE column type not resolved");
    nested_column_mappings[col_aliases[i]] = select_list[i]->GetReturnValueType();
  }

  nested_table_alias_map_[table_name] = nested_column_mappings;
}

void BinderContext::AddCTETableAlias(const std::string &cte_table_name, const std::string &table_alias) {
  if (cte_table_name == table_alias) {
    return;
  }

  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(("Duplicate alias " + table_alias).c_str(), common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }

  // Find schema for CTE table in nested_table_map in this context or previous contexts
  auto current_context = common::ManagedPointer(this);
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(cte_table_name);
    if (iter != current_context->nested_table_alias_map_.end()) {
      // Copy schema for CTE table for this alias
      nested_table_alias_map_[table_alias] = iter->second;
      break;
    }
    current_context = current_context->GetUpperContext();
  }

  if (nested_table_alias_map_.find(table_alias) == nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(("CTE table not in nested alias map " + cte_table_name).c_str(),
                           common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
  }
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

void BinderContext::SetTableName(common::ManagedPointer<parser::ColumnValueExpression> expr,
                                 common::ManagedPointer<parser::SelectStatement> node) {
  // In the case of FROM-less queries, we do not need to set table names on expressions
  if (node->GetSelectTable() != nullptr) {
    auto type = node->GetSelectTable()->GetTableReferenceType();
    if (type == parser::TableReferenceType::NAME || type == parser::TableReferenceType::SELECT) {
      const auto &table_alias = node->GetSelectTable()->GetAlias();
      auto expr_table_alias = expr->GetAlias();
      if (expr_table_alias.Empty()) {
        expr->SetTableAlias(table_alias);
      } else if (expr_table_alias != table_alias) {
        throw BINDER_EXCEPTION(fmt::format("missing FROM-clause entry for table \"{}\"", expr_table_alias.GetName()),
                               common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
      }
    }
  }
}

bool BinderContext::SetColumnPosTuple(common::ManagedPointer<parser::ColumnValueExpression> expr) {
  auto col_name = expr->GetColumnName();
  auto alias_name = parser::AliasType(expr->GetColumnName());
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
          auto &table_name = entry.first;
          auto table_alias = current_context->GetOrCreateTableAlias(table_name);
          expr->SetTableAlias(table_alias);
        } else {
          throw BINDER_EXCEPTION(fmt::format("Ambiguous column name \"{}\"", col_name),
                                 common::ErrorCode::ERRCODE_AMBIGUOUS_COLUMN);
        }
      }
    }
    // Check nested table
    for (auto &entry : current_context->nested_table_alias_map_) {
      auto iter = entry.second.find(alias_name);
      bool get_match = iter != entry.second.end();
      auto matches = std::count_if(entry.second.begin(), entry.second.end(),
                                   [=](auto it) { return entry.second.key_eq()(it.first, alias_name); });
      if (get_match) {
        // if there is more than one match, then the requested alias name is ambiguous
        if (!find_matched && (matches == 1)) {
          // First match
          find_matched = true;
          auto &table_name = entry.first;
          auto table_alias = current_context->GetOrCreateTableAlias(table_name);
          expr->SetTableAlias(table_alias);
          expr->SetReturnValueType(entry.second[alias_name]);
          expr->SetColumnName(col_name);
          expr->SetColumnOID(catalog::MakeTempOid<catalog::col_oid_t>(iter->first.GetSerialNo().UnderlyingValue()));
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

bool BinderContext::CheckNestedTableColumn(const parser::AliasType &alias, const std::string &col_name,
                                           common::ManagedPointer<parser::ColumnValueExpression> expr) {
  auto current_context = common::ManagedPointer(this);
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(alias.GetName());
    if (iter != current_context->nested_table_alias_map_.end()) {
      auto col_iter = iter->second.find(parser::AliasType(col_name));
      if (col_iter == iter->second.end()) {
        throw BINDER_EXCEPTION(fmt::format("Cannot find column \"{}\"", col_name),
                               common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
      }
      expr->SetReturnValueType(col_iter->second);
      expr->SetDepth(current_context->depth_);
      expr->SetColumnName(col_name);
      expr->SetTableAlias(alias);
      expr->SetColumnOID(catalog::MakeTempOid<catalog::col_oid_t>(col_iter->first.GetSerialNo().UnderlyingValue()));
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
  const bool target_specified = table_star->IsTargetTableSpecified();

  bool target_found = false;
  for (auto &entry : regular_table_alias_list_) {
    // If a target is specified, check that the entry matches the target table
    if (target_specified && entry != table_star->GetTargetTable()) {
      continue;
    }

    target_found = true;
    auto table_data = regular_table_alias_map_[entry];
    auto &schema = std::get<2>(table_data);
    const auto col_cnt = schema.GetColumns().size();
    auto table_alias = GetOrCreateTableAlias(entry);
    for (std::uint32_t i = 0; i < col_cnt; ++i) {
      const auto &col_obj = schema.GetColumn(i);
      auto tv_expr = new parser::ColumnValueExpression(table_alias, std::string(col_obj.Name()));
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
    for (const auto &entry : nested_table_alias_map_) {
      const auto &table_alias_name = entry.first;
      const auto &cols = entry.second;

      // TODO(tanujnay112) make the nested_table_alias_map hold ordered maps
      // this is to order the generated columns in the same order that they appear
      // in the nested table; the serial number of their aliases signifies this ordering
      std::vector<std::pair<parser::AliasType, type::TypeId>> cols_vector{cols.begin(), cols.end()};
      std::sort(
          cols_vector.begin(), cols_vector.end(),
          [](const std::pair<parser::AliasType, type::TypeId> &A, const std::pair<parser::AliasType, type::TypeId> &B) {
            return A.first.GetSerialNo() < B.first.GetSerialNo();
          });
      const auto table_alias = GetOrCreateTableAlias(table_alias_name);
      for (const auto &col_entry : cols_vector) {
        auto tv_expr = new parser::ColumnValueExpression{table_alias, col_entry.first.GetName()};
        tv_expr->SetReturnValueType(col_entry.second);
        tv_expr->DeriveExpressionName();
        tv_expr->SetColumnOID(
            catalog::MakeTempOid<catalog::col_oid_t>(col_entry.first.GetSerialNo().UnderlyingValue()));
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

void BinderContext::AddTableAliasMapping(const std::string &alias_name, const parser::AliasType &alias_type) {
  table_alias_name_to_type_map_[alias_name] = alias_type;
}
bool BinderContext::HasTableAlias(const std::string &alias_name) {
  return table_alias_name_to_type_map_.find(alias_name) != table_alias_name_to_type_map_.end();
}
parser::AliasType &BinderContext::GetTableAlias(const std::string &alias_name) {
  return table_alias_name_to_type_map_[alias_name];
}

parser::AliasType BinderContext::GetOrCreateTableAlias(const std::string &alias_name) {
  /*
   * When binding a SELECT statement we would have saved the table alias of a TableRef with it's unique serial number so
   * we return that. For other query types such as UPDATE and DELETE we do not have an alias saved so we just make a new
   * one using the table name.
   */
  if (HasTableAlias(alias_name)) {
    return GetTableAlias(alias_name);
  }
  return parser::AliasType(alias_name);
}

parser::AliasType BinderContext::FindTableAlias(const std::string &alias_name) {
  /*
   * When binding a SELECT statement we would have saved the table alias of a TableRef with it's unique serial number so
   * we return that when we find it. For other query types such as UPDATE and DELETE we do not have an alias saved so we
   * just make a new one using the table name once we've searched through all contexts.
   */
  auto current_context = common::ManagedPointer(this);
  while (current_context != nullptr) {
    if (current_context->HasTableAlias(alias_name)) {
      return current_context->GetTableAlias(alias_name);
    }
    current_context = current_context->GetUpperContext();
  }
  return parser::AliasType(alias_name);
}

}  // namespace noisepage::binder
