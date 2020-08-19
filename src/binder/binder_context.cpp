#include "binder/binder_context.h"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/error/exception.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "parser/table_ref.h"

namespace terrier::binder {

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
}

void BinderContext::AddNewTable(const std::string &new_table_name,
                                const std::vector<common::ManagedPointer<parser::ColumnDefinition>> &new_columns) {
  if (regular_table_alias_map_.find(new_table_name) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(new_table_name) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(fmt::format("Duplicate alias \"{}\"", new_table_name),
                           common::ErrorCode::ERRCODE_DUPLICATE_ALIAS);
  }

  std::unordered_map<parser::AliasType, type::TypeId, parser::AliasType::HashKey> column_alias_map;

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

  std::unordered_map<parser::AliasType, type::TypeId, parser::AliasType::HashKey> column_alias_map;
  size_t i = 0;
  auto cols = col_aliases.size();
  for (auto &expr : select_list) {
    parser::AliasType alias;
    if (i < cols) {
      alias = col_aliases[i];
    } else if (!expr->GetAliasName().empty()) {
      alias = expr->GetAlias();
    } else if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto tv_expr = reinterpret_cast<parser::ColumnValueExpression *>(expr.Get());
      alias = parser::AliasType(tv_expr->GetColumnName());
    } else {
      i++;
      continue;
    }

    column_alias_map[alias] = expr->GetReturnValueType();
    i++;
  }
  nested_table_alias_map_[table_alias] = column_alias_map;
}

void BinderContext::AddCTETable(common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                const std::string &table_name,
                                const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list,
                                const std::vector<parser::AliasType> &col_aliases) {
  if (nested_table_alias_map_.find(table_name) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION("Duplicate cte table definition", common::ErrorCode::ERRCODE_DUPLICATE_TABLE);
  }
//  std::vector<catalog::Schema::Column> schema_columns;
  std::unordered_map<parser::AliasType, type::TypeId,
                     parser::AliasType::HashKey> nested_column_mappings;
  for (size_t i = 0; i < col_aliases.size(); i++) {
//    catalog::Schema::Column col(col_aliases[i].GetName(), select_list[i]->GetReturnValueType(), false,
//                                parser::ConstantValueExpression(select_list[i]->GetReturnValueType()),
//                                TEMP_OID(catalog::col_oid_t, i));
//    schema_columns.push_back(col);
    nested_column_mappings[col_aliases[i]] = select_list[i]->GetReturnValueType();
  }

//  catalog::Schema cte_schema(schema_columns);
//  nested_table_alias_map_[table_name] =
//      TableMetadata(TEMP_OID(catalog::db_oid_t, catalog::NULL_OID),
//                    TEMP_OID(catalog::table_oid_t, accessor->GetNewTempOid()), schema_columns);
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
  if (node->GetSelectTable() != nullptr) {
    auto type = node->GetSelectTable()->GetTableReferenceType();
    if (type == parser::TableReferenceType::NAME || type == parser::TableReferenceType::SELECT) {
      auto table_alias = node->GetSelectTable()->GetAlias();
      auto expr_table_name = expr->GetTableName();
      if (expr_table_name.empty()) {
        expr->SetTableName(table_alias);
      } else if (expr_table_name != table_alias) {
        throw BINDER_EXCEPTION(fmt::format("missing FROM-clause entry for table \"{}\"", expr_table_name),
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
          expr->SetTableName(entry.first);
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
                                   [=](auto it){ return entry.second.key_eq()(it.first, alias_name);});
      if (get_match) {
        if (!find_matched && (matches == 1)) {
          // First match
          find_matched = true;
          expr->SetTableName(entry.first);
          expr->SetReturnValueType(entry.second[alias_name]);
          expr->SetColumnName(col_name);
          expr->SetColumnOID(TEMP_OID(catalog::col_oid_t , iter->first.GetSerialNo()));
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
      auto col_iter = iter->second.find(parser::AliasType(col_name));
      if (col_iter == iter->second.end()) {
        throw BINDER_EXCEPTION(fmt::format("Cannot find column \"{}\"", col_name),
                               common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
      }
      expr->SetReturnValueType(col_iter->second);
      expr->SetDepth(current_context->depth_);
      expr->SetColumnName(col_name);
      expr->SetTableName(alias);
      expr->SetColumnOID(TEMP_OID(catalog::col_oid_t, col_iter->first.GetSerialNo()));
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

void BinderContext::GenerateAllColumnExpressions(
    common::ManagedPointer<parser::ParseResult> parse_result,
    common::ManagedPointer<std::vector<common::ManagedPointer<parser::AbstractExpression>>> exprs,
    common::ManagedPointer<parser::SelectStatement> stmt,
    std::string table_name) {
  // Set containing tables whose columns are to be included in the SELECT * query results
  std::unordered_set<std::string> constituent_table_aliases;
  stmt->GetSelectTable()->GetConstituentTableAliases(&constituent_table_aliases);
  if (!table_name.empty()) {
    if (constituent_table_aliases.count(table_name) == 0) {
      // SELECT table_name.* FROM ..., where the from clause does not contain table_name
      throw BINDER_EXCEPTION(fmt::format("missing FROM-clause entry for table \"{}\"", table_name),
      common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
    }
    else {
      constituent_table_aliases.clear();
      constituent_table_aliases.insert(table_name);
    }
  }

  for (auto &entry : regular_table_alias_map_) {
    auto &table_alias = entry.first;
    if (constituent_table_aliases.count(table_alias) > 0) {
      auto &schema = std::get<2>(entry.second);
      auto col_cnt = schema.GetColumns().size();
      for (uint32_t i = 0; i < col_cnt; i++) {
        const auto &col_obj = schema.GetColumn(i);
        auto tv_expr = new parser::ColumnValueExpression(std::string(entry.first), std::string(col_obj.Name()));
        tv_expr->SetReturnValueType(col_obj.Type());
        tv_expr->DeriveExpressionName();
        tv_expr->SetDatabaseOID(std::get<0>(entry.second));
        tv_expr->SetTableOID(std::get<1>(entry.second));
        tv_expr->SetColumnOID(col_obj.Oid());
        tv_expr->SetDepth(depth_);

        auto unique_tv_expr =
            std::unique_ptr<parser::AbstractExpression>(reinterpret_cast<parser::AbstractExpression *>(tv_expr));
        parse_result->AddExpression(std::move(unique_tv_expr));
        auto new_tv_expr = common::ManagedPointer(parse_result->GetExpressions().back());
        exprs->push_back(new_tv_expr);
      }
    }
  }

  for (auto &entry : nested_table_alias_map_) {
    auto &table_alias = entry.first;
    if (constituent_table_aliases.count(table_alias) > 0) {
      auto &cols = entry.second;
      for (auto &col_entry : cols) {
        auto tv_expr = new parser::ColumnValueExpression(std::string(table_alias), std::string(col_entry.first.GetName()));
        tv_expr->SetReturnValueType(col_entry.second);
        tv_expr->DeriveExpressionName();
        tv_expr->SetAlias(col_entry.first);
        tv_expr->SetColumnOID(TEMP_OID(catalog::col_oid_t, col_entry.first.GetSerialNo()));
        tv_expr->SetDepth(depth_);

        auto unique_tv_expr =
            std::unique_ptr<parser::AbstractExpression>(reinterpret_cast<parser::AbstractExpression *>(tv_expr));
        parse_result->AddExpression(std::move(unique_tv_expr));
        auto new_tv_expr = common::ManagedPointer(parse_result->GetExpressions().back());
        // All derived columns do not have bound oids, thus keep them as INVALID_OIDs
        exprs->push_back(new_tv_expr);
      }
    }
  }
}

common::ManagedPointer<BinderContext::TableMetadata> BinderContext::GetTableMapping(const std::string &table_name) {
  if (regular_table_alias_map_.find(table_name) == regular_table_alias_map_.end()) {
    return nullptr;
  }
  return common::ManagedPointer(&regular_table_alias_map_[table_name]);
}

}  // namespace terrier::binder