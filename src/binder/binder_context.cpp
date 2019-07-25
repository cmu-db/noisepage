#pragma once

#include <string>
#include "binder/binder_context.h"
#include "common/exception.h"

#include "catalog/catalog_accessor.h"
#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "parser/expression/column_value_expression.h"
#include "parser/table_ref.h"
#include "storage/storage_manager.h"

namespace terrier::binder {

void BinderContext::AddRegularTable(transaction::TransactionContext *txn, std::shared_ptr<parser::TableRef> table_ref, const std::string default_database_name) {

  // TODO: pass the catalog accessor as a parameter, probably remove txn and db_name
  //  as catalog accessor already have transaction and db_oid in the object

  // TODO: if we are going to substitute the schema_name with namespace_name and schema_id with namespace_id
  //  then, what is the schema name in table_ref??? should it be namespace also?
  AddRegularTable(txn, table_ref->GetDatabaseName(), table_ref->GetSchemaName(), table_ref->GetTableName(), table_ref->GetAlias());
}

void BinderContext::AddRegularTable(transaction::TransactionContext *txn, const std::string db_name, const std::string namespace_name,
                                    const std::string table_name, const std::string table_alias) {

  // TODO: pass the catalog accessor as a parameter, probably remove txn and db_name
  //  as catalog accessor already have transaction and db_oid in the object
  //  But does the db_oid in the accessor same as that stored in Statements?
  auto ns_id = catalog_accessor_.GetNamespaceOid(namespace_name);
  auto table_id = catalog_accessor_.GetTableOid(ns_id, table_name);

  auto schema = catalog_accessor_.GetSchema(table_id);

  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(("Duplicate alias " + table_alias).c_str());
  }
  regular_table_alias_map_[table_alias] = schema;
}

void BinderContext::AddNestedTable(const std::string &table_alias, const std::vector<std::shared_ptr<parser::AbstractExpression>> &select_list) {
  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(("Duplicate alias " + table_alias).c_str());
  }

  std::unordered_map<std::string, type::TypeId> column_alias_map;
  for (auto &expr : select_list) {
    std::string alias;
    if (!expr->GetAlias().empty()) {
      alias = expr->GetAlias();
    }
    else if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_TUPLE) {
      auto tv_expr = reinterpret_cast<parser::ColumnValueExpression *>(expr.get());
      alias = tv_expr->GetColumnName();
    }
    else continue;

    std::transform(alias.begin(), alias.end(), alias.begin(), ::tolower);
    column_alias_map[alias] = expr->GetReturnValueType();
  }
  nested_table_alias_map_[table_alias] = column_alias_map;
}

bool BinderContext::GetColumnPosTuple(const std::string &col_name, std::shared_ptr<catalog::Schema> &schema,
    std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::col_oid_t> &col_pos_tuple, type::TypeId &value_type) {

  catalog::Schema::Column column_object;
  try{
    column_object = schema->GetColumn(col_name);
  } catch (const Exception &e) {
    return false;
  }

  auto col_pos = column_object.GetOid();
  col_pos_tuple = std::make_tuple(schema->GetDatabaseOid(), table_obj->GetTableOid(), col_pos);
  value_type = column_object.GetType();
  return true;
}

bool BinderContext::GetColumnPosTuple(
    std::shared_ptr<BinderContext> current_context, const std::string &col_name,
    std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::col_oid_t> &col_pos_tuple, std::string &table_alias,
    type::TypeId &value_type, int &depth) {
  bool find_matched = false;
  while (current_context != nullptr) {
    // Check regular table
    for (auto entry : current_context->regular_table_alias_map_) {
      bool get_matched = GetColumnPosTuple(col_name, entry.second, col_pos_tuple, value_type);
      if (get_matched) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
        } else {
          throw BINDER_EXCEPTION(("Ambiguous column name " + col_name).c_str());
        }
      }
    }
    // Check nested table
    for (auto entry : current_context->nested_table_alias_map_) {
      bool get_match = entry.second.find(col_name) != entry.second.end();
      if (get_match) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
          value_type = entry.second[col_name];
        } else {
          throw BINDER_EXCEPTION(("Ambiguous column name " + col_name).c_str());
        }
      }
    }
    if (find_matched) {
      depth = current_context->depth_;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::GetRegularTableObj(std::shared_ptr<BinderContext> current_context, std::string &alias,
    std::shared_ptr<catalog::Schema> &schema, int &depth) {
  while (current_context != nullptr) {
    auto iter = current_context->regular_table_alias_map_.find(alias);
    if (iter != current_context->regular_table_alias_map_.end()) {
      schema = iter->second;
      // TODO: again, is depth discarded
      depth = current_context->depth_;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::CheckNestedTableColumn(
    std::shared_ptr<BinderContext> current_context, std::string &alias,
    std::string &col_name, type::TypeId &value_type, int &depth) {
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(alias);
    if (iter != current_context->nested_table_alias_map_.end()) {
      auto col_iter = iter->second.find(col_name);
      if (col_iter == iter->second.end()) {
        throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
      }
      value_type = col_iter->second;
      depth = current_context->depth_;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

void BinderContext::GenerateAllColumnExpressions(const std::vector<std::shared_ptr<parser::AbstractExpression>> &exprs) {
  for (auto &entry : regular_table_alias_map_) {
    auto &schema = entry.second;
    auto col_cnt = schema->GetColumns().size();
    for (uint32_t i = 0; i < col_cnt; i++) {
      auto col_obj = schema->GetColumn(i);
      auto tv_expr = new parser::TupleValueExpression(col_obj.GetType(), std::string(col_obj.GetName()), std::string(entry.first));
      // TODO:
      //  anywhere with set on expression, since the expression is immutable, we will need to
      //  either copy expression first, or having a suitable constructor in the expression to construct a fresh one
      //  **Add Copy Constructor in expressions**
      //tv_expr->SetValueType(col_obj->GetColumnType());

      // TODO: we haven't figured out expression name
      //tv_expr->DeduceExpressionName();

      // TODO: there is no Bound... what was the bound in the first place?
      //tv_expr->SetBoundOid(table_obj->GetDatabaseOid(), table_obj->GetTableOid(), col_obj->GetColumnId());
      exprs.emplace_back(tv_expr);
    }
  }

  for (auto &entry : nested_table_alias_map_) {
    auto &table_alias = entry.first;
    auto &cols = entry.second;
    for (auto &col_entry : cols) {
      auto tv_expr = new parser::TupleValueExpression(col_entry.second, std::string(col_entry.first), std::string(table_alias));
      //tv_expr->SetValueType(col_entry.second);
      //tv_expr->DeduceExpressionName();
      // All derived columns do not have bound column id. We need to set them to
      // all zero to get rid of garbage value and make comparison work
      //tv_expr->SetBoundOid(0, 0, 0);
      exprs.emplace_back(tv_expr);
    }
  }
}

}  // namespace terrier::binder
