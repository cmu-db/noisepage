#include <string>
#include "binder/binder_context.h"
#include "common/exception.h"

#include "catalog/catalog_accessor.h"
#include "parser/expression/column_value_expression.h"
#include "parser/table_ref.h"

namespace terrier::binder {

void BinderContext::AddRegularTable(catalog::CatalogAccessor *accessor, parser::TableRef *table_ref) {

  // TODO: pass the catalog accessor as a parameter, probably remove txn and db_name
  //  as catalog accessor already have transaction and db_oid in the object

  // TODO: if we are going to substitute the schema_name with namespace_name and schema_id with namespace_id
  //  then, what is the schema name in table_ref??? should it be namespace also?
  AddRegularTable(accessor, table_ref->GetDatabaseName(), table_ref->GetTableName(), table_ref->GetAlias());
}

void BinderContext::AddRegularTable(catalog::CatalogAccessor *accessor, const std::string &db_name,
                                    const std::string &table_name, const std::string &table_alias) {

  // TODO: pass the catalog accessor as a parameter, probably remove txn and db_name
  //  as catalog accessor already have transaction and db_oid in the object
  //  But does the db_oid in the accessor same as that stored in Statements?
  auto db_id = accessor->GetDatabaseOid(db_name);
  auto table_id = accessor->GetTableOid(table_name);

  auto schema = accessor->GetSchema(table_id);

  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw BINDER_EXCEPTION(("Duplicate alias " + table_alias).c_str());
  }
  regular_table_alias_map_[table_alias] = std::make_tuple(db_id, table_id, schema);
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


bool BinderContext::ColumnInSchema(catalog::Schema &schema, std::string &col_name) {
  try{
    auto column_object = schema.GetColumn(col_name);
  } catch (const Exception &e) {
    return false;
  }
  return true;
}

void BinderContext::GetColumnPosTuple(const std::string &col_name, std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> tuple, parser::ColumnValueExpression *expr) {

  auto column_object = std::get<2>(tuple).GetColumn(col_name);
  expr->SetDatabaseOID(std::get<0>(tuple));
  expr->SetTableOID(std::get<1>(tuple));
  expr->SetColumnOID(column_object.GetOid());
  expr->SetColumnName(col_name);
  expr->SetReturnValueType(column_object.GetType());
}

bool BinderContext::GetColumnPosTuple(std::shared_ptr<BinderContext> current_context, parser::ColumnValueExpression *expr) {

  auto col_name = expr->GetColumnName();
  std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);

  bool find_matched = false;
  while (current_context != nullptr) {
    // Check regular table
    for (auto &entry : current_context->regular_table_alias_map_) {
      bool get_matched = ColumnInSchema(std::get<2>(entry.second), col_name);
      if (get_matched) {
        if (!find_matched) {
          // First match
          find_matched = true;
          GetColumnPosTuple(col_name, entry.second, expr);
          expr->SetTableName(entry.first);
        } else {
          throw BINDER_EXCEPTION(("Ambiguous column name " + col_name).c_str());
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
          throw BINDER_EXCEPTION(("Ambiguous column name " + col_name).c_str());
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

bool BinderContext::GetRegularTableObj(std::shared_ptr<BinderContext> current_context, std::string &alias,
                                       parser::ColumnValueExpression *expr, std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> &tuple) {
  while (current_context != nullptr) {
    auto iter = current_context->regular_table_alias_map_.find(alias);
    if (iter != current_context->regular_table_alias_map_.end()) {
      tuple = iter->second;
      expr->SetDepth(current_context->depth_);
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::CheckNestedTableColumn(
    std::shared_ptr<BinderContext> current_context, std::string &alias,
    std::string &col_name, parser::ColumnValueExpression *expr) {
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(alias);
    if (iter != current_context->nested_table_alias_map_.end()) {
      auto col_iter = iter->second.find(col_name);
      if (col_iter == iter->second.end()) {
        throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
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

void BinderContext::GenerateAllColumnExpressions(std::vector<std::shared_ptr<parser::AbstractExpression>> &exprs) {
  for (auto &entry : regular_table_alias_map_) {
    auto &schema = std::get<2>(entry.second);
    auto col_cnt = schema.GetColumns().size();
    for (uint32_t i = 0; i < col_cnt; i++) {
      auto col_obj = schema.GetColumn(i);
      // TODO (Ling): change use of shared_ptr
      auto tv_expr = std::make_shared<parser::ColumnValueExpression>(std::string(entry.first), std::string(col_obj.GetName()));
      tv_expr->SetReturnValueType(col_obj.GetType());
      tv_expr->DeduceExpressionName();
      tv_expr->SetDatabaseOID(std::get<0>(entry.second));
      tv_expr->SetTableOID(std::get<1>(entry.second));
      tv_expr->SetColumnOID(col_obj.GetOid());

      exprs.emplace_back(tv_expr);
    }
  }

  for (auto &entry : nested_table_alias_map_) {
    auto &table_alias = entry.first;
    auto &cols = entry.second;
    for (auto &col_entry : cols) {
      auto tv_expr = std::make_shared<parser::ColumnValueExpression>(std::string(table_alias), std::string(col_entry.first));
      tv_expr->SetReturnValueType(col_entry.second);
      tv_expr->DeduceExpressionName();
      // All derived columns do not have bound oids, thus keep them as INVALID_OIDs
      exprs.emplace_back(tv_expr);
    }
  }
}

}  // namespace terrier::binder
