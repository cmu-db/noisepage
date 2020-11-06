#include "parser/expression/column_value_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::parser {

std::unique_ptr<AbstractExpression> ColumnValueExpression::Copy() const {
  auto expr = std::make_unique<ColumnValueExpression>(GetDatabaseOid(), GetTableOid(), GetColumnOid());
  expr->SetMutableStateForCopy(*this);
  expr->table_name_ = this->table_name_;
  expr->column_name_ = this->column_name_;
  expr->SetDatabaseOID(this->database_oid_);
  expr->SetTableOID(this->table_oid_);
  expr->SetColumnOID(this->column_oid_);
  return expr;
}

common::hash_t ColumnValueExpression::Hash() const {
  common::hash_t hash = common::HashUtil::Hash(GetExpressionType());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetReturnValueType()));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_name_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_oid_));
  return hash;
}

bool ColumnValueExpression::operator==(const AbstractExpression &rhs) const {
  if (GetExpressionType() != rhs.GetExpressionType()) return false;
  if (GetReturnValueType() != rhs.GetReturnValueType()) return false;

  auto const &other = dynamic_cast<const ColumnValueExpression &>(rhs);
  if (GetColumnName() != other.GetColumnName()) return false;
  if (GetTableName() != other.GetTableName()) return false;
  if (GetColumnOid() != other.GetColumnOid()) return false;
  if (GetTableOid() != other.GetTableOid()) return false;
  return GetDatabaseOid() == other.GetDatabaseOid();
}

void ColumnValueExpression::DeriveExpressionName() {
  if (!(this->GetAlias().empty()))
    this->SetExpressionName(this->GetAlias());
  else
    this->SetExpressionName(column_name_);
}

void ColumnValueExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}

nlohmann::json ColumnValueExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();
  j["table_name"] = table_name_;
  j["column_name"] = column_name_;
  j["database_oid"] = database_oid_;
  j["table_oid"] = table_oid_;
  j["column_oid"] = column_oid_;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> ColumnValueExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto e1 = AbstractExpression::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  table_name_ = j.at("table_name").get<std::string>();
  column_name_ = j.at("column_name").get<std::string>();
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  column_oid_ = j.at("column_oid").get<catalog::col_oid_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ColumnValueExpression);

}  // namespace noisepage::parser
