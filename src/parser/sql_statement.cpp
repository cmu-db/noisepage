#include "parser/sql_statement.h"

#include "common/json.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

nlohmann::json TableInfo::ToJson() const {
  nlohmann::json j;
  j["table_name"] = table_name_;
  j["namespace_name"] = namespace_name_;
  j["database_name"] = database_name_;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> TableInfo::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  table_name_ = j.at("table_name").get<std::string>();
  namespace_name_ = j.at("namespace_name").get<std::string>();
  database_name_ = j.at("database_name").get<std::string>();
  return exprs;
}

nlohmann::json SQLStatement::ToJson() const {
  nlohmann::json j;
  j["stmt_type"] = stmt_type_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> SQLStatement::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  stmt_type_ = j.at("stmt_type").get<StatementType>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(SQLStatement);

}  // namespace terrier::parser
