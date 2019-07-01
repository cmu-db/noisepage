#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents a column tuple value.
 */
class ColumnValueExpression : public AbstractExpression {
 public:
  /**
   * This constructor is called only in postgresparser, setting the column name,
   * and optionally setting the table name and alias.
   * Namespace name is always set to empty string, as the postgresparser does not know the namespace name.
   * Parameter namespace name is included so that the program can differentiate this constructor from
   * another constructor that sets the namespace name, table name. and column name.
   * @param namespace_name namespace name
   * @param table_name table name
   * @param col_name column name
   * @param alias alias of the expression
   */
  ColumnValueExpression(std::string namespace_name, std::string table_name, std::string col_name, const char *alias)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, alias, {}),
        column_name_(std::move(col_name)),
        table_name_(std::move(table_name)),
        namespace_name_(std::move(namespace_name)) {}

  /**
   * @param table_name table name
   * @param col_name column name
   */
  ColumnValueExpression(std::string table_name, std::string col_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        column_name_(std::move(col_name)),
        table_name_(std::move(table_name)) {}

  /**
   * @param namespace_name namespace name
   * @param table_name table name
   * @param col_name column name
   */
  ColumnValueExpression(std::string namespace_name, std::string table_name, std::string col_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        column_name_(std::move(col_name)),
        table_name_(std::move(table_name)),
        namespace_name_(std::move(namespace_name)) {}

  /**
   * @param table_oid table OID
   * @param column_oid column OID
   */
  ColumnValueExpression(catalog::table_oid_t table_oid, catalog::col_oid_t column_oid)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        column_oid_(column_oid),
        table_oid_(table_oid) {}
  /**
   * Default constructor for deserialization
   */
  ColumnValueExpression() = default;

  /**
   * @return column name
   */
  std::string GetColumnName() const { return column_name_; }

  /**
   * @return table name
   */
  std::string GetTableName() const { return table_name_; }

  /**
   * @return namespace name
   */
  std::string GetNamespaceName() const { return namespace_name_; }

  /**
   * @return column oid
   */
  catalog::col_oid_t GetColumnOid() const { return column_oid_; }

  /**
   * @return table oid
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }
  
  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<ColumnValueExpression>(*this); }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_oid_));
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const ColumnValueExpression &>(rhs);
    if (GetColumnName() != other.GetColumnName() || GetTableName() != other.GetTableName() || GetNamespaceName() != other.GetNamespaceName()) return false;
    return  GetColumnOid() == other.GetColumnOid() && GetTableOid() == other.GetTableOid();
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["column_name"] = column_name_;
    j["table_name"] = table_name_;
    j["namespace_name"] = namespace_name_;
    j["column_oid"] = column_oid_;
    j["table_oid"] = table_oid_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    column_name_ = j.at("column_name").get<std::string>();
    table_name_ = j.at("table_name").get<std::string>();
    namespace_name_ = j.at("namespace_name").get<std::string>();
    column_oid_ = j.at("column_oid").get<catalog::col_oid_t>();
    table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  }

 private:
  void DeduceExpressionName() override {
    if (!this->GetAlias().empty()) return;
    this->SetExpressionName(column_name_);
  }

  std::string column_name_;
  std::string table_name_;
  std::string namespace_name_;
  catalog::col_oid_t column_oid_;
  catalog::table_oid_t table_oid_;
};

DEFINE_JSON_DECLARATIONS(ColumnValueExpression);

}  // namespace terrier::parser
