#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a logical tuple value.
 */
class TupleValueExpression : public AbstractExpression {
 public:
  // TODO(WAN): I feel like this should be renamed. Maybe parameters reordered too.
  /**
   * @param col_name column name
   * @param table_name table name
   */
  TupleValueExpression(std::string col_name, std::string table_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        col_name_(std::move(col_name)),
        table_name_(std::move(table_name)) {}

  /**
   * Default constructor for deserialization
   */
  TupleValueExpression() = default;

  ~TupleValueExpression() override = default;

  /**
   * @return column name
   */
  std::string GetColumnName() const { return col_name_; }

  /**
   * @return table name
   */
  std::string GetTableName() const { return table_name_; }

  const AbstractExpression *Copy() const override { return new TupleValueExpression(col_name_, table_name_); }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   */
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    TERRIER_ASSERT(children.empty(), "TupleValueExpression should have 0 children");
    return new TupleValueExpression(col_name_, table_name_);
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const TupleValueExpression &>(rhs);
    return GetColumnName() == other.GetColumnName() && GetTableName() == other.GetTableName();
  }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["col_name"] = col_name_;
    j["table_name"] = table_name_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    col_name_ = j.at("col_name").get<std::string>();
    table_name_ = j.at("table_name").get<std::string>();
  }


  const std::tuple<catalog::db_oid_t,
                   catalog::table_oid_t,
                   catalog::col_oid_t> &GetBoundOid();

 private:
  std::string col_name_;
  std::string table_name_;
};

DEFINE_JSON_DECLARATIONS(TupleValueExpression);

}  // namespace terrier::parser
