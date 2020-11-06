#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace noisepage::parser {

/**
 * FunctionExpression represents a function invocation (except for CAST(), which is a TypeCastExpression).
 */
class FunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new function expression with the given name and children.
   * @param func_name function name
   * @param return_value_type function return value type
   * @param children children arguments for the function
   */
  FunctionExpression(std::string &&func_name, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::FUNCTION, return_value_type, std::move(children)),
        func_name_(std::move(func_name)) {}

  /** Default constructor for deserialization. */
  FunctionExpression() = default;

  /**
   * Instantiate a new function expression with the given name and children.
   * @param func_name function name
   * @param return_value_type function return value type
   * @param children children arguments for the function
   * @param proc_oid proc id
   */
  FunctionExpression(std::string &&func_name, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children, catalog::proc_oid_t proc_oid)
      : AbstractExpression(ExpressionType::FUNCTION, return_value_type, std::move(children)),
        func_name_(std::move(func_name)),
        proc_oid_(proc_oid) {}
  /**
   * Copies this FunctionExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;
  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override;

  common::hash_t Hash() const override;

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const FunctionExpression &>(rhs);
    return GetFuncName() == other.GetFuncName();
  }

  /** @return function name */
  const std::string &GetFuncName() const { return func_name_; }

  void DeriveExpressionName() override { SetExpressionName(GetFuncName()); }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

  /**
   * Sets the proc oid of this node
   * @param proc_oid proc oid to set this node to point to
   */
  void SetProcOid(catalog::proc_oid_t proc_oid) { proc_oid_ = proc_oid; }

  /**
   * Gets the bound proc_oid of the function
   * @return proc_oid of the function bound to this expression
   */
  catalog::proc_oid_t GetProcOid() const { return proc_oid_; }

 private:
  /** Name of function to be invoked. */
  std::string func_name_;

  // To quote Tianyu, "Parsers are dumb. They don't know shit."
  // We should keep it that way, resist adding codegen hacks here.

  catalog::proc_oid_t proc_oid_;
};

DEFINE_JSON_HEADER_DECLARATIONS(FunctionExpression);

}  // namespace noisepage::parser
