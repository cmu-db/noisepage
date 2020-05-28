#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/value.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_util.h"

namespace terrier::binder {
class BindNodeVisitor;
}

namespace terrier::parser {

/**
 * ConstantValueExpression represents a constant, e.g. numbers, string literals.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  /**
   * Construct a NULL CVE of provided type
   * @param type SQL type for NULL, apparently can be INVALID coming out of the parser for NULLs
   */
  explicit ConstantValueExpression(const type::TypeId type)
      : ConstantValueExpression(type, execution::sql::Val(true)) {}

  /**
   * Construct a CVE of provided type and value
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   */
  template <typename T>
  ConstantValueExpression(type::TypeId type, T value);

  /**
   * Construct a CVE of provided type and value
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   * @param buffer StringVal might not be inlined, so take ownership of that buffer as well
   */
  ConstantValueExpression(type::TypeId type, execution::sql::StringVal value, std::unique_ptr<byte[]> buffer);

  /** Default constructor for deserialization. */
  ConstantValueExpression() = default;

  /**
   * Copy assignment operator
   * @param other CVE to copy
   * @return self-reference
   */
  ConstantValueExpression &operator=(const ConstantValueExpression &other);

  /**
   * Move assignment operator
   * @param other CVE to move
   * @return self-reference
   */
  ConstantValueExpression &operator=(ConstantValueExpression &&other) noexcept;

  /**
   * Move constructor
   * @param other CVE to move
   */
  ConstantValueExpression(ConstantValueExpression &&other) noexcept;

  /**
   * Copy constructor
   * @param other CVE to copy
   */
  ConstantValueExpression(const ConstantValueExpression &other);

  common::hash_t Hash() const override;

  bool operator==(const AbstractExpression &other) const override;

  /**
   * Copies this ConstantValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    return std::unique_ptr<AbstractExpression>{std::make_unique<ConstantValueExpression>(*this)};
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    TERRIER_ASSERT(children.empty(), "ConstantValueExpression should have 0 children");
    return Copy();
  }

  void DeriveExpressionName() override {
    if (!this->GetAlias().empty()) {
      this->SetExpressionName(this->GetAlias());
    }
  }

  execution::sql::BoolVal GetBoolVal() const {
    TERRIER_ASSERT(std::holds_alternative<execution::sql::BoolVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::BoolVal>(value_);
  }

  execution::sql::Integer GetInteger() const {
    TERRIER_ASSERT(std::holds_alternative<execution::sql::Integer>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::Integer>(value_);
  }

  execution::sql::Real GetReal() const {
    TERRIER_ASSERT(std::holds_alternative<execution::sql::Real>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::Real>(value_);
  }

  execution::sql::DateVal GetDateVal() const {
    TERRIER_ASSERT(std::holds_alternative<execution::sql::DateVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::DateVal>(value_);
  }

  execution::sql::TimestampVal GetTimestampVal() const {
    TERRIER_ASSERT(std::holds_alternative<execution::sql::TimestampVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::TimestampVal>(value_);
  }

  execution::sql::StringVal GetStringVal() const {
    TERRIER_ASSERT(std::holds_alternative<execution::sql::StringVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::StringVal>(value_);
  }

  /**
   * Change the underlying value of this CVE. Used by the BinderSherpa to promote parameters
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   * @param buffer StringVal might not be inlined, so take ownership of that buffer as well
   */
  void SetValue(const type::TypeId type, const execution::sql::Val value, std::unique_ptr<byte[]> buffer) {
    return_value_type_ = type;
    value_ = value;
    buffer_ = std::move(buffer);
    Validate();
  }
  /**
   * Change the underlying value of this CVE. Used by the BinderSherpa to promote parameters
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   */
  void SetValue(const type::TypeId type, const execution::sql::Val value) { SetValue(type, value, nullptr); }

  bool IsNull() const {
    if (std::holds_alternative<execution::sql::Val>(value_) && std::get<execution::sql::Val>(value_).is_null_)
      return true;
    switch (return_value_type_) {
      case type::TypeId::BOOLEAN: {
        return GetBoolVal().is_null_;
      }
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT: {
        return GetInteger().is_null_;
      }
      case type::TypeId::DECIMAL: {
        return GetReal().is_null_;
      }
      case type::TypeId::TIMESTAMP: {
        return GetTimestampVal().is_null_;
      }
      case type::TypeId::DATE: {
        return GetDateVal().is_null_;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        return GetStringVal().is_null_;
      }
      default:
        UNREACHABLE("Invalid TypeId.");
    }
  }

  template <typename T>
  T Peek() const;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  friend class binder::BindNodeVisitor; /* value_ may be modified, e.g., when parsing dates. */
  void Validate() const;
  std::variant<execution::sql::Val, execution::sql::BoolVal, execution::sql::Integer, execution::sql::Real,
               execution::sql::Decimal, execution::sql::StringVal, execution::sql::DateVal,
               execution::sql::TimestampVal>
      value_;
  std::unique_ptr<byte[]> buffer_ = nullptr;
};  // namespace terrier::parser

DEFINE_JSON_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
