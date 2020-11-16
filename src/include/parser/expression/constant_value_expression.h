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

namespace noisepage::binder {
class BindNodeVisitor;
}

namespace noisepage::parser {

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
   * @tparam T execution value type to copy from
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to copy
   */
  template <typename T>
  ConstantValueExpression(type::TypeId type, T value);

  /**
   * Construct a CVE of provided type and value
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to copy
   * @param buffer StringVal might not be inlined, so take ownership of that buffer
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
    NOISEPAGE_ASSERT(children.empty(), "ConstantValueExpression should have 0 children");
    return Copy();
  }

  void DeriveExpressionName() override {
    if (!this->GetAlias().empty()) {
      this->SetExpressionName(this->GetAlias());
    }
  }

  /**
   * @return copy of the underlying Val
   */
  execution::sql::BoolVal GetBoolVal() const {
    NOISEPAGE_ASSERT(std::holds_alternative<execution::sql::BoolVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::BoolVal>(value_);
  }

  /**
   * @return copy of the underlying Val
   */
  execution::sql::Integer GetInteger() const {
    NOISEPAGE_ASSERT(std::holds_alternative<execution::sql::Integer>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::Integer>(value_);
  }

  /**
   * @return copy of the underlying Val
   */
  execution::sql::Real GetReal() const {
    NOISEPAGE_ASSERT(std::holds_alternative<execution::sql::Real>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::Real>(value_);
  }

  /**
   * @return copy of the underlying Val
   */
  execution::sql::DateVal GetDateVal() const {
    NOISEPAGE_ASSERT(std::holds_alternative<execution::sql::DateVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::DateVal>(value_);
  }

  /**
   * @return copy of the underlying Val
   */
  execution::sql::TimestampVal GetTimestampVal() const {
    NOISEPAGE_ASSERT(std::holds_alternative<execution::sql::TimestampVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::TimestampVal>(value_);
  }

  /**
   * @return copy of the underlying Val
   * @warning StringVal may not have inlined its value, in which case the StringVal returned by this function will hold
   * a pointer to the buffer in this CVE. In that case, do not destroy this CVE before the copied StringVal
   */
  execution::sql::StringVal GetStringVal() const {
    NOISEPAGE_ASSERT(std::holds_alternative<execution::sql::StringVal>(value_), "Invalid variant type for Get.");
    return std::get<execution::sql::StringVal>(value_);
  }

  /**
   * Change the underlying value of this CVE. Used by the BinderSherpa to promote parameters
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to copy
   * @param buffer StringVal might not be inlined, so take ownership of that buffer
   */
  void SetValue(const type::TypeId type, const execution::sql::StringVal value, std::unique_ptr<byte[]> buffer) {
    return_value_type_ = type;
    value_ = value;
    buffer_ = std::move(buffer);
    Validate();
  }

  /**
   * Change the underlying value of this CVE. Used by the BinderSherpa to promote parameters
   * @tparam T execution value type to copy from
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to copy
   */
  template <typename T>
  void SetValue(type::TypeId type, T value) {
    return_value_type_ = type;
    value_ = value;
    buffer_ = nullptr;
    Validate();
  }

  /**
   * @return true if CVE value represents a NULL
   */
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

  /**
   * Extracts the underlying execution value as a C++ type
   * @tparam T C++ type to extract
   * @return copy of the underlying value as the requested type
   * @warning std::string_view returned by this function will hold a pointer to the buffer in this CVE. In that case, do
   * not destroy this CVE before the std::string_view
   */
  template <typename T>
  T Peek() const;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /** @return A string representation of this ConstantValueExpression. */
  std::string ToString() const;

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
               execution::sql::DecimalVal, execution::sql::StringVal, execution::sql::DateVal,
               execution::sql::TimestampVal>
      value_{execution::sql::Val(true)};
  std::unique_ptr<byte[]> buffer_ = nullptr;
};

DEFINE_JSON_HEADER_DECLARATIONS(ConstantValueExpression);

/// @cond DOXYGEN_IGNORE
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::Val value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::BoolVal value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::Integer value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::Real value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::DecimalVal value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::StringVal value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::DateVal value);
extern template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                                 const execution::sql::TimestampVal value);

extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::Val value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::BoolVal value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::Integer value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::Real value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::DecimalVal value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::StringVal value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::DateVal value);
extern template void ConstantValueExpression::SetValue(const type::TypeId type,
                                                       const execution::sql::TimestampVal value);

extern template bool ConstantValueExpression::Peek() const;
extern template int8_t ConstantValueExpression::Peek() const;
extern template int16_t ConstantValueExpression::Peek() const;
extern template int32_t ConstantValueExpression::Peek() const;
extern template int64_t ConstantValueExpression::Peek() const;
extern template float ConstantValueExpression::Peek() const;
extern template double ConstantValueExpression::Peek() const;
extern template execution::sql::Date ConstantValueExpression::Peek() const;
extern template execution::sql::Timestamp ConstantValueExpression::Peek() const;
extern template std::string_view ConstantValueExpression::Peek() const;
/// @endcond

}  // namespace noisepage::parser
