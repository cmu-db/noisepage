#pragma once

#include <memory>
#include <string>
#include <utility>
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
      : ConstantValueExpression(type, std::make_unique<execution::sql::Val>(true), nullptr) {}

  /**
   * Construct a CVE of provided type and value
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   */
  ConstantValueExpression(type::TypeId type, std::unique_ptr<execution::sql::Val> value);

  /**
   * Construct a CVE of provided type and value
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   * @param buffer StringVal might not be inlined, so take ownership of that buffer as well
   */
  ConstantValueExpression(type::TypeId type, std::unique_ptr<execution::sql::Val> value, std::unique_ptr<byte> buffer);

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

  /** @return the constant value stored in this expression */
  common::ManagedPointer<execution::sql::Val> GetValue() const { return common::ManagedPointer(value_); }

  /**
   * Change the underlying value of this CVE. Used by the BinderSherpa to promote parameters
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   * @param buffer StringVal might not be inlined, so take ownership of that buffer as well
   */
  void SetValue(const type::TypeId type, std::unique_ptr<execution::sql::Val> value, std::unique_ptr<byte> buffer) {
    return_value_type_ = type;
    value_ = std::move(value);
    buffer_ = std::move(buffer);
    TERRIER_ASSERT(value_ != nullptr, "Didn't provide a value.");
  }
  /**
   * Change the underlying value of this CVE. Used by the BinderSherpa to promote parameters
   * @param type SQL type, apparently can be INVALID coming out of the parser for NULLs
   * @param value underlying value to take ownership of
   */
  void SetValue(const type::TypeId type, std::unique_ptr<execution::sql::Val> value) {
    SetValue(type, std::move(value), nullptr);
    TERRIER_ASSERT(value_->is_null_ || (type != type::TypeId::VARCHAR && type != type::TypeId::VARBINARY) ||
                       (buffer_ == nullptr && GetValue().CastManagedPointerTo<execution::sql::StringVal>()->len_ <=
                                                  execution::sql::StringVal::InlineThreshold()) ||
                       (buffer_ != nullptr && GetValue().CastManagedPointerTo<execution::sql::StringVal>()->len_ >
                                                  execution::sql::StringVal::InlineThreshold()),
                   "Value should either be NULL, a non-varlen type, or varlen and below the threshold with no owned "
                   "buffer, or varlen and above the threshold with a provided buffer.");
  }

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
  /** The constant held inside this ConstantValueExpression. */
  std::unique_ptr<execution::sql::Val> value_;
  std::unique_ptr<byte> buffer_;
};  // namespace terrier::parser

DEFINE_JSON_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
