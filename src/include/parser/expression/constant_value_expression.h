#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/value.h"
#include "parser/expression/abstract_expression.h"
#include "util/time_util.h"

namespace terrier::binder {
class BindNodeVisitor;
}

namespace terrier::parser {
/**
 * ConstantValueExpression represents a constant, e.g. numbers, string literals.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  explicit ConstantValueExpression(const type::TypeId type)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, type, {}),
        value_(std::make_unique<execution::sql::Val>(true)) {}

  ConstantValueExpression(const type::TypeId type, std::unique_ptr<execution::sql::Val> value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, type, {}), value_(std::move(value)) {
    TERRIER_ASSERT(
        type != type::TypeId::VARCHAR && type != type::TypeId::VARBINARY,
        "Constructor can't handle taking ownership of non-inlined varlens so it can't be used with StringVals.");
  }

  ConstantValueExpression(const type::TypeId type, std::unique_ptr<execution::sql::Val> value, byte *const buffer)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, type, {}), value_(std::move(value)), buffer_(buffer) {
    TERRIER_ASSERT(type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY,
                   "Constructor is just for potentially taking ownership of non-lined varlens.");
    TERRIER_ASSERT(value_->is_null_ ||
                       (buffer_ == nullptr && GetValue().CastManagedPointerTo<execution::sql::StringVal>()->len_ <=
                                                  execution::sql::StringVal::InlineThreshold()) ||
                       (buffer_ != nullptr && GetValue().CastManagedPointerTo<execution::sql::StringVal>()->len_ >
                                                  execution::sql::StringVal::InlineThreshold()),
                   "Value should either be NULL, below the threshold with no owned buffer, or above the threshold with "
                   "a provided buffer.");
  }

  /** Default constructor for deserialization. */
  ConstantValueExpression() = default;

  ~ConstantValueExpression() override { delete[] buffer_; }

  ConstantValueExpression(const ConstantValueExpression &other) : AbstractExpression(other) {
    if (other.value_->is_null_) {
      value_ = std::make_unique<execution::sql::Val>(true);
    } else {
      switch (other.GetReturnValueType()) {
        case type::TypeId::BOOLEAN: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
          value_ = std::make_unique<execution::sql::BoolVal>(val);
          break;
        }
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
          value_ = std::make_unique<execution::sql::Integer>(val);
          break;
        }
        case type::TypeId::DECIMAL: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
          value_ = std::make_unique<execution::sql::Real>(val);
          break;
        }
        case type::TypeId::TIMESTAMP: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_;
          value_ = std::make_unique<execution::sql::TimestampVal>(val);
          break;
        }
        case type::TypeId::DATE: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_;
          value_ = std::make_unique<execution::sql::DateVal>(val);
          break;
        }
        case type::TypeId::VARCHAR:
        case type::TypeId::VARBINARY: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::StringVal>();
          // Inlined
          if (val->len_ <= execution::sql::StringVal::InlineThreshold()) {
            value_ = std::make_unique<execution::sql::StringVal>(val->Content(), val->len_);
            break;
          }
          // TODO(Matt): smarter allocation?
          buffer_ = common::AllocationUtil::AllocateAligned(val->len_);
          std::memcpy(buffer_, val->Content(), val->len_);
          value_ = std::make_unique<execution::sql::StringVal>(reinterpret_cast<const char *>(buffer_), val->len_);
          break;
        }
        default:
          UNREACHABLE("Invalid TypeId.");
      }
    }
    this->SetMutableStateForCopy(other);
  }

  // FIXME(Matt): hashing stuff
  //  common::hash_t Hash() const override {
  //    return common::HashUtil::CombineHashes(AbstractExpression::Hash(), value_.Hash());
  //  }

  bool operator==(const AbstractExpression &other) const override {
    // FIXME(Matt): equality stuff
    if (!AbstractExpression::operator==(other)) return false;
    const auto &const_expr = dynamic_cast<const ConstantValueExpression &>(other);
    return value_ == const_expr.value_;
  }

  /**
   * Copies this ConstantValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    std::unique_ptr<ConstantValueExpression> expr = nullptr;

    if (value_->is_null_) {
      if (return_value_type_ != type::TypeId::VARCHAR && return_value_type_ != type::TypeId::VARBINARY) {
        expr =
            std::make_unique<ConstantValueExpression>(return_value_type_, std::make_unique<execution::sql::Val>(true));
      } else {
        expr = std::make_unique<ConstantValueExpression>(return_value_type_,
                                                         std::make_unique<execution::sql::Val>(true), nullptr);
      }
    } else {
      switch (return_value_type_) {
        case type::TypeId::BOOLEAN: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
          expr = std::make_unique<ConstantValueExpression>(return_value_type_,
                                                           std::make_unique<execution::sql::BoolVal>(val));
          break;
        }
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
          expr = std::make_unique<ConstantValueExpression>(return_value_type_,
                                                           std::make_unique<execution::sql::Integer>(val));
          break;
        }
        case type::TypeId::DECIMAL: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
          expr = std::make_unique<ConstantValueExpression>(return_value_type_,
                                                           std::make_unique<execution::sql::Real>(val));
          break;
        }
        case type::TypeId::TIMESTAMP: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_;
          expr = std::make_unique<ConstantValueExpression>(return_value_type_,
                                                           std::make_unique<execution::sql::TimestampVal>(val));
          break;
        }
        case type::TypeId::DATE: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_;
          expr = std::make_unique<ConstantValueExpression>(return_value_type_,
                                                           std::make_unique<execution::sql::DateVal>(val));
          break;
        }
        case type::TypeId::VARCHAR:
        case type::TypeId::VARBINARY: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::StringVal>();
          // Inlined
          if (val->len_ <= execution::sql::StringVal::InlineThreshold()) {
            expr = std::make_unique<ConstantValueExpression>(
                return_value_type_, std::make_unique<execution::sql::StringVal>(val->Content(), val->len_), nullptr);
            break;
          }
          // TODO(Matt): smarter allocation?
          auto *const buffer = common::AllocationUtil::AllocateAligned(val->len_);
          std::memcpy(buffer, val->Content(), val->len_);
          expr = std::make_unique<ConstantValueExpression>(
              return_value_type_,
              std::make_unique<execution::sql::StringVal>(reinterpret_cast<const char *>(buffer), val->len_), buffer);
          break;
        }
        default:
          UNREACHABLE("Invalid TypeId.");
      }
    }

    expr->SetMutableStateForCopy(*this);
    return expr;
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
    } else {
      // FIXME(Matt): this stuff
      this->SetExpressionName("");
    }
  }

  /** @return the constant value stored in this expression */
  common::ManagedPointer<execution::sql::Val> GetValue() const { return common::ManagedPointer(value_); }

  void SetValue(const type::TypeId type, std::unique_ptr<execution::sql::Val> value) {
    TERRIER_ASSERT(
        type != type::TypeId::VARCHAR && type != type::TypeId::VARBINARY,
        "SetValue can't handle taking ownership of non-inlined varlens so it can't be used with StringVals.");
    return_value_type_ = type;
    value_ = std::move(value);
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /**
   * @return expression serialized to json
   * @note TransientValue::ToJson() is private, ConstantValueExpression is a friend
   * @see TransientValue for why TransientValue::ToJson is private
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();

    if (!value_->is_null_) {
      switch (return_value_type_) {
        case type::TypeId::BOOLEAN: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
          j["value"] = val;
          break;
        }
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
          j["value"] = val;
          break;
        }
        case type::TypeId::DECIMAL: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
          j["value"] = val;
          break;
        }
        case type::TypeId::TIMESTAMP: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_.ToNative();
          j["value"] = val;
          break;
        }
        case type::TypeId::DATE: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_.ToNative();
          j["value"] = val;
          break;
        }
        case type::TypeId::VARCHAR:
        case type::TypeId::VARBINARY: {
          const auto val = GetValue().CastManagedPointerTo<execution::sql::StringVal>()->StringView();
          j["value"] = val;
          break;
        }
        default:
          UNREACHABLE("Invalid TypeId.");
      }
    }
    return j;
  }

  /**
   * @param j json to deserialize
   * @note TransientValue::FromJson() is private, ConstantValueExpression is a friend
   * @see TransientValue for why TransientValue::FromJson is private
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));

    if (j.find("value") != j.end()) {
      // it's not NULL
      switch (return_value_type_) {
        case type::TypeId::BOOLEAN: {
          value_ = std::make_unique<execution::sql::BoolVal>(j.at("value").get<bool>());
          break;
        }
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT: {
          value_ = std::make_unique<execution::sql::Integer>(j.at("value").get<int64_t>());
          break;
        }
        case type::TypeId::DECIMAL: {
          value_ = std::make_unique<execution::sql::Real>(j.at("value").get<double>());
          break;
        }
        case type::TypeId::TIMESTAMP: {
          value_ = std::make_unique<execution::sql::TimestampVal>(
              execution::sql::Timestamp::FromNative(j.at("value").get<uint64_t>()));
          break;
        }
        case type::TypeId::DATE: {
          value_ = std::make_unique<execution::sql::DateVal>(
              execution::sql::Date::FromNative(j.at("value").get<uint32_t>()));
          break;
        }
        case type::TypeId::VARCHAR:
        case type::TypeId::VARBINARY: {
          const auto string_val = j.at("value").get<std::string>();
          if (string_val.length() <= execution::sql::StringVal::InlineThreshold()) {
            value_ = std::make_unique<execution::sql::StringVal>(string_val.c_str(), string_val.length());
          } else {
            // TODO(Matt): smarter allocation?
            buffer_ = common::AllocationUtil::AllocateAligned(string_val.length());
            std::memcpy(buffer_, string_val.c_str(), string_val.length());
            value_ = std::make_unique<execution::sql::StringVal>(reinterpret_cast<const char *>(buffer_),
                                                                 string_val.length());
          }
          break;
        }
        default:
          UNREACHABLE("Invalid TypeId.");
      }
    } else {
      value_ = std::make_unique<execution::sql::Val>(true);
    }

    return exprs;
  }

 private:
  friend class binder::BindNodeVisitor; /* value_ may be modified, e.g., when parsing dates. */
  /** The constant held inside this ConstantValueExpression. */
  std::unique_ptr<execution::sql::Val> value_;

  byte *buffer_ = nullptr;
};

DEFINE_JSON_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
