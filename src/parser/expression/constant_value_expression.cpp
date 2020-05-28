#include "parser/expression/constant_value_expression.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

template <typename T>
ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const T value)
    : AbstractExpression(ExpressionType::VALUE_CONSTANT, type, {}), value_(value) {
  Validate();
}

ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::StringVal value,
                                                 std::unique_ptr<byte[]> buffer)
    : AbstractExpression(ExpressionType::VALUE_CONSTANT, type, {}), value_(value), buffer_(std::move(buffer)) {
  Validate();
}

void ConstantValueExpression::Validate() const {
  if (std::holds_alternative<execution::sql::Val>(value_)) {
    TERRIER_ASSERT(
        std::get<execution::sql::Val>(value_).is_null_,
        "Should have only constructed a base-type Val in the event of a NULL (likely coming out of PostgresParser).");
  } else if (std::holds_alternative<execution::sql::BoolVal>(value_)) {
    TERRIER_ASSERT(return_value_type_ == type::TypeId::BOOLEAN, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::Integer>(value_)) {
    TERRIER_ASSERT(return_value_type_ == type::TypeId::TINYINT || return_value_type_ == type::TypeId::SMALLINT ||
                       return_value_type_ == type::TypeId::INTEGER || return_value_type_ == type::TypeId::BIGINT,
                   "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::Real>(value_)) {
    TERRIER_ASSERT(return_value_type_ == type::TypeId::DECIMAL, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::DateVal>(value_)) {
    TERRIER_ASSERT(return_value_type_ == type::TypeId::DATE, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::TimestampVal>(value_)) {
    TERRIER_ASSERT(return_value_type_ == type::TypeId::TIMESTAMP, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::StringVal>(value_)) {
    TERRIER_ASSERT(return_value_type_ == type::TypeId::VARCHAR || return_value_type_ == type::TypeId::VARBINARY,
                   "Invalid TypeId for Val type.");
    TERRIER_ASSERT(GetStringVal().is_null_ ||
                       (buffer_ == nullptr && GetStringVal().len_ <= execution::sql::StringVal::InlineThreshold()) ||
                       (buffer_ != nullptr && GetStringVal().len_ > execution::sql::StringVal::InlineThreshold()),
                   "StringVal should either be NULL, below the inline threshold with no owned buffer, or above the "
                   "inline threshold with a provided buffer.");
  } else {
    UNREACHABLE("Unknown Val type!");
  }
}

template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Val value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::BoolVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Integer value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Real value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Decimal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                          const execution::sql::StringVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::DateVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                          const execution::sql::TimestampVal value);

template <typename T>
T ConstantValueExpression::Peek() const {
  if constexpr (std::is_same_v<T, bool>) {
    return static_cast<T>(GetBoolVal().val_);
  }
  if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
                std::is_same_v<T, int64_t>) {
    return static_cast<T>(GetInteger().val_);
  }
  if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    return static_cast<T>(GetReal().val_);
  }
  if constexpr (std::is_same_v<T, execution::sql::Date>) {
    return static_cast<T>(GetDateVal().val_);
  }
  if constexpr (std::is_same_v<T, execution::sql::Timestamp>) {
    return static_cast<T>(GetTimestampVal().val_);
  }
  if constexpr (std::is_same_v<T, std::string_view>) {
    return static_cast<T>(GetStringVal().StringView());
  }
  UNREACHABLE("Invalid type for GetAs.");
}

template bool ConstantValueExpression::Peek() const;
template int8_t ConstantValueExpression::Peek() const;
template int16_t ConstantValueExpression::Peek() const;
template int32_t ConstantValueExpression::Peek() const;
template int64_t ConstantValueExpression::Peek() const;
template float ConstantValueExpression::Peek() const;
template double ConstantValueExpression::Peek() const;
template execution::sql::Date ConstantValueExpression::Peek() const;
template execution::sql::Timestamp ConstantValueExpression::Peek() const;
template std::string_view ConstantValueExpression::Peek() const;

ConstantValueExpression &ConstantValueExpression::operator=(const ConstantValueExpression &other) {
  if (this != &other) {  // self-assignment check expected
    // AbstractExpression fields we need copied over
    expression_type_ = other.expression_type_;
    expression_name_ = other.expression_name_;
    alias_ = other.alias_;
    return_value_type_ = other.return_value_type_;
    depth_ = other.depth_;
    has_subquery_ = other.has_subquery_;
    // ConstantValueExpression fields
    if (other.value_->is_null_) {
      value_ = std::make_unique<execution::sql::Val>(true);
      buffer_ = nullptr;
    } else {
      switch (other.GetReturnValueType()) {
        case type::TypeId::BOOLEAN: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
          value_ = std::make_unique<execution::sql::BoolVal>(val);
          buffer_ = nullptr;
          break;
        }
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
          value_ = std::make_unique<execution::sql::Integer>(val);
          buffer_ = nullptr;
          break;
        }
        case type::TypeId::DECIMAL: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
          value_ = std::make_unique<execution::sql::Real>(val);
          buffer_ = nullptr;
          break;
        }
        case type::TypeId::TIMESTAMP: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_;
          value_ = std::make_unique<execution::sql::TimestampVal>(val);
          buffer_ = nullptr;
          break;
        }
        case type::TypeId::DATE: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_;
          value_ = std::make_unique<execution::sql::DateVal>(val);
          buffer_ = nullptr;
          break;
        }
        case type::TypeId::VARCHAR:
        case type::TypeId::VARBINARY: {
          const auto val = other.GetValue().CastManagedPointerTo<execution::sql::StringVal>();
          auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
          value_ = std::move(string_val.first);
          buffer_ = std::move(string_val.second);
          break;
        }
        default:
          UNREACHABLE("Invalid TypeId.");
      }
    }
  }
  return *this;
}

ConstantValueExpression &ConstantValueExpression::operator=(ConstantValueExpression &&other) noexcept {
  if (this != &other) {  // self-assignment check expected
    // AbstractExpression fields we need moved over
    expression_type_ = other.expression_type_;
    expression_name_ = std::move(other.expression_name_);
    alias_ = std::move(other.alias_);
    return_value_type_ = other.return_value_type_;
    depth_ = other.depth_;
    has_subquery_ = other.has_subquery_;
    // ConstantValueExpression fields
    value_ = std::move(other.value_);
    buffer_ = std::move(other.buffer_);
    // Set other to NULL because unclear what else it would be in this case
    other.value_ = std::make_unique<execution::sql::Val>(true);
  }
  return *this;
}

ConstantValueExpression::ConstantValueExpression(ConstantValueExpression &&other) noexcept : AbstractExpression(other) {
  value_ = std::move(other.value_);
  buffer_ = std::move(other.buffer_);
  other.value_ = std::make_unique<execution::sql::Val>(true);
}

ConstantValueExpression::ConstantValueExpression(const ConstantValueExpression &other) : AbstractExpression(other) {
  if (other.GetReturnValueType() == type::TypeId::VARCHAR || other.GetReturnValueType() == type::TypeId::VARBINARY) {
    value_ = std::make_unique<execution::sql::Val>(true);
  } else {
    const auto val = other.GetStringVal();

    auto string_val = execution::sql::ValueUtil::CreateStringVal(val);

    value_ = string_val.first;
    buffer_ = std::move(string_val.second);
  }
}

common::hash_t ConstantValueExpression::Hash() const {
  const auto hash =
      common::HashUtil::CombineHashes(AbstractExpression::Hash(), common::HashUtil::Hash(value_->is_null_));
  if (value_->is_null_) return hash;

  switch (GetReturnValueType()) {
    case type::TypeId::BOOLEAN: {
      const auto val = GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(val));
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      const auto val = GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(val));
    }
    case type::TypeId::DECIMAL: {
      const auto val = GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(val));
    }
    case type::TypeId::TIMESTAMP: {
      const auto val = GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_.ToNative();
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(val));
    }
    case type::TypeId::DATE: {
      const auto val = GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_.ToNative();
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(val));
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      const auto val = GetValue().CastManagedPointerTo<execution::sql::StringVal>()->StringView();
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(val));
    }
    default:
      UNREACHABLE("Invalid TypeId.");
  }
}

bool ConstantValueExpression::operator==(const AbstractExpression &other) const {
  if (!AbstractExpression::operator==(other)) return false;
  const auto &other_cve = dynamic_cast<const ConstantValueExpression &>(other);

  if (value_->is_null_ != other_cve.value_->is_null_) return false;
  if (value_->is_null_ && other_cve.value_->is_null_) return true;

  switch (other.GetReturnValueType()) {
    case type::TypeId::BOOLEAN: {
      const auto this_val = GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
      const auto other_val = other_cve.GetValue().CastManagedPointerTo<execution::sql::BoolVal>()->val_;
      return this_val == other_val;
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      const auto this_val = GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
      const auto other_val = other_cve.GetValue().CastManagedPointerTo<execution::sql::Integer>()->val_;
      return this_val == other_val;
    }
    case type::TypeId::DECIMAL: {
      const auto this_val = GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
      const auto other_val = other_cve.GetValue().CastManagedPointerTo<execution::sql::Real>()->val_;
      return this_val == other_val;
    }
    case type::TypeId::TIMESTAMP: {
      const auto this_val = GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_;
      const auto other_val = other_cve.GetValue().CastManagedPointerTo<execution::sql::TimestampVal>()->val_;
      return this_val == other_val;
    }
    case type::TypeId::DATE: {
      const auto this_val = GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_;
      const auto other_val = other_cve.GetValue().CastManagedPointerTo<execution::sql::DateVal>()->val_;
      return this_val == other_val;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      const auto this_val = GetValue().CastManagedPointerTo<execution::sql::StringVal>()->StringView();
      const auto other_val = other_cve.GetValue().CastManagedPointerTo<execution::sql::StringVal>()->StringView();
      return this_val == other_val;
    }
    default:
      UNREACHABLE("Invalid TypeId.");
  }
}

nlohmann::json ConstantValueExpression::ToJson() const {
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

std::vector<std::unique_ptr<AbstractExpression>> ConstantValueExpression::FromJson(const nlohmann::json &j) {
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
        value_ =
            std::make_unique<execution::sql::DateVal>(execution::sql::Date::FromNative(j.at("value").get<uint32_t>()));
        break;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        auto string_val = execution::sql::ValueUtil::CreateStringVal(j.at("value").get<std::string>());

        value_ = std::move(string_val.first);
        buffer_ = std::move(string_val.second);

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
}  // namespace terrier::parser
