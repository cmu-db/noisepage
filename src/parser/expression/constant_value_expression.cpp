#include "parser/expression/constant_value_expression.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
#include "parser/expression/abstract_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::parser {

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
    NOISEPAGE_ASSERT(
        std::get<execution::sql::Val>(value_).is_null_,
        "Should have only constructed a base-type Val in the event of a NULL (likely coming out of PostgresParser).");
  } else if (std::holds_alternative<execution::sql::BoolVal>(value_)) {
    NOISEPAGE_ASSERT(return_value_type_ == type::TypeId::BOOLEAN, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::Integer>(value_)) {
    NOISEPAGE_ASSERT(return_value_type_ == type::TypeId::TINYINT || return_value_type_ == type::TypeId::SMALLINT ||
                         return_value_type_ == type::TypeId::INTEGER || return_value_type_ == type::TypeId::BIGINT,
                     "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::Real>(value_)) {
    NOISEPAGE_ASSERT(return_value_type_ == type::TypeId::DECIMAL, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::DateVal>(value_)) {
    NOISEPAGE_ASSERT(return_value_type_ == type::TypeId::DATE, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::TimestampVal>(value_)) {
    NOISEPAGE_ASSERT(return_value_type_ == type::TypeId::TIMESTAMP, "Invalid TypeId for Val type.");
  } else if (std::holds_alternative<execution::sql::StringVal>(value_)) {
    NOISEPAGE_ASSERT(return_value_type_ == type::TypeId::VARCHAR || return_value_type_ == type::TypeId::VARBINARY,
                     "Invalid TypeId for Val type.");
    NOISEPAGE_ASSERT(
        GetStringVal().is_null_ ||
            (buffer_ == nullptr && GetStringVal().GetLength() <= execution::sql::StringVal::InlineThreshold()) ||
            (buffer_ != nullptr && GetStringVal().GetLength() > execution::sql::StringVal::InlineThreshold()),
        "StringVal should either be NULL, below the InlineThreshold with no owned buffer, or above the "
        "InlineThreshold with a provided buffer.");
  } else {
    UNREACHABLE("Unknown Val type!");
  }
}

template <typename T>
T ConstantValueExpression::Peek() const {
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (std::is_same_v<T, bool>) {
    return static_cast<T>(GetBoolVal().val_);
  }
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
                std::is_same_v<T, int64_t>) {  // NOLINT: bugprone-suspicious-semicolon: seems like a false positive
                                               // because of constexpr
    return static_cast<T>(GetInteger().val_);
  }
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    return static_cast<T>(GetReal().val_);
  }
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (std::is_same_v<T, execution::sql::Date>) {
    return GetDateVal().val_;
  }
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (std::is_same_v<T, execution::sql::Timestamp>) {
    return GetTimestampVal().val_;
  }
  // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
  if constexpr (std::is_same_v<T, std::string_view>) {
    return std::get<execution::sql::StringVal>(value_).StringView();
  }
  UNREACHABLE("Invalid type for Peek.");
}

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
    if (std::holds_alternative<execution::sql::StringVal>(other.value_)) {
      auto string_val = execution::sql::ValueUtil::CreateStringVal(other.GetStringVal());

      value_ = string_val.first;
      buffer_ = std::move(string_val.second);
    } else {
      value_ = other.value_;
      buffer_ = nullptr;
    }
  }
  Validate();
  return *this;
}

ConstantValueExpression::ConstantValueExpression(const ConstantValueExpression &other) : AbstractExpression(other) {
  if (std::holds_alternative<execution::sql::StringVal>(other.value_)) {
    auto string_val = execution::sql::ValueUtil::CreateStringVal(other.GetStringVal());

    value_ = string_val.first;
    buffer_ = std::move(string_val.second);
  } else {
    value_ = other.value_;
  }
  Validate();
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
    value_ = other.value_;
    buffer_ = std::move(other.buffer_);
    // Set other to NULL because unclear what else it would be in this case
    other.value_ = execution::sql::Val(true);
  }
  Validate();
  return *this;
}

ConstantValueExpression::ConstantValueExpression(ConstantValueExpression &&other) noexcept : AbstractExpression(other) {
  value_ = other.value_;
  buffer_ = std::move(other.buffer_);
  // Set other to NULL because unclear what else it would be in this case
  other.value_ = execution::sql::Val(true);
  Validate();
}

common::hash_t ConstantValueExpression::Hash() const {
  const auto hash = common::HashUtil::CombineHashes(AbstractExpression::Hash(), common::HashUtil::Hash(IsNull()));
  if (IsNull()) return hash;

  switch (GetReturnValueType()) {
    case type::TypeId::BOOLEAN: {
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Peek<bool>()));
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Peek<int64_t>()));
    }
    case type::TypeId::DECIMAL: {
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Peek<double>()));
    }
    case type::TypeId::TIMESTAMP: {
      return common::HashUtil::CombineHashes(hash,
                                             common::HashUtil::Hash(Peek<execution::sql::Timestamp>().ToNative()));
    }
    case type::TypeId::DATE: {
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Peek<execution::sql::Date>().ToNative()));
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      return common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(Peek<std::string_view>()));
    }
    default:
      UNREACHABLE("Invalid TypeId.");
  }
}

bool ConstantValueExpression::operator==(const AbstractExpression &other) const {
  if (!AbstractExpression::operator==(other)) return false;
  const auto &other_cve = dynamic_cast<const ConstantValueExpression &>(other);

  if (IsNull() != other_cve.IsNull()) return false;
  if (IsNull() && other_cve.IsNull()) return true;

  switch (other.GetReturnValueType()) {
    case type::TypeId::BOOLEAN: {
      return Peek<bool>() == other_cve.Peek<bool>();
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      return Peek<int64_t>() == other_cve.Peek<int64_t>();
    }
    case type::TypeId::DECIMAL: {
      return Peek<double>() == other_cve.Peek<double>();
    }
    case type::TypeId::TIMESTAMP: {
      return Peek<execution::sql::Timestamp>() == other_cve.Peek<execution::sql::Timestamp>();
    }
    case type::TypeId::DATE: {
      return Peek<execution::sql::Date>() == other_cve.Peek<execution::sql::Date>();
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      return Peek<std::string_view>() == other_cve.Peek<std::string_view>();
    }
    default:
      UNREACHABLE("Invalid TypeId.");
  }
}

std::string ConstantValueExpression::ToString() const {
  switch (GetReturnValueType()) {
    case type::TypeId::BOOLEAN: {
      return fmt::format("{}", GetBoolVal().val_);
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      return fmt::format("{}", GetInteger().val_);
    }
    case type::TypeId::DECIMAL: {
      return fmt::format("{}", GetReal().val_);
    }
    case type::TypeId::TIMESTAMP: {
      return fmt::format("{}", GetTimestampVal().val_.ToString());
    }
    case type::TypeId::DATE: {
      return fmt::format("{}", GetDateVal().val_.ToString());
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      return fmt::format("{}", GetStringVal().val_.StringView());
    }
    default:
      UNREACHABLE("Invalid TypeId.");
  }
}

nlohmann::json ConstantValueExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();

  if (!IsNull()) {
    switch (return_value_type_) {
      case type::TypeId::BOOLEAN: {
        j["value"] = Peek<bool>();
        break;
      }
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT: {
        j["value"] = Peek<int64_t>();
        break;
      }
      case type::TypeId::DECIMAL: {
        j["value"] = Peek<double>();
        break;
      }
      case type::TypeId::TIMESTAMP: {
        j["value"] = Peek<execution::sql::Timestamp>().ToNative();
        break;
      }
      case type::TypeId::DATE: {
        j["value"] = Peek<execution::sql::Date>().ToNative();
        break;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        std::string val{Peek<std::string_view>()};
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
        value_ = execution::sql::BoolVal(j.at("value").get<bool>());
        break;
      }
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT: {
        value_ = execution::sql::Integer(j.at("value").get<int64_t>());
        break;
      }
      case type::TypeId::DECIMAL: {
        value_ = execution::sql::Real(j.at("value").get<double>());
        break;
      }
      case type::TypeId::TIMESTAMP: {
        value_ = execution::sql::TimestampVal(execution::sql::Timestamp::FromNative(j.at("value").get<uint64_t>()));
        break;
      }
      case type::TypeId::DATE: {
        value_ = execution::sql::DateVal(execution::sql::Date::FromNative(j.at("value").get<uint32_t>()));
        break;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        auto string_val = execution::sql::ValueUtil::CreateStringVal(j.at("value").get<std::string>());

        value_ = string_val.first;
        buffer_ = std::move(string_val.second);

        break;
      }
      default:
        UNREACHABLE("Invalid TypeId.");
    }
  } else {
    value_ = execution::sql::Val(true);
  }

  Validate();

  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ConstantValueExpression);

template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Val value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::BoolVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Integer value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::Real value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                          const execution::sql::DecimalVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                          const execution::sql::StringVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type, const execution::sql::DateVal value);
template ConstantValueExpression::ConstantValueExpression(const type::TypeId type,
                                                          const execution::sql::TimestampVal value);

template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::Val value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::BoolVal value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::Integer value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::Real value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::DecimalVal value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::StringVal value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::DateVal value);
template void ConstantValueExpression::SetValue(const type::TypeId type, const execution::sql::TimestampVal value);

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

}  // namespace noisepage::parser
