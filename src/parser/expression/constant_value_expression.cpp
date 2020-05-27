#include "parser/expression/constant_value_expression.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "common/hash_util.h"
#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

ConstantValueExpression::ConstantValueExpression(const type::TypeId type, std::unique_ptr<execution::sql::Val> value)
    : ConstantValueExpression(type, std::move(value), nullptr) {
  TERRIER_ASSERT(value_ != nullptr, "Didn't provide a value.");
}

ConstantValueExpression::ConstantValueExpression(const type::TypeId type, std::unique_ptr<execution::sql::Val> value,
                                                 std::unique_ptr<byte> buffer)
    : AbstractExpression(ExpressionType::VALUE_CONSTANT, type, {}),
      value_(std::move(value)),
      buffer_(std::move(buffer)) {
  TERRIER_ASSERT(value_ != nullptr, "Didn't provide a value.");
  TERRIER_ASSERT(value_->is_null_ || (type != type::TypeId::VARCHAR && type != type::TypeId::VARBINARY) ||
                     (buffer_ == nullptr && GetValue().CastManagedPointerTo<execution::sql::StringVal>()->len_ <=
                                                execution::sql::StringVal::InlineThreshold()) ||
                     (buffer_ != nullptr && GetValue().CastManagedPointerTo<execution::sql::StringVal>()->len_ >
                                                execution::sql::StringVal::InlineThreshold()),
                 "Value should either be NULL, a non-varlen type, or varlen and below the threshold with no owned "
                 "buffer, or varlen and above the threshold with a provided buffer.");
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

DEFINE_JSON_BODY_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
