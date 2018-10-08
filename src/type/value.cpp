#include "type/value.h"
#include <cstring>
#include <string>
#include "type/type_id.h"

namespace terrier::type {

Value::~Value() {
  switch (type_id_) {
    case TypeId::VARCHAR:
    case TypeId::VARBINARY:
      free(value_.varchar);
      break;

    default:
      break;
  }
}

// copy constructor
Value::Value(const Value &other) {
  type_id_ = other.type_id_;
  value_ = other.value_;
}

// scalar constructors
Value::Value(boolean_t value) {
  value_.boolean = value;
  type_id_ = TypeId::BOOLEAN;
}

Value::Value(int8_t value) {
  value_.tinyint = value;
  type_id_ = TypeId::TINYINT;
}

Value::Value(int16_t value) {
  value_.smallint = value;
  type_id_ = TypeId::SMALLINT;
}

Value::Value(int32_t value) {
  value_.integer = value;
  type_id_ = TypeId::INTEGER;
}

Value::Value(int64_t value) {
  value_.bigint = value;
  type_id_ = TypeId::BIGINT;
}

Value::Value(double value) {
  value_.decimal = value;
  type_id_ = TypeId::DECIMAL;
}

Value::Value(timestamp_t value) {
  value_.timestamp = value;
  type_id_ = TypeId::TIMESTAMP;
}

Value::Value(date_t value) {
  value_.date = value;
  type_id_ = TypeId::DATE;
}

// varchar
Value::Value(const std::string &value) {
  type_id_ = TypeId::VARCHAR;
  // we don't want the null terminator
  size_t str_len = value.length() - 1;
  value_.varchar = static_cast<byte *>(malloc(str_len));
  memcpy(const_cast<char *>(value.data()), value_.varchar, str_len);
}

Value::Value(const byte *data, uint32_t len) {
  type_id_ = TypeId::VARBINARY;
  value_.varchar = static_cast<byte *>(malloc(len));
  memcpy(const_cast<byte *>(data), value_.varchar, len);
}

bool Value::operator==(const Value &rhs) const {
  bool return_bool = false;

  TypeId my_type = type_id_;
  if (my_type != rhs.GetType()) {
    return false;
  }

  switch (my_type) {
    case TypeId::BOOLEAN:
      return_bool = (*GetBooleanValue() == *rhs.GetBooleanValue());
      break;

    case TypeId::TINYINT:
      return_bool = (*GetTinyIntValue() == *rhs.GetTinyIntValue());
      break;

    case TypeId::SMALLINT:
      return_bool = (*GetSmallIntValue() == *rhs.GetSmallIntValue());
      break;

    case TypeId::INTEGER:
      return_bool = (*GetIntValue() == *rhs.GetIntValue());
      break;

    case TypeId::BIGINT:
      return_bool = (*GetBigIntValue() == *rhs.GetBigIntValue());
      break;

    case TypeId::DATE:
      return_bool = (*GetDateValue() == *rhs.GetDateValue());
      break;

    case TypeId::DECIMAL:
      return_bool = (*GetDecimalValue() == *rhs.GetDecimalValue());
      break;

    case TypeId::TIMESTAMP:
      return_bool = (*GetTimestampValue() == *rhs.GetTimestampValue());
      break;

    default:
      TERRIER_ASSERT(false, "unsupported type");
      break;
  }
  return return_bool;
}

}  // namespace terrier::type
