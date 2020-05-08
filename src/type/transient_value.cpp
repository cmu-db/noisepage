#include "type/transient_value.h"
#include "common/json.h"

namespace terrier::type {

nlohmann::json TransientValue::ToJson() const {
  nlohmann::json j;
  j["type"] = type_;
  if (Type() == TypeId::VARCHAR && !Null()) {
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(data_);
    auto varchar = std::string(reinterpret_cast<const char *const>(data_), length + sizeof(uint32_t));
    j["data"] = varchar;
  } else {
    j["data"] = data_;
  }
  return j;
}


void TransientValue::FromJson(const nlohmann::json &j) {
  type_ = j.at("type").get<TypeId>();
  if (Type() == TypeId::VARCHAR && !Null()) {
    data_ = 0;
    CopyVarChar(reinterpret_cast<const char *const>(j.at("data").get<std::string>().c_str()));
  } else {
    data_ = j.at("data").get<uintptr_t>();
  }
}

DEFINE_JSON_BODY_DECLARATIONS(TransientValue);

} // namespace terrier::type