#pragma once

#include "common/json.h"
#include "common/strong_typedef.h"

namespace terrier::common {

#define STRONG_TYPEDEF_BODY(name, underlying_type)                                            \
  using name = ::terrier::common::StrongTypeAlias<tags::name##_typedef_tag, underlying_type>; \
  namespace tags {                                                                            \
  void to_json(nlohmann::json &j, const name &c) { j = c.ToJson(); }  /* NOLINT */            \
  void from_json(const nlohmann::json &j, name &c) { c.FromJson(j); } /* NOLINT */            \
  }

}  // namespace terrier::common
