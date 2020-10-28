#pragma once

#include "common/json.h"
#include "common/strong_typedef.h"

namespace noisepage::common {

/*
 * Using this macro also requires, using the matching macro STRONG_TYPEDEF_HEADER from strong_typedef.h.
 */

#define STRONG_TYPEDEF_BODY(name, underlying_type)                                              \
  using name = ::noisepage::common::StrongTypeAlias<tags::name##_typedef_tag, underlying_type>; \
  namespace tags {                                                                              \
  void to_json(nlohmann::json &j, const name &c) { j = c.ToJson(); }  /* NOLINT */              \
  void from_json(const nlohmann::json &j, name &c) { c.FromJson(j); } /* NOLINT */              \
  }

}  // namespace noisepage::common
