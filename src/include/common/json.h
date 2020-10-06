#pragma once

#include <memory>

#include "common/managed_pointer.h"
#include "nlohmann/json.hpp"

namespace terrier::common {
// TODO(WAN): I think we should be using the adt_serializable, not this hack..
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 *
 * Using this macro also requires using the matching macro DEFINE_JSON_HEADER_DECLARATIONS
 * from json_header.h.
 */
using json = nlohmann::json;

#define DEFINE_JSON_BODY_DECLARATIONS(ClassName)                                        \
  void to_json(nlohmann::json &j, const ClassName &c) { j = c.ToJson(); } /* NOLINT */  \
  void to_json(nlohmann::json &j, const std::unique_ptr<ClassName> c) {   /* NOLINT */  \
    if (c != nullptr) {                                                                 \
      j = *c;                                                                           \
    } else {                                                                            \
      j = nullptr;                                                                      \
    }                                                                                   \
  }                                                                                     \
  void to_json(nlohmann::json &j, common::ManagedPointer<ClassName> c) { /* NOLINT */   \
    if (c != nullptr) {                                                                 \
      j = c->ToJson();                                                                  \
    } else {                                                                            \
      j = nullptr;                                                                      \
    }                                                                                   \
  }                                                                                     \
  void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */ \
  void from_json(const nlohmann::json &j, std::unique_ptr<ClassName> c) {  /* NOLINT */ \
    if (c != nullptr) {                                                                 \
      c->FromJson(j);                                                                   \
    }                                                                                   \
  }

}  // namespace terrier::common
