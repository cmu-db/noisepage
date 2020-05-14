#pragma once

#include <memory>
#include "common/managed_pointer.h"
#include "nlohmann/json.hpp"

namespace terrier::common {
// TODO(WAN): I think we should be using the adt_serializable, not this hack..
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 *
 * In order to use the following macro, you need to include common/json_header.h and use
 * DEFINE_JSON_HEADER_DECLARATIONS in the .h file, and then need to include this file and use
 * DEFINE_JSON_BODY_DECLARATIONS in the .cpp file with the same argument.
 *
 */
using json = nlohmann::json;

#define DEFINE_JSON_BODY_DECLARATIONS(ClassName)                                                          \
  void to_json(nlohmann::json &j, const ClassName &c) {j = c.ToJson();} /* NOLINT */                      \
  void to_json(nlohmann::json &j, const std::unique_ptr<ClassName> c) { /* NOLINT */                      \
    if (c != nullptr) {                                                                                   \
      j = *c;                                                                                             \
    } else {                                                                                              \
      j = nullptr;                                                                                        \
    }                                                                                                     \
  }                                                                                                       \
  void to_json(nlohmann::json &j, common::ManagedPointer<ClassName> c) { /* NOLINT */                     \
    if (c != nullptr) {                                                                                   \
      j = c->ToJson();                                                                                    \
    } else {                                                                                              \
      j = nullptr;                                                                                        \
    }                                                                                                     \
  }                                                                                                       \
  void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */                   \
  void from_json(const nlohmann::json &j, std::unique_ptr<ClassName> c) {  /* NOLINT */                   \
    if (c != nullptr) {                                                                                   \
      c->FromJson(j);                                                                                     \
    }                                                                                                     \
  }

}  // namespace terrier::common
