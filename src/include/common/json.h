#pragma once

#include <memory>
#include "common/managed_pointer.h"
#include "nlohmann/json.hpp"

namespace terrier::common {
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 */
using json = nlohmann::json;

#define DEFINE_JSON_DECLARATIONS(ClassName)                                                          \
  inline void to_json(nlohmann::json &j, const ClassName &c) { j = c.ToJson(); } /* NOLINT */        \
  inline void to_json(nlohmann::json &j, const ClassName *c) {                   /* NOLINT */        \
    if (c != nullptr) {                                                                              \
      j = *c;                                                                                        \
    } else {                                                                                         \
      j = nullptr;                                                                                   \
    }                                                                                                \
  }                                                                                                  \
  inline void to_json(nlohmann::json &j, const common::ManagedPointer<ClassName> c) { /* NOLINT */   \
    if (c) {                                                                                         \
      j = *c;                                                                                        \
    } else {                                                                                         \
      j = nullptr;                                                                                   \
    }                                                                                                \
  }                                                                                                  \
  inline void from_json(const nlohmann::json &j, ClassName &c) { c.FromJson(j); } /* NOLINT */       \
  inline void from_json(const nlohmann::json &j, ClassName *c) {                  /* NOLINT */       \
    if (c != nullptr) {                                                                              \
      c->FromJson(j);                                                                                \
    }                                                                                                \
  }                                                                                                  \
  inline void from_json(const nlohmann::json &j, common::ManagedPointer<ClassName> c) { /* NOLINT */ \
    if (c) {                                                                                         \
      c->FromJson(j);                                                                                \
    }                                                                                                \
  }

}  // namespace terrier::common
