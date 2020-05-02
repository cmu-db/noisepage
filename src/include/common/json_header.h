#pragma once

#include <memory>
#include "common/managed_pointer.h"
#include "nlohmann/json_fwd.hpp"

namespace terrier::common {
// TODO(WAN): I think we should be using the adt_serializable, not this hack..
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 */
using json = nlohmann::json;

#define DEFINE_JSON_HEADER_DECLARATIONS(ClassName)                                           \
  inline void to_json(nlohmann::json &j, const ClassName &c);                   /* NOLINT */ \
  inline void to_json(nlohmann::json &j, const std::unique_ptr<ClassName> c);   /* NOLINT */ \
  inline void to_json(nlohmann::json &j, common::ManagedPointer<ClassName> c);  /* NOLINT */ \
  inline void from_json(const nlohmann::json &j, ClassName &c);                 /* NOLINT */ \
  inline void from_json(const nlohmann::json &j, std::unique_ptr<ClassName> c);

}  // namespace terrier::common