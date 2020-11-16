#pragma once

#include <memory>

#include "common/managed_pointer.h"
#include "nlohmann/json_fwd.hpp"

namespace noisepage::common {
// TODO(WAN): I think we should be using the adt_serializable, not this hack..
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 *
 * In order to use the following macro, you need to include this file and use
 * DEFINE_JSON_HEADER_DECLARATIONS in the .h file, and then need to include common/json.h
 * and use DEFINE_JSON_BODY_DECLARATIONS in the .cpp file with the same argument.
 *
 */
using json = nlohmann::json;

#define DEFINE_JSON_HEADER_DECLARATIONS(ClassName)                                   \
  void to_json(nlohmann::json &j, const ClassName &c);                  /* NOLINT */ \
  void to_json(nlohmann::json &j, const std::unique_ptr<ClassName> c);  /* NOLINT */ \
  void to_json(nlohmann::json &j, common::ManagedPointer<ClassName> c); /* NOLINT */ \
  void from_json(const nlohmann::json &j, ClassName &c);                /* NOLINT */ \
  void from_json(const nlohmann::json &j, std::unique_ptr<ClassName> c);

}  // namespace noisepage::common
