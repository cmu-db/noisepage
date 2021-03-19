#pragma once

#include <string>

#include "common/error/exception.h"

/** Used in defining the actual enum values. */
#define ENUM_DEFINE_ITEM(EnumType, EnumVal) EnumVal,

/** Used in defining EnumTypeToString(). */
#define ENUM_TO_STRING(EnumType, EnumVal) \
  case EnumType::EnumVal: {               \
    return #EnumVal;                      \
  }

/** Used in defining EnumTypeFromString(). Hardcoded to expect str to be in scope. */
#define ENUM_FROM_STRING(EnumType, EnumVal)                             \
  else if (str == #EnumVal) { /* NOLINT readability/braces confusion */ \
    return EnumType::EnumVal;                                           \
  }

/**
 * Defines:
 * 1. An enum class named EnumName of type EnumCppType.
 * 2. An EnumNameToString() function.
 * 3. An EnumNameFromString() function.
 *
 * Please see ExpressionType for example usage.
 *
 * The purpose of this macro is to keep the ToString and FromString defined and in-sync.
 * The string version of the enum is the same as the enum value itself.
 */
#define ENUM_DEFINE(EnumName, EnumCppType, EnumValMacro)                                         \
  enum class EnumName : EnumCppType { EnumValMacro(ENUM_DEFINE_ITEM) NUM_ENUM_ENTRIES };         \
                                                                                                 \
  /** @return String version of @p type. */                                                      \
  inline std::string EnumName##ToString(EnumName type) { /* NOLINT inline usage */               \
    switch (type) {                                                                              \
      EnumValMacro(ENUM_TO_STRING);                                                              \
      default: {                                                                                 \
        throw CONVERSION_EXCEPTION(                                                              \
            ("No enum conversion for: " + std::to_string(static_cast<uint64_t>(type))).c_str()); \
      }                                                                                          \
    }                                                                                            \
  }                                                                                              \
                                                                                                 \
  /** @return Enum version of @p str. */                                                         \
  inline EnumName EnumName##FromString(const std::string &str) { /* NOLINT inline usage */       \
    if (false) { /* This check starts an if-else chain for the macro. */                         \
    }                                                                                            \
    EnumValMacro(ENUM_FROM_STRING) else { /* NOLINT readability/braces confusion */              \
      throw CONVERSION_EXCEPTION("No enum conversion for: " + str);                              \
    }                                                                                            \
  }
