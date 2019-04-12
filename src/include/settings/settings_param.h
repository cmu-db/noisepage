#pragma once
#include <type/transient_value.h>
#include <string>
#include <utility>

namespace terrier::settings {

enum class Param {                     // NOLINT
#define __SETTING_ENUM__               // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_ENUM__                // NOLINT
};                                     // NOLINT

/**
 * ParamInfo is the structure to hold settings information.
 */
struct ParamInfo {
  /**
   * The constructor of ParamInfo
   * @param name setting name
   * @param value setting value
   * @param desc a description of the setting
   * @param default_value the default value of the setting
   * @param is_mutable if the setting is mutable or not
   */
  ParamInfo(std::string name, type::TransientValue &&value, std::string desc, type::TransientValue &&default_value,
            bool is_mutable)
      : name(std::move(name)),
        value(std::move(value)),
        desc(std::move(desc)),
        default_value(std::move(default_value)),
        is_mutable(is_mutable) {}

 private:
  friend class SettingsManager;
  std::string name;
  type::TransientValue value;
  std::string desc;
  type::TransientValue default_value;
  bool is_mutable;
};

}  // namespace terrier::settings
