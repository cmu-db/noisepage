#pragma once
#include <type/value.h>
#include <string>

namespace terrier::settings {

enum class Param {
#define __SETTING_ENUM__
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_ENUM__
};

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
  ParamInfo(std::string name, const type::Value &value, std::string desc, const type::Value &default_value,
            bool is_mutable)
      : name(std::move(name)),
        value(value),
        desc(std::move(desc)),
        default_value(default_value),
        is_mutable(is_mutable) {}

 private:
  friend class SettingsManager;
  std::string name;
  type::Value value;
  std::string desc;
  type::Value default_value;
  bool is_mutable;
};

}  // namespace terrier::settings