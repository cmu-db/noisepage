#pragma once
#include <type/transient_value.h>
#include <string>
#include <utility>

namespace terrier {
class DBMain;
}

namespace terrier::settings {
using callback_fn = void (DBMain::*)(void *, void *, const std::shared_ptr<common::ActionContext> &action_context);

/**
 * Param is a enum class, where all the setting names
 * are defined as enumerators.
 *
 * The enumerator list is defined by expanding the
 * _SETTING_ENUM_ part of macros defined in
 * settings_common.h
 */
/// @cond DOXYGEN_IGNORE
enum class Param {                     // NOLINT
#define __SETTING_ENUM__               // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_ENUM__                // NOLINT
};                                     // NOLINT
/// @endcond

/**
 * ParamInfo is the structure to hold settings information.
 */
class ParamInfo {
 public:
  /**
   * The constructor of ParamInfo
   * @param name setting name
   * @param value setting value
   * @param desc a description of the setting
   * @param default_value the default value of the setting
   * @param is_mutable if the setting is mutable or not
   * @param min_value allowed minimum value
   * @param max_value allowed maximum value
   * @param callback function invoked when param is changed
   */
  ParamInfo(std::string name, type::TransientValue &&value, std::string desc, type::TransientValue &&default_value,
            bool is_mutable, double min_value, double max_value, callback_fn callback)
      : name(std::move(name)),
        value(std::move(value)),
        desc(std::move(desc)),
        default_value(std::move(default_value)),
        is_mutable(is_mutable),
        min_value(min_value),
        max_value(max_value),
        callback(callback) {}

 private:
  friend class SettingsManager;
  std::string name;
  type::TransientValue value;
  std::string desc;
  type::TransientValue default_value;
  bool is_mutable;
  double min_value;
  double max_value;
  callback_fn callback;
};

}  // namespace terrier::settings
