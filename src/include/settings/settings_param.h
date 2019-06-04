#pragma once
#include <memory>
#include <string>
#include <utility>
#include "common/managed_pointer.h"
#include "type/transient_value.h"

namespace terrier {
class DBMain;
}

namespace terrier::settings {

using callback_fn = void (*)(void *, void *, DBMain *,
                             const common::ManagedPointer<common::ActionContext> &action_context);

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
      : name_(std::move(name)),
        value_(std::move(value)),
        desc_(std::move(desc)),
        default_value_(std::move(default_value)),
        is_mutable_(is_mutable),
        min_value_(min_value),
        max_value_(max_value),
        callback_(callback) {}

 private:
  friend class terrier::DBMain;
  friend class SettingsManager;
  std::string name_;
  type::TransientValue value_;
  std::string desc_;
  type::TransientValue default_value_;
  bool is_mutable_;
  double min_value_;
  double max_value_;
  callback_fn callback_;
};

}  // namespace terrier::settings
