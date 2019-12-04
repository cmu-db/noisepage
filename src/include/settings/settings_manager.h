#pragma once

#include <gflags/gflags.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/action_context.h"
#include "common/exception.h"
#include "common/shared_latch.h"
#include "loggers/settings_logger.h"
#include "settings/settings_param.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier::settings {
using setter_callback_fn = void (*)(common::ManagedPointer<common::ActionContext> action_context);

/**
 * A wrapper for pg_settings table, does not store values in it.
 * Stores and triggers callbacks when a tunable parameter is changed.
 * Static class.
 */

class SettingsManager {
 public:
  SettingsManager() = delete;
  SettingsManager(const SettingsManager &) = delete;

  /**
   * The constructor of settings manager
   * @param db a pointer to the DBMain
   */
  explicit SettingsManager(common::ManagedPointer<DBMain> db_main);

  /**
   * Get the value of an integer setting
   * @param param setting name
   * @return current setting value
   */
  int32_t GetInt(Param param);

  /**
   * Get the value of an int64_t setting
   * @param param setting name
   * @return current setting value
   */
  int64_t GetInt64(Param param);

  /**
   * Get the value of a double setting
   * @param param setting name
   * @return current setting value
   */
  double GetDouble(Param param);

  /**
   * Get the value of a boolean setting
   * @param param setting name
   * @return current setting value
   */
  bool GetBool(Param param);

  /**
   * Get the value of a string setting
   * @param param setting name
   * @return current setting value
   */
  std::string GetString(Param param);

  /**
   * Set the value of an integer setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting an integer param
   * @param setter_callback callback from caller
   */
  void SetInt(Param param, int32_t value, common::ManagedPointer<common::ActionContext> action_context,
              setter_callback_fn setter_callback);

  /**
   * Set the value of an int64_t setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting an integer param
   * @param setter_callback callback from caller
   */
  void SetInt64(Param param, int64_t value, common::ManagedPointer<common::ActionContext> action_context,
                setter_callback_fn setter_callback);

  /**
   * Set the value of a double setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting a double param
   * @param setter_callback callback from caller
   */
  void SetDouble(Param param, double value, common::ManagedPointer<common::ActionContext> action_context,
                 setter_callback_fn setter_callback);

  /**
   * Set the value of a boolean setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting a boolean param
   * @param setter_callback callback from caller
   */
  void SetBool(Param param, bool value, common::ManagedPointer<common::ActionContext> action_context,
               setter_callback_fn setter_callback);

  /**
   * Set the value of a string setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting a string param
   * @param setter_callback callback from caller
   */
  void SetString(Param param, const std::string_view &value,
                 common::ManagedPointer<common::ActionContext> action_context, setter_callback_fn setter_callback);

  /**
   * Validate values from DBMain map
   */
  void ValidateParams();

  /**
   * Construct settings param map from settings_defs.h
   * @param param_map
   */
  static void ConstructParamMap(                                                               // NOLINT
      std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> &param_map);  // NOLINT

 private:
  common::ManagedPointer<DBMain> db_main_;
  common::SharedLatch latch_;

  void ValidateSetting(Param param, const type::TransientValue &min_value, const type::TransientValue &max_value);

  type::TransientValue &GetValue(Param param);
  bool SetValue(Param param, type::TransientValue value);
  bool ValidateValue(const type::TransientValue &value, const type::TransientValue &min_value,
                     const type::TransientValue &max_value);
  common::ActionState InvokeCallback(Param param, void *old_value, void *new_value,
                                     common::ManagedPointer<common::ActionContext> action_context);
};

}  // namespace terrier::settings
