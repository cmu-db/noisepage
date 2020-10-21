#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/shared_latch.h"
#include "gflags/gflags.h"
#include "loggers/settings_logger.h"
#include "settings/settings_param.h"

namespace terrier::parser {
class ConstantValueExpression;
}

namespace terrier::settings {
using setter_callback_fn = void (*)(common::ManagedPointer<common::ActionContext> action_context);

/**
 * A wrapper for pg_settings table, does not store values in it.
 * Stores and triggers callbacks when a tunable parameter is changed.
 * Static class.
 */

class SettingsManager {
 public:
  DISALLOW_COPY_AND_MOVE(SettingsManager)

  /**
   * The constructor of settings manager
   * @param db_main a pointer to the DBMain
   * @param param_map constructed parameter map to take ownership of
   */
  explicit SettingsManager(common::ManagedPointer<DBMain> db_main,
                           std::unordered_map<settings::Param, settings::ParamInfo> &&param_map);

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
   * Get a copy of the default setting for a given parameter.
   * @param name setting name
   */
  const parser::ConstantValueExpression &GetDefault(const std::string &name);

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
   * Set the given parameter.
   * @param name The parameter name.
   * @param values The parameter's new value(s).
   */
  void SetParameter(const std::string &name,
                    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &values);

  /**
   * Construct settings param map from settings_defs.h
   * @param param_map
   */
  static void ConstructParamMap(
      std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> &param_map);  // NOLINT

 private:
  common::ManagedPointer<DBMain> db_main_;
  std::unordered_map<settings::Param, settings::ParamInfo> param_map_;
  std::unordered_map<std::string, settings::Param> param_name_map_;

  common::SharedLatch latch_;

  void ValidateSetting(Param param, const parser::ConstantValueExpression &min_value,
                       const parser::ConstantValueExpression &max_value);

  /** @return The Param corresponding to the given name; throws exception if doesn't exist. */
  Param GetParam(const std::string &name) const;
  /** @return The ParamInfo corresponding to the given parameter; throws exception if doesn't exist. */
  const ParamInfo &GetParamInfo(const settings::Param &param) const;

  parser::ConstantValueExpression &GetValue(Param param);
  bool SetValue(Param param, parser::ConstantValueExpression value);
  bool ValidateValue(const parser::ConstantValueExpression &value, const parser::ConstantValueExpression &min_value,
                     const parser::ConstantValueExpression &max_value);
  common::ActionState InvokeCallback(Param param, void *old_value, void *new_value,
                                     common::ManagedPointer<common::ActionContext> action_context);

  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}
};

}  // namespace terrier::settings
