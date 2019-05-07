#pragma once

#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "catalog/settings_handle.h"
#include "common/action_context.h"
#include "common/exception.h"
#include "loggers/settings_logger.h"
#include "settings/settings_param.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier {
class DBMain;
}

namespace terrier::settings {
using setter_callback_fn = void (*)(const std::shared_ptr<common::ActionContext> &action_context);

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
   * @param catalog a shared pointer to the system catalog
   * @param txn_manager a pointer to the transaction manager
   */
  SettingsManager(DBMain *db, catalog::Catalog *catalog, transaction::TransactionManager *txn_manager);

  /**
   * Get the value of an integer setting
   * @param param setting name
   * @return current setting value
   */
  int32_t GetInt(Param param);

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
  std::string_view GetString(Param param);

  /**
   * Set the value of an integer setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting an integer param
   * @param setter_callback callback from caller
   */
  void SetInt(Param param, int32_t value, std::shared_ptr<common::ActionContext> action_context,
              setter_callback_fn setter_callback);

  /**
   * Set the value of a double setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting a double param
   * @param setter_callback callback from caller
   */
  void SetDouble(Param param, double value, std::shared_ptr<common::ActionContext> action_context,
                 setter_callback_fn setter_callback);

  /**
   * Set the value of a boolean setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting a boolean param
   * @param setter_callback callback from caller
   */
  void SetBool(Param param, bool value, std::shared_ptr<common::ActionContext> action_context,
               setter_callback_fn setter_callback);

  /**
   * Set the value of a string setting
   * @param param setting name
   * @param value the new value
   * @param action_context action context for setting a string param
   * @param setter_callback callback from caller
   */
  void SetString(Param param, const std::string_view &value, std::shared_ptr<common::ActionContext> action_context,
                 setter_callback_fn setter_callback);

  // Call this method in Catalog->Bootstrap
  // to store information into pg_settings
  /**
   * Store initial value of settings into pg_settings
   */
  void InitializeCatalog();

  /**
   * Get current values of all the settings
   * @return the string consists of the values
   */
  const std::string GetInfo();

  /**
   * Print current values of all the settings
   */
  void ShowInfo();

  /**
   * Validate values from DBMain map
   */
  void ValidateParams();

 private:
  DBMain *db_;
  catalog::SettingsHandle settings_handle_;
  transaction::TransactionManager *txn_manager_;
  common::SharedLatch latch_;

  void ValidateSetting(Param param, const type::TransientValue &min_value, const type::TransientValue &max_value);

  type::TransientValue &GetValue(Param param);
  bool SetValue(Param param, const type::TransientValue &value);
  bool ValidateValue(const type::TransientValue &value, const type::TransientValue &min_value,
                     const type::TransientValue &max_value);
  common::ActionState InvokeCallback(Param param, void *old_value, void *new_value,
                                     std::shared_ptr<common::ActionContext> action_context);
};

}  // namespace terrier::settings
