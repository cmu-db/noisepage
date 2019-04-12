#pragma once

#include <catalog/settings_handle.h>
#include <gflags/gflags.h>
#include <unordered_map>
#include "catalog/settings_handle.h"
#include "common/exception.h"
#include "loggers/settings_logger.h"
#include "main/main_database.h"
#include "settings/settings_param.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

#define __SETTING_GFLAGS_DECLARE__
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DECLARE__

namespace terrier::settings {

using callback_fn = void (*)(void *, void *);

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
   * @param catalog a shared pointer to the system catalog
   * @param txn_manager a pointer to the transaction manager
   */
  SettingsManager(const std::shared_ptr<catalog::Catalog> &catalog, transaction::TransactionManager *txn_manager);

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
   */
  void SetInt(Param param, int32_t value);

  /**
   * Set the value of a double setting
   * @param param setting name
   * @param value the new value
   */
  void SetDouble(Param param, double value);

  /**
   * Set the value of a boolean setting
   * @param param setting name
   * @param value the new value
   */
  void SetBool(Param param, bool value);

  /**
   * Set the value of a string setting
   * @param param setting name
   * @param value the new value
   */
  void SetString(Param param, const std::string_view &value);

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
   * Migrate values from GFlags to internal map
   */
  void InitParams();

 private:
  catalog::SettingsHandle settings_handle_;
  transaction::TransactionManager *txn_manager_;
  std::unordered_map<Param, ParamInfo> param_map_;
  std::unordered_map<Param, callback_fn> callback_map_;

  void DefineSetting(Param param, const std::string &name, const type::TransientValue &value,
                     const std::string &description, const type::TransientValue &default_value,
                     const type::TransientValue &min_value, const type::TransientValue &max_value, bool is_mutable,
                     callback_fn callback = nullptr);

  type::TransientValue GetValue(Param param);
  void SetValue(Param param, const type::TransientValue &value);
  bool ValidateValue(const type::TransientValue &value, const type::TransientValue &min_value,
                     const type::TransientValue &max_value);
};

}  // namespace terrier::settings
