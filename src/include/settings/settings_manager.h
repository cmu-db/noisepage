#pragma once

#include <unordered_map>
#include <catalog/settings_handle.h>
#include "type/value.h"
#include "common/exception.h"
#include "settings/settings_param.h"

namespace terrier::settings {

/*
 * SettingsManager:
 * A wrapper for pg_settings table, does not store values in it.
 * Stores and triggers callbacks when a tunable parameter is changed.
 * Static class.
*/

class SettingsManager {
 public:
  SettingsManager() = delete;
  SettingsManager(SettingsManager&) = delete;
  SettingsManager(std::shared_ptr<catalog::Catalog> catalog, transaction::TransactionManager *txn_manager);

  int32_t GetInt(Param param);
  double GetDouble(Param param);
  bool GetBool(Param param);
  std::string GetString(Param param);

  void SetInt(Param param, int32_t value);
  void SetBool(Param param, bool value);
  void SetString(Param param, const std::string &value);

  // Call this method in Catalog->Bootstrap
  // to store information into pg_settings
  void InitializeCatalog();

  const std::string GetInfo();

  void ShowInfo();

  void InitParams();

 private:
  catalog::SettingsHandle settings_handle_;
  transaction::TransactionManager *txn_manager_;
  std::unordered_map<Param, ParamInfo> param_map_;

  void DefineSetting(Param param, const std::string &name,
                     const type::Value &value,
                     const std::string &description,
                     const type::Value &default_value,
                     const type::Value &min_value,
                     const type::Value &max_value,
                     bool is_mutable, bool is_persistent);

  type::Value GetValue(Param param);
  void SetValue(Param param, const type::Value &value);


};

}  // namespace terrier::settings
