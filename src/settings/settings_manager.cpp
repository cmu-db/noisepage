#include <gflags/gflags.h>
#include "settings/settings_manager.h"
#include "type/value_factory.h"

// This will expand to define all the settings defined in settings.h
// using GFlag's DEFINE_...() macro. See settings_macro.h.
#define __SETTING_GFLAGS_DEFINE__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_GFLAGS_DEFINE__

namespace terrier::settings {

// Used for building temporary transactions
void EmptyCallback(void * /*unused*/) {}

SettingsManager::SettingsManager(std::shared_ptr<catalog::Catalog> catalog,
                                 transaction::TransactionManager *txn_manager) :
  settings_handle_(catalog->GetSettingsHandle()), txn_manager_(txn_manager) {
  static int instance_number = 0;
  instance_number++;

  InitParams();
}

void SettingsManager::InitParams() {
  // This will expand to invoke settings_manager::DefineSetting on
  // all of the settings defined in settings.h. See settings_macro.h.
  #define __SETTING_DEFINE__
  #include "settings/settings_macro.h"
  #include "settings/settings.h"
  #undef __SETTING_DEFINE__
}

void SettingsManager::DefineSetting(Param param, const std::string &name,
                                    const type::Value &value,
                                    const std::string &description,
                                    const type::Value &default_value,
                                    const type::Value &min_value,
                                    const type::Value &max_value,
                                    bool is_mutable, bool is_persistent) {
  /*
  if (settings_.find(param) != settings_.end()) {
    throw SettingsException("settings " + name + " already exists");
  }

  // Only below types support min-max bound checking
  if (value.GetTypeId() == type::TypeId::INTEGER ||
      value.GetTypeId() == type::TypeId::SMALLINT ||
      value.GetTypeId() == type::TypeId::TINYINT ||
      value.GetTypeId() == type::TypeId::DECIMAL) {
    if (!value.CompareBetweenInclusive(min_value, max_value))
      throw SettingsException("Value given for \"" + name +
          "\" is not in its min-max bounds (" +
          min_value.ToString() + "-" +
          max_value.ToString() + ")");
  }
   */
  param_map_.emplace(param, ParamInfo(name, value, description, default_value, is_mutable, is_persistent));

}

void SettingsManager::InitializeCatalog() {

  auto txn = txn_manager_->BeginTransaction();
  auto column_num = catalog::SettingsHandle::schema_cols_.size();

  using Index = catalog::SettingsTableColumn;
  using ValueFactory = type::ValueFactory;

  for(auto pair : param_map_)
  {
    Param param = pair.first;
    ParamInfo info = pair.second;

    catalog::settings_oid_t oid(static_cast<uint32_t>(param));
    std::vector<type::Value> entry(column_num, type::ValueFactory::GetNullValue(type::TypeId::VARCHAR));

    entry[static_cast<int>(Index::OID)] = ValueFactory::GetIntegerValue(!oid);
    entry[static_cast<int>(Index::NAME)] = ValueFactory::GetVarcharValue(info.name.c_str());
    entry[static_cast<int>(Index::SHORT_DESC)] = ValueFactory::GetVarcharValue(info.desc.c_str());

    settings_handle_.InsertRow(txn, entry);
  }

  txn_manager_->Commit(txn, EmptyCallback, nullptr);
}

int32_t SettingsManager::GetInt(Param param) {
  return 0;
}

int16_t SettingsManager::GetSmallInt(Param param)
{
  return 0;
}

double SettingsManager::GetDouble(Param param) {
  //TODO
  return 0.0;
}

bool SettingsManager::GetBool(Param param) {
  //TODO
  return false;
}

void SettingsManager::SetInt(Param param, int32_t value) {
  SetValue(param, type::ValueFactory::GetIntegerValue(value));
}

void SettingsManager::SetBool(Param param, bool value) {
  SetValue(param, type::ValueFactory::GetBooleanValue(value));
}

void SettingsManager::SetString(Param param, const std::string &value) {
  SetValue(param, type::ValueFactory::GetVarcharValue(value.c_str()));
}

std::string SettingsManager::GetString(Param param) {
  //TODO
  return "";
}



const std::string SettingsManager::GetInfo() {
  /*
  const uint32_t box_width = 72;
  const std::string title = "PELOTON SETTINGS";

  std::string info;
  info.append(StringUtil::Format("%*s\n", box_width / 2 + title.length() / 2,
                                 title.c_str()));
  info.append(StringUtil::Repeat("=", box_width)).append("\n");

  // clang-format off
  info.append(StringUtil::Format("%34s:   %-34i\n", "Port", GetInt(Param::port)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Socket Family", GetString(Param::socket_family).c_str()));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Statistics", GetInt(Param::stats_mode) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Max Connections", GetInt(Param::max_connections)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Index Tuner", GetBool(Param::index_tuner) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Layout Tuner", GetBool(Param::layout_tuner) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   (queue size %i, %i threads)\n", "Worker Pool", GetInt(Param::monoqueue_task_queue_size), GetInt(Param::monoqueue_worker_pool_size)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Parallel Query Execution", GetBool(Param::parallel_execution) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Min. Parallel Table Scan Size", GetInt(Param::min_parallel_table_scan_size)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Code-generation", GetBool(Param::codegen) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Print IR Statistics", GetBool(Param::print_ir_stats) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Dump IR", GetBool(Param::dump_ir) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Optimization Timeout", GetInt(Param::task_execution_timeout)));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Number of GC threads", GetInt(Param::gc_num_threads)));
  // clang-format on

  return StringBoxUtil::Box(info);
   */
  return "";
}

void SettingsManager::ShowInfo() { /*LOG_INFO("\n%s\n", GetInfo().c_str());*/ }

type::Value SettingsManager::GetValue(Param param) {
  /*
  // TODO: Look up the value from catalog
  // Because querying a catalog table needs to create a new transaction and
  // creating transaction needs to get setting values,
  // it will be a infinite recursion here.

  auto param = settings_.find(param);
  return param->second.value;
   */

  return type::ValueFactory::GetNullValue(type::TypeId::VARCHAR);
}

void SettingsManager::SetValue(Param param, const type::Value &value) {
  // TODO: Need update API in settings handle; otherwise we need to use delete + insert
}




}  // namespace terrier::settings
