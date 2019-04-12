#include <gflags/gflags.h>
#include <memory>
#include <string>
#include <vector>

#include "settings/settings_manager.h"
#include "type/transient_value_factory.h"
// This will expand to define all the settings defined in settings.h
// using GFlag's DEFINE_...() macro. See settings_common.h.
#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier::settings {

using Index = catalog::SettingsTableColumn;
using ValueFactory = type::TransientValueFactory;
using ValuePeeker = type::TransientValuePeeker;

// Used for building temporary transactions
void EmptyCallback(void * /*unused*/) {}

SettingsManager::SettingsManager(const std::shared_ptr<catalog::Catalog> &catalog,
                                 transaction::TransactionManager *txn_manager)
    : settings_handle_(catalog->GetSettingsHandle()), txn_manager_(txn_manager) {
  InitParams();
  InitializeCatalog();
}

void SettingsManager::InitParams() {
// This will expand to invoke settings_manager::DefineSetting on
// all of the settings defined in settings.h. See settings_common.h.
#define __SETTING_DEFINE__
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_DEFINE__
}

void SettingsManager::DefineSetting(Param param, const std::string &name, const type::TransientValue &value,
                                    const std::string &description, const type::TransientValue &default_value,
                                    const type::TransientValue &min_value, const type::TransientValue &max_value,
                                    bool is_mutable, callback_fn callback) {
  if (!ValidateValue(value, min_value, max_value)) {
    SETTINGS_LOG_ERROR(
        "Value given for \"{}"
        "\" is not in its min-max bounds ({}-{})",
        name, ValuePeeker::PeekVarChar(min_value), ValuePeeker::PeekVarChar(max_value));
    throw SETTINGS_EXCEPTION("Invalid setting value");
  }

  param_map_.emplace(param, ParamInfo(name, ValueFactory::GetCopy(value), description,
                                      ValueFactory::GetCopy(default_value), is_mutable));
  callback_map_.emplace(param, callback);
}

void SettingsManager::InitializeCatalog() {
  auto txn = txn_manager_->BeginTransaction();
  auto column_num = catalog::SettingsHandle::schema_cols_.size();

  for (const auto &pair : param_map_) {
    const Param param = pair.first;
    const ParamInfo &info = pair.second;

    catalog::settings_oid_t oid(static_cast<uint32_t>(param));
    std::vector<type::TransientValue> entry;
    for (auto i = column_num; i > 0; --i) {
      // NOLINTNEXTLINE
      entry.emplace_back(ValueFactory::GetNull(type::TypeId::VARCHAR));
    }

    entry[static_cast<int>(Index::OID)] = ValueFactory::GetInteger(!oid);
    entry[static_cast<int>(Index::NAME)] = ValueFactory::GetVarChar(info.name.c_str());
    entry[static_cast<int>(Index::SHORT_DESC)] = ValueFactory::GetVarChar(info.desc.c_str());

    settings_handle_.InsertRow(txn, entry);
  }

  txn_manager_->Commit(txn, EmptyCallback, nullptr);
  delete txn;
}

int32_t SettingsManager::GetInt(Param param) { return ValuePeeker::PeekInteger(GetValue(param)); }

double SettingsManager::GetDouble(Param param) { return ValuePeeker::PeekDecimal(GetValue(param)); }

bool SettingsManager::GetBool(Param param) { return ValuePeeker::PeekBoolean(GetValue(param)); }

std::string_view SettingsManager::GetString(Param param) { return ValuePeeker::PeekVarChar(GetValue(param)); }

void SettingsManager::SetInt(Param param, int32_t value) {
  int old_value = GetInt(param);
  SetValue(param, ValueFactory::GetInteger(value));
  callback_fn callback = callback_map_.find(param)->second;
  callback(static_cast<void *>(&old_value), static_cast<void *>(&value));
}

void SettingsManager::SetDouble(Param param, double value) {
  double old_value = GetDouble(param);
  SetValue(param, ValueFactory::GetDecimal(value));
  callback_fn callback = callback_map_.find(param)->second;
  callback(static_cast<void *>(&old_value), static_cast<void *>(&value));
}

void SettingsManager::SetBool(Param param, bool value) {
  bool old_value = GetBool(param);
  SetValue(param, ValueFactory::GetBoolean(value));
  callback_fn callback = callback_map_.find(param)->second;
  callback(static_cast<void *>(&old_value), static_cast<void *>(&value));
}

void SettingsManager::SetString(Param param, const std::string_view &value) {
  std::string_view old_value = GetString(param);
  SetValue(param, ValueFactory::GetVarChar(value));
  callback_fn callback = callback_map_.find(param)->second;
  std::string_view new_value(value);
  callback(static_cast<void *>(&old_value), static_cast<void *>(&new_value));
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
  info.append(StringUtil::Format("%34s:   %-34s\n", "Index Tuner", GetBool(Param::index_tuner) ? "enabled" :
  "disabled")); info.append(StringUtil::Format("%34s:   %-34s\n", "Layout Tuner", GetBool(Param::layout_tuner) ?
  "enabled" : "disabled")); info.append(StringUtil::Format("%34s:   (queue size %i, %i threads)\n", "Worker Pool",
  GetInt(Param::monoqueue_task_queue_size), GetInt(Param::monoqueue_worker_pool_size)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Parallel Query Execution", GetBool(Param::parallel_execution) ?
  "enabled" : "disabled")); info.append(StringUtil::Format("%34s:   %-34i\n", "Min. Parallel Table Scan Size",
  GetInt(Param::min_parallel_table_scan_size))); info.append(StringUtil::Format("%34s:   %-34s\n", "Code-generation",
  GetBool(Param::codegen) ? "enabled" : "disabled")); info.append(StringUtil::Format("%34s:   %-34s\n", "Print IR
  Statistics", GetBool(Param::print_ir_stats) ? "enabled" : "disabled")); info.append(StringUtil::Format("%34s:
  %-34s\n", "Dump IR", GetBool(Param::dump_ir) ? "enabled" : "disabled")); info.append(StringUtil::Format("%34s:
  %-34i\n", "Optimization Timeout", GetInt(Param::task_execution_timeout))); info.append(StringUtil::Format("%34s:
  %-34i\n", "Number of GC threads", GetInt(Param::gc_num_threads)));
  // clang-format on

  return StringBoxUtil::Box(info);
   */
  return "";
}

void SettingsManager::ShowInfo() { /*LOG_INFO("\n%s\n", GetInfo().c_str());*/
}

type::TransientValue SettingsManager::GetValue(Param param) {
  auto &param_info = param_map_.find(param)->second;
  return ValueFactory::GetCopy(param_info.value);
}

void SettingsManager::SetValue(Param param, const type::TransientValue &value) {
  auto &param_info = param_map_.find(param)->second;

  if (!param_info.is_mutable) throw SETTINGS_EXCEPTION((param_info.name + " is not mutable.").c_str());

  param_info.value = ValueFactory::GetCopy(value);

  auto txn = txn_manager_->BeginTransaction();
  auto entry = settings_handle_.GetSettingsEntry(txn, param_info.name);
  entry->SetColumn(static_cast<int32_t>(Index::SETTING), value);
  txn_manager_->Commit(txn, EmptyCallback, nullptr);
  delete txn;
}

bool SettingsManager::ValidateValue(const type::TransientValue &value, const type::TransientValue &min_value,
                                    const type::TransientValue &max_value) {
  switch (value.Type()) {
    case type::TypeId::INTEGER:
      return ValuePeeker::PeekInteger(value) >= ValuePeeker::PeekInteger(min_value) &&
             ValuePeeker::PeekInteger(value) <= ValuePeeker::PeekInteger(max_value);
    case type::TypeId ::DECIMAL:
      return ValuePeeker::PeekDecimal(value) >= ValuePeeker::PeekDecimal(min_value) &&
             ValuePeeker::PeekDecimal(value) <= ValuePeeker::PeekDecimal(max_value);
    default:
      return true;
  }
}

}  // namespace terrier::settings
