//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger.cpp
//
// Identification: src/common/logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"

// #include "common/internal_types.h"
#include "easylogging++/easylogging++.h"
// #include "settings/settings_manager.h"

// Initialize for Easylogging++ library
INITIALIZE_EASYLOGGINGPP

namespace terrier {

/* @brief   Initialize Peloton logger to set Easylogging++ according to settings
 */
void Logger::InitializeLogger() {
  el::Configurations logger_conf;
  logger_conf.setToDefault();

  // log flush threshold
  logger_conf.set(el::Level::Global,
                  el::ConfigurationType::LogFlushThreshold,
                  LOG_FLUSH_THERESHOLD);

  // log format setting
  logger_conf.set(el::Level::Global,
                  el::ConfigurationType::Format,
                  LOG_FORMAT);

  /*
   * ToDo: Need settings_manager and internal_types to set log level on booting
   *

  // standard output setting
  logger_conf.set(el::Level::Global,
                  el::ConfigurationType::ToStandardOutput,
                  settings::SettingsManager::GetString(
                      settings::SettingId::log_to_standard_output));

  // file output setting
  logger_conf.set(el::Level::Global,
                  el::ConfigurationType::ToFile,
                  settings::SettingsManager::GetString(
                      settings::SettingId::log_to_file));

  if (settings::SettingsManager::GetBool(settings::SettingId::log_to_file)) {
    // file path setting
    logger_conf.set(el::Level::Global,
                    el::ConfigurationType::Filename,
                    settings::SettingsManager::GetString(
                        settings::SettingId::log_file_path));
  }

  // log level setting for Easylogging++ by log_level setting parameter in Peloton
  auto log_level = StringToPelotonLogLevel(
      settings::SettingsManager::GetString(settings::SettingId::log_level));
  switch (log_level) {
    case PelotonLogLevel::ERROR:
      logger_conf.set(el::Level::Warning,
                      el::ConfigurationType::Enabled,
                      "false");
      // no break
    case PelotonLogLevel::WARN:
      logger_conf.set(el::Level::Info,
                      el::ConfigurationType::Enabled,
                      "false");
      // no break
    case PelotonLogLevel::INFO:
      logger_conf.set(el::Level::Debug,
                      el::ConfigurationType::Enabled,
                      "false");
      // no break
    case PelotonLogLevel::DEBUG:
      logger_conf.set(el::Level::Trace,
                      el::ConfigurationType::Enabled,
                      "false");
      // no break
    case PelotonLogLevel::TRACE:
      // enable all log levels
      break;
    default: {
      throw Exception(StringUtil::Format(
          "Invalid Peloton log level appears '%s'",
          PelotonLogLevelToString(log_level).c_str()));
    }
  }
	*/

  // apply the logger configuration
  el::Loggers::reconfigureLogger(LOGGER_NAME, logger_conf);
}

}  // namespace terrier
