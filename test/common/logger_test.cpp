//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_test.cpp
//
// Identification: test/common/logger_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"
#include "common/logger.h"
// #include "settings/settings_manager.h"

namespace terrier {

//===--------------------------------------------------------------------===//
// Logger Tests
//===--------------------------------------------------------------------===//

class LoggerTests : public ::testing::Test {};

// ToDo:: Need settings_manager and internal_types to test all settings
TEST_F(LoggerTests, StandardOutputTest) {
  LOG_ERROR("error message to standard output");
  LOG_WARN("warning message to standard output");
  LOG_TRACE("trace message to standard output");
  LOG_INFO("info message to standard output");
  LOG_DEBUG("debug message to standard output");

//  std::ifstream ifs(
//      settings::SettingsManager::GetString(
//          settings::SettingId::log_file_path));
//  EXPECT_FALSE(ifs.is_open());
}

/*
TEST_F(LoggerTests, LogFileTest) {
  settings::SettingsManager::SetBool(settings::SettingId::log_to_standard_output,
                                     false);
  settings::SettingsManager::SetBool(settings::SettingId::log_to_file,
                                     true);
  settings::SettingsManager::SetString(settings::SettingId::log_file_path,
                                       "./log_test.log");
  Logger::InitializeLogger();

  LOG_ERROR("error message to log file");
  LOG_WARN("warning message to log file");
  LOG_TRACE("trace message to log file");
  LOG_INFO("info message to log file");
  LOG_DEBUG("debug message to log file");

  std::ifstream ifs("./log_test.log");
  EXPECT_TRUE(ifs.is_open());
  std::string str;
  int count = 0;
  while(getline(ifs, str)) count++;
#if LOG_LEVEL == LOG_LEVEL_OFF
  EXPECT_EQ(0, count);
#elif LOG_LEVEL == LOG_LEVEL_ERROR
  EXPECT_EQ(1, count);
#elif LOG_LEVEL == LOG_LEVEL_WARN
  EXPECT_EQ(2, count);
#elif LOG_LEVEL == LOG_LEVEL_INFO
  EXPECT_EQ(3, count);
#elif LOG_LEVEL == LOG_LEVEL_DEBUG
  EXPECT_EQ(4, count);
#elif LOG_LEVEL == LOG_LEVEL_TRACE
  EXPECT_EQ(5, count);
#elif LOG_LEVEL == LOG_LEVEL_ALL
  EXPECT_EQ(5, count);
#else
  EXPECT_TRUE(false);
#endif

  ifs.close();
  std::remove("./log_test.log");
}

TEST_F(LoggerTests, LogLevelTest) {
  std::ifstream ifs;
  std::string str, log_file_name;
  int count;

  // ERROR
#if LOG_LEVEL <= LOG_LEVEL_ERROR
  log_file_name = "./error_log_test.log";
  settings::SettingsManager::SetString(settings::SettingId::log_level,
                                       "ERROR");
  settings::SettingsManager::SetString(settings::SettingId::log_file_path,
                                       log_file_name);
  Logger::InitializeLogger();

  LOG_ERROR("error message to log file");
  LOG_WARN("warning message to log file");
  LOG_TRACE("trace message to log file");
  LOG_INFO("info message to log file");
  LOG_DEBUG("debug message to log file");

  ifs.open(log_file_name);
  EXPECT_TRUE(ifs.is_open());
  count = 0;
  while(getline(ifs, str)) count++;
  EXPECT_EQ(1, count);
  ifs.close();
  std::remove(log_file_name.c_str());
#endif

  // WARN
#if LOG_LEVEL <= LOG_LEVEL_WARN
  log_file_name = "./warn_log_test.log";
  settings::SettingsManager::SetString(settings::SettingId::log_level,
                                       "WARN");
  settings::SettingsManager::SetString(settings::SettingId::log_file_path,
                                       log_file_name);
  Logger::InitializeLogger();

  LOG_ERROR("error message to log file");
  LOG_WARN("warning message to log file");
  LOG_TRACE("trace message to log file");
  LOG_INFO("info message to log file");
  LOG_DEBUG("debug message to log file");

  ifs.open(log_file_name);
  EXPECT_TRUE(ifs.is_open());
  count = 0;
  while(std::getline(ifs, str)) count++;
  EXPECT_EQ(2, count);
  ifs.close();
  std::remove(log_file_name.c_str());
#endif

  // INFO
#if LOG_LEVEL <= LOG_LEVEL_INFO
  log_file_name = "./info_log_test.log";
  settings::SettingsManager::SetString(settings::SettingId::log_level,
                                       "INFO");
  settings::SettingsManager::SetString(settings::SettingId::log_file_path,
                                       log_file_name);
  Logger::InitializeLogger();

  LOG_ERROR("error message to log file");
  LOG_WARN("warning message to log file");
  LOG_TRACE("trace message to log file");
  LOG_INFO("info message to log file");
  LOG_DEBUG("debug message to log file");

  ifs.open(log_file_name);
  EXPECT_TRUE(ifs.is_open());
  count = 0;
  while(getline(ifs, str)) count++;
  EXPECT_EQ(3, count);
  ifs.close();
  std::remove(log_file_name.c_str());
#endif

  // DEBUG
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
  log_file_name = "./debug_log_test.log";
  settings::SettingsManager::SetString(settings::SettingId::log_level,
                                       "DEBUG");
  settings::SettingsManager::SetString(settings::SettingId::log_file_path,
                                       log_file_name);
  Logger::InitializeLogger();

  LOG_ERROR("error message to log file");
  LOG_WARN("warning message to log file");
  LOG_TRACE("trace message to log file");
  LOG_INFO("info message to log file");
  LOG_DEBUG("debug message to log file");

  ifs.open(log_file_name);
  EXPECT_TRUE(ifs.is_open());
  count = 0;
  while(getline(ifs, str)) count++;
  EXPECT_EQ(4, count);
  ifs.close();
  std::remove(log_file_name.c_str());
#endif

  // TRACE
#if LOG_LEVEL <= LOG_LEVEL_TRACE
  log_file_name = "./trace_log_test.log";
  settings::SettingsManager::SetString(settings::SettingId::log_level,
                                       "TRACE");
  settings::SettingsManager::SetString(settings::SettingId::log_file_path,
                                       log_file_name);
  Logger::InitializeLogger();

  LOG_ERROR("error message to log file");
  LOG_WARN("warning message to log file");
  LOG_TRACE("trace message to log file");
  LOG_INFO("info message to log file");
  LOG_DEBUG("debug message to log file");

  ifs.open(log_file_name);
  EXPECT_TRUE(ifs.is_open());
  count = 0;
  while(getline(ifs, str)) count++;
  EXPECT_EQ(5, count);
  ifs.close();
  std::remove(log_file_name.c_str());
#endif

}
*/
}  // namespace terrier
