//
// Created by pakhtar on 8/10/18.
//

#include "loggers/main_logger.h"

std::shared_ptr<spdlog::sinks::basic_file_sink_mt> default_sink;
std::shared_ptr<spdlog::logger> main_logger;

void init_main_logger()
{
  // create the default, shared sink
  default_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("log.txt");

  // the terrier, top level logger
  main_logger = std::make_shared<spdlog::logger>("main_logger", default_sink);
  spdlog::register_logger(main_logger);
}
