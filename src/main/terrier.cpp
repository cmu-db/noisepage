#include <iostream>
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "storage/garbage_collector.h"

int main() {
  // initialize loggers
  try {
    init_main_logger();
    // initialize namespace specific loggers
    ::terrier::storage::init_storage_logger();

    // Flush all *registered* loggers using a worker thread.
    // Registered loggers must be thread safe for this to work correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  }
  catch (const spdlog::spdlog_ex &ex) {
    std::cout << "debug log init failed " << ex.what() << std::endl;
    return 1;
  }

  // log init now complete
  LOG_INFO("woof!");
  std::cout << "hello world!" << std::endl;

  // shutdown loggers
  spdlog::shutdown();
}
