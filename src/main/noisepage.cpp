#include <csignal>
#include <unordered_map>
#include <utility>

#include "common/managed_pointer.h"
#include "gflags/gflags.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "settings/settings_manager.h"

/**
 * Need a global pointer to access from SignalHandler, unfortunately. Do not remove from this anonymous namespace since
 * the pointer is meant only for the signal handler. If you think you need a global pointer to db_main somewhere else in
 * the system, you're probably doing something wrong.
 */
namespace {
noisepage::common::ManagedPointer<noisepage::DBMain> db_main_handler_ptr = nullptr;
}

/**
 * The signal handler to be invoked for SIGINT and SIGTERM
 * @param sig_num portable signal number passed to the handler by the kernel
 */
void SignalHandler(int32_t sig_num) {
  if ((sig_num == SIGINT || sig_num == SIGTERM) && db_main_handler_ptr != nullptr) {
    db_main_handler_ptr->ForceShutdown();
  }
}

/**
 * Register SignalHandler for SIGINT and SIGTERM
 * @return 0 if successful, otherwise errno
 */
int32_t RegisterSignalHandler() {
  // Initialize a signal handler to call SignalHandler()
  struct sigaction sa;  // NOLINT
  sa.sa_handler = &SignalHandler;
  sa.sa_flags = SA_RESTART;

  sigfillset(&sa.sa_mask);

  // Terminal interrupt signal (usually from ^c, portable number is 2)
  if (sigaction(SIGINT, &sa, nullptr) == -1) {
    return errno;
  }

  // Terminate signal from administrator (portable number is 15)
  if (sigaction(SIGTERM, &sa, nullptr) == -1) {
    return errno;
  }

  return 0;
}

int main(int argc, char *argv[]) {
  // Register signal handler so we can kill the server once it's running
  const auto register_result = RegisterSignalHandler();
  if (register_result != 0) {
    return register_result;
  }

  // Parse Setting Values
  ::gflags::SetUsageMessage("Usage Info: \n");
  ::gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize debug loggers
  noisepage::LoggersUtil::Initialize();

  // Generate Settings Manager map
  std::unordered_map<noisepage::settings::Param, noisepage::settings::ParamInfo> param_map;
  noisepage::settings::SettingsManager::ConstructParamMap(param_map);

  auto db_main = noisepage::DBMain::Builder()
                     .SetSettingsParameterMap(std::move(param_map))
                     .SetUseSettingsManager(true)
                     .SetUseGC(true)
                     .SetUseCatalog(true)
                     .SetUseGCThread(true)
                     .SetUseStatsStorage(true)
                     .SetUseExecution(true)
                     .SetUseTrafficCop(true)
                     .SetUseNetwork(true)
                     .SetUseMessenger(true)
                     .Build();

  db_main_handler_ptr = db_main.get();

  db_main->Run();

  noisepage::LoggersUtil::ShutDown();
  return 0;
}
