#include "main/db_main.h"

#include <memory>
#include <unordered_map>
#include <utility>

#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier {

DBMain::DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : param_map_(std::move(param_map)) {
  LoggersUtil::Initialize(false);
}

void DBMain::Run() {
  running_ = true;
  network_layer_->GetServer()->SetPort(static_cast<uint16_t>(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::port)->second.value_)));
  network_layer_->GetServer()->RunServer();

  {
    std::unique_lock<std::mutex> lock(network_layer_->GetServer()->RunningMutex());
    network_layer_->GetServer()->RunningCV().wait(lock, [=] { return !(network_layer_->GetServer()->Running()); });
  }
}

void DBMain::ForceShutdown() {
  if (running_) {
    network_layer_->GetServer()->StopServer();
  }
}

DBMain::~DBMain() {
  ForceShutdown();
  LoggersUtil::ShutDown();
}

}  // namespace terrier
