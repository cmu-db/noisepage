#include "main/db_main.h"

namespace terrier {

DBMain::DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : param_map_(std::move(param_map)) {}

void DBMain::Run() {
  running_ = true;
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
