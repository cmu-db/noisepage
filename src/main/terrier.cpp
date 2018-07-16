#include <iostream>
#include "common/common_defs.h"
#include "common/logger.h"
int main() {
  terrier::Logger::InitializeLogger();
  LOG_INFO("hello world!");
}
