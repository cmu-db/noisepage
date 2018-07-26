#include <iostream>
#include "common/typedefs.h"
#include "common/logger.h"
int main() {
  terrier::Logger::InitializeLogger();
  LOG_INFO("hello world!");
}
