#include "loggers/main_logger.h"

int Foo(int x, int y) {
  LOG_INFO("hello world from a dummy file!");
  return x + y;
}
