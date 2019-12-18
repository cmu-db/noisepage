#pragma once
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "loggers/loggers_util.h"

namespace terrier {

class TerrierTest : public ::testing::Test {
 private:
  terrier::LoggersHandle loggers_handle_;
};

}  // namespace terrier
