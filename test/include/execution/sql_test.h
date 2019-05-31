#pragma once

#include "gtest/gtest.h"

#include "execution/tpl_test.h"  // NOLINT

#include "execution/sql/execution_structures.h"

namespace tpl {

class SqlBasedTest : public TplTest {
 protected:
  void SetUp() override {
    TplTest::SetUp();
    sql::ExecutionStructures::Instance();
  }
};

}  // namespace tpl
