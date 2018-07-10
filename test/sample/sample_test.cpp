#include <iostream>

#include "common/harness.h"

class SampleTerrierTests : public TerrierTest {};

TEST_F(SampleTerrierTests, SampleTest) {
  std::cout << "Hello from a sample Google Test!" << std::endl;
}