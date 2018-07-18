//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// statistics.cpp
//
// Identification: src/common/statistics.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <iostream>
#include "common/statistics.h"

namespace terrier {

void Statistics::PrintStats (){
  SetStats();
  std::cout << "The Json value about the statistics is shown below: " << std::endl;
//  std::cout << json_value_ << std::endl << std::endl;
}

}  // namespace terrier
