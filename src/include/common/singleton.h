//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// singleton.h
//
// Identification: src/include/common/singleton.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "execution/codegen.h"

namespace terrier {

template <typename T>
class Singleton {
 public:
  static T &Instance() {
    static T instance;
    return instance;
  }

 protected:
  Singleton() {}
  ~Singleton() {}

 private:
  DISALLOW_COPY_AND_MOVE(Singleton);
};

}  // namespace terrier