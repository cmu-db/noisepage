//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.h
//
// Identification: src/include/common/init.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace terrier {

class ThreadPool;

extern ThreadPool thread_pool;

//===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

class TerrierInit {
 public:
  static void Initialize();

  static void Shutdown();

  static void SetUpThread();

  static void TearDownThread();
};

}  // namespace terrier
