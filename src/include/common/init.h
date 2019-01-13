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

/**
 * Class used to initialize and tear down the Terrier network server
 */
class TerrierInit {
 public:

  /**
   * @brief Initializes the Terrier server
   */
  static void Initialize();

  /**
   * @brief Shuts down the Terrier server
   */
  static void Shutdown();

};

}  // namespace terrier
