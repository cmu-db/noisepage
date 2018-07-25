//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// statistics.h
//
// Identification: src/include/statistics/statistics.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <tbb/concurrent_unordered_map.h>
#include "common/macros.h"

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier {
namespace statistics {

/**
 *  Basic template class for performance counters to collect and print statistics
 *  of classes. Actual counters will be implemented as a template specialization.
 *
 *  All member functions in this class do not anything to disable all for
 *  Release mode. To collect and print statistics for each class, it is necessary
 *  to specialize the template class and to implement functions with
 *  '#ifndef NDEBUG' macro. See block_store_pc.h as the sample implementation.
 *
 *  @tparam T the type of counting performance stuff
 **/

template <typename T>
class PerformanceCounters {
 public:
  PerformanceCounters() {};

  /** @brief increment counter by counter name
   *  @param name  Counter name to be incremented
   */
  void IncrementCounter(UNUSED_ATTRIBUTE std::string name) {};

  /** @brief decrement counter by counter name
   *  @param name  Counter name to be decremented
   */
  void DecrementCounter(UNUSED_ATTRIBUTE std::string name) {};

  /** @brief Add value to a counter
   *  @param name  Counter name to be added
   *  @param value  value to add to the counter
   */
  void AddCounter(UNUSED_ATTRIBUTE std::string name,
                  UNUSED_ATTRIBUTE int value) {};

  /** @brief Subtract value from a counter
   *  @param name  Counter name to be subtracted
   *  @param value  value to subtract from the counter
   */
  void SubCounter(UNUSED_ATTRIBUTE std::string name,
                  UNUSED_ATTRIBUTE int value) {};

  // Add functions below in case to collect more various statistics
  // in a specialized template class.


  /** @brief Print the statistics in the Json value. */
  void PrintPerformanceCounters() {};
};

}  // namespace statistics
}  // namespace terrier
