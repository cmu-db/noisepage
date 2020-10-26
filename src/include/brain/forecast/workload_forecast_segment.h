#pragma once

#include <queue>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "parser/expression/constant_value_expression.h"

namespace terrier::brain {
/**
 * 
 */
class WorkloadForecastSegment {
 public:
  /**
   * Constructor for the .
   * @param query_ids
   * @param num_executions_ the 
   */
  WorkloadForecastSegment(std::vector<const uint64_t> query_ids, std::vector<const uint64_t> num_executions_);


 private:
 
  /**
   * 
   */
  
  std::vector<const uint64_t> query_ids_;
  std::vector<const uint64_t> num_executions_;
};

}  // namespace terrier::brain::forecast
