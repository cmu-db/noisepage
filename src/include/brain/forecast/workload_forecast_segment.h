#pragma once

#include <queue>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "execution/exec_defs.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::brain {
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
  WorkloadForecastSegment(std::vector<execution::query_id_t> query_ids, std::vector<uint64_t> num_executions_);
  
  void Peek();

 private:
 
  /**
   * 
   */
  
  std::vector<execution::query_id_t> query_ids_;
  std::vector<uint64_t> num_executions_;
};

}  // namespace terrier::brain::forecast
