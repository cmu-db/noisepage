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
  WorkloadForecastSegment(std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec);
  
  void Peek();

  std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec_;

 private:
 
  /**
   * 
   */
};

}  // namespace terrier::brain::forecast
