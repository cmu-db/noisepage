#pragma once

#include <queue>
#include <tuple>
#include <unordered_map>
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
  explicit WorkloadForecastSegment(std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec);

  std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec_;

 private:
  /**
   *
   */
};

}  // namespace noisepage::brain
