#pragma once

#include <queue>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "execution/exec_defs.h"

namespace noisepage::selfdriving {

/**
 * Contains query ids and number of executions for each query for queries predicted to be in this time interval
 */
class WorkloadForecastSegment {
 public:
  /**
   * Constructor for WorkloadForecastSegment
   * @param id_to_num_exec Map from qids to number of execution of this query in this interval
   */
  explicit WorkloadForecastSegment(std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec);

  /**
   * Return const reference to id_to_num_exec_
   */
  const std::unordered_map<execution::query_id_t, uint64_t> &GetIdToNumexec() const { return id_to_num_exec_; }

 private:
  std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec_;
};

}  // namespace noisepage::selfdriving
