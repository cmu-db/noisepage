#pragma once

#include <queue>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <unordered_set>

#include "execution/exec_defs.h"

namespace noisepage::selfdriving {
class PilotUtil;

namespace pilot {
class MonteCarloSearchTree;
}

/**
 * Contains query ids and number of executions for each query for queries predicted to be in this time interval
 */
class WorkloadForecastSegment {
 public:
  /**
   * Constructor for WorkloadForecastSegment
   * @param id_to_num_exec Map from qids to number of execution of this query in this interval
   */
  explicit WorkloadForecastSegment(std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec,
                                   std::vector<uint64_t> db_oids);

  const std::vector<uint64_t> &GetDBOids() { return db_oids_; }

 private:
  std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec_;
  std::vector<uint64_t> db_oids_;
  friend class PilotUtil;
};

}  // namespace noisepage::selfdriving
