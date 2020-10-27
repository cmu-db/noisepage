#include "brain/forecast/workload_forecast_segment.h"

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "execution/exec_defs.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::brain {

WorkloadForecastSegment::WorkloadForecastSegment(
    std::vector<execution::query_id_t> query_ids,
    std::vector<uint64_t> num_executions)
    : query_ids_(query_ids),
      num_executions_(num_executions) {
}

void WorkloadForecastSegment::Peek() {
  std::cout << "size: " << query_ids_.size() << std::endl;
  for (auto i = 0; i < query_ids_.size(); i++){
    std::cout << "qid: " << query_ids_[i] << "; num_exec: " << num_executions_[i] << std::endl;
  }
  std::cout << std::endl;
}

}  // namespace terrier::brain::forecast
