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
    std::unordered_map<execution::query_id_t, uint64_t> id_to_num_exec)
    : id_to_num_exec_(id_to_num_exec) {
}

void WorkloadForecastSegment::Peek() {
  std::cout << "size: " << id_to_num_exec_.size() << std::endl;
  for (auto it = id_to_num_exec_.begin(); it != id_to_num_exec_.end(); it++){
    std::cout << "qid: " << it->first << "; num_exec: " << it->second << std::endl;
  }
  std::cout << std::endl;
}

}  // namespace terrier::brain::forecast
