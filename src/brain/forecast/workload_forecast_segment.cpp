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

namespace terrier::brain {

WorkloadForecastSegment::WorkloadForecastSegment(
    std::vector<execution::query_id_t> query_ids,
    std::vector<const uint64_t> num_executions)
    : query_ids(query_ids_),
      num_executions(num_executions_) {
}


}  // namespace terrier::brain::forecast
