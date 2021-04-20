#include "self_driving/forecasting/workload_forecast_segment.h"

#include "execution/exec_defs.h"

namespace noisepage::selfdriving {

WorkloadForecastSegment::WorkloadForecastSegment(std::unordered_map<execution::query_id_t, double> id_to_num_exec)
    : id_to_num_exec_(std::move(id_to_num_exec)) {}

}  // namespace noisepage::selfdriving
