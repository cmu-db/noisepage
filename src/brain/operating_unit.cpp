#include "brain/operating_unit.h"

namespace terrier::brain {

std::atomic<execution::feature_id_t> ExecutionOperatingUnitFeature::feature_id_counter{1};

}  // namespace terrier::brain
