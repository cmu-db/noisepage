#include <memory>
#include "common/main_stat_registry.h"

std::shared_ptr<terrier::common::StatisticsRegistry> main_stat_reg;

void init_main_stat_reg() {
  main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();
}
