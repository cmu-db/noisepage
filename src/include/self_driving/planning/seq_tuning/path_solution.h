#pragma once

#include <set>
#include <utility>
#include <vector>

#include "self_driving/planning/action/action_defs.h"

namespace noisepage::selfdriving::pilot {
/** Stores the configurations and cost related to a solution found in a graph */

struct PathSolution {
  /** The sequence of configurations on the path */
  std::vector<std::set<action_id_t>> config_on_path;

  /** The set of unique configurations on the path */
  std::set<std::set<action_id_t>> unique_config_on_path;

  /** Distance from source to distance following this path */
  double path_length;

  bool operator<(const PathSolution &other_path) const { return path_length < other_path.path_length; }
};

}  // namespace noisepage::selfdriving::pilot
