#pragma once

#include "self_driving/pilot/action/abstract_action.h"

namespace noisepage::selfdriving::pilot {

/**
 * Represent a drop index self-driving action
 */
class DropIndexAction : public AbstractAction {
 private:
  std::string index_name_;
  uint32_t num_threads_;

  std::vector<action_id_t> create_index_action_ids_;
};

}  // namespace noisepage::selfdriving::pilot
