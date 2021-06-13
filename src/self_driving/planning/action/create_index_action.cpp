#include "self_driving/planning/action/create_index_action.h"

#include "self_driving/planning/mcts/action_state.h"

namespace noisepage::selfdriving::pilot {

void CreateIndexAction::ModifyActionState(ActionState *action_state) { action_state->AddIndex(index_name_, id_); }

}  // namespace noisepage::selfdriving::pilot
