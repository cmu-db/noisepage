#include "self_driving/planning/action/drop_index_action.h"

#include "self_driving/planning/mcts/action_state.h"

namespace noisepage::selfdriving::pilot {

void DropIndexAction::ModifyActionState(ActionState *action_state) { action_state->RemoveIndex(index_name_, id_); }

}  // namespace noisepage::selfdriving::pilot
