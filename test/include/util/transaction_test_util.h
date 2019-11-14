#pragma once
#include "transaction/deferred_action_manager.h"

namespace terrier {

class TransactionTestUtil{
    public:
        static int ActionsInDeferredQueue(transaction::DeferredActionManager *deferred_action) {
            return deferred_action->new_deferred_actions_.size() + deferred_action->back_log_.size();
        }
};
}