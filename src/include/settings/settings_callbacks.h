#pragma once

#include <memory>
#include "common/action_context.h"

namespace terrier {
class DBMain;
}

namespace terrier::settings {

class Callbacks {
 public:
  Callbacks() = delete;

  static void NoOp(void *old_value, void *new_value, DBMain *db_main,
                   const std::shared_ptr<common::ActionContext> &action_context);

  static void BufferSegmentPoolSizeLimit(void *old_value, void *new_value, DBMain *db_main,
                                         const std::shared_ptr<common::ActionContext> &action_context);
};

}  // namespace terrier::settings
