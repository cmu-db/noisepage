#pragma once

#include <memory>
#include "common/action_context.h"

namespace terrier {
class DBMain;
}

namespace terrier::settings {

/**
 * Utility class for defining callbacks for settings in settings_defs.h.
 */
class Callbacks {
 public:
  Callbacks() = delete;

  /**
   * Default callback for settings in the Settings Manager. Does nothing but set the state to SUCCESS.
   * @param old_value old settings value
   * @param new_value new settings value
   * @param db_main pointer to db_main
   * @param action_context pointer to the action context for this settings change
   */
  static void NoOp(void *old_value, void *new_value, DBMain *db_main,
                   const std::shared_ptr<common::ActionContext> &action_context);

  /**
   * Changes the buffer segment pool size limit.
   * @param old_value old settings value
   * @param new_value new settings value
   * @param db_main pointer to db_main
   * @param action_context pointer to the action context for this settings change
   */
  static void BufferSegmentPoolSizeLimit(void *old_value, void *new_value, DBMain *db_main,
                                         const std::shared_ptr<common::ActionContext> &action_context);

  /**
   * Changes the buffer segment pool reuse limit.
   * @param old_value old settings value
   * @param new_value new settings value
   * @param db_main pointer to db_main
   * @param action_context pointer to the action context for this settings change
   */
  static void BufferSegmentPoolReuseLimit(void *old_value, void *new_value, DBMain *db_main,
                                          const std::shared_ptr<common::ActionContext> &action_context);

  /**
   * Changes the number of worker pool threads.
   * @param old_value old settings value
   * @param new_value new settings value
   * @param db_main pointer to db_main
   * @param action_context pointer to the action context for this settings change
   */
  static void WorkerPoolThreads(void *old_value, void *new_value, DBMain *db_main,
                                const std::shared_ptr<common::ActionContext> &action_context);

  /**
   * Changes the number of buffers the log manager uses.
   * @param old_value old settings value
   * @param new_value new settings value
   * @param db_main pointer to db_main
   * @param action_context pointer to the action context for this settings change
   */
  static void NumLogManagerBuffers(void *old_value, void *new_value, DBMain *db_main,
                                   const std::shared_ptr<common::ActionContext> &action_context);
};
}  // namespace terrier::settings
