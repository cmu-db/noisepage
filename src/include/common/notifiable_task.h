#pragma once

#include <ev++.h>
#include <unordered_set>

#include "common/dedicated_thread_task.h"
#include "common/io_timeout_event.h"
#include "common/macros.h"
#include "loggers/common_logger.h"

namespace terrier::common {

/**
 * @brief NotifiableTasks can be configured to handle events with callbacks, and
 * executes within an event loop
 *
 * More specifically, NotifiableTasks are backed by libev, and takes care of
 * memory management with the library.
 */
class NotifiableTask : public DedicatedThreadTask {
 public:
  /** Construct a new NotifiableTask instance with the specified task id. */
  explicit NotifiableTask(std::unique_ptr<ev::loop_ref> loop, int task_id);

  /** Destroy the NotifiableTask, deleting and freeing all of its registered events. */
  ~NotifiableTask() override;

  /** @return The unique ID assigned to the task. */
  size_t Id() const { return task_id_; }

  /**
   * @brief Register an I/O event with the event loop associated with this
   * notifiable task.
   *
   * After registration, the event firing will result in the callback registered
   * executing on the thread this task is running on. Certain events have the
   * same life cycle as the task itself, in which case it is safe to ignore the
   * return value and have these events be freed on destruction of the task.
   * However, if this is not the case, the caller will need to save the return
   * value and manually unregister the event with the task.
   * @see UnregisterIoEvent().
   *
   * @tparam function callback function to register with event
   * @param fd the file descriptor or signal to be monitored, or -1. (if manual
   *         or time-based)
   * @param flags desired events to monitor: bitfield of ev::READ, ev::WRITE.
   * @param arg an argument to be passed to the callback function
   * @param timeout the maximum amount of time to wait for an event, defaults to
   *        null which will wait forever
   * @return pointer to the allocated event.
   */
  template <void (*function)(ev::io &event, int)>  // NOLINT
  IoTimeoutEvent *RegisterIoEvent(int fd, uint16_t flags, void *arg,
                                  const ev_tstamp timeout = IoTimeoutEvent::WAIT_FOREVER) {
    auto *event = new IoTimeoutEvent(*loop_);
    event->Set<function>(arg);
    io_events_.insert(event);
    event->Start(fd, flags, timeout);
    return event;
  }

  /**
   * @brief Register an event that can only be fired if someone
   * activates on it manually.
   *
   * After registration, the event firing will result in the callback registered
   * executing on the thread this task is running on. Certain events have the
   * same life cycle as the task itself, in which case it is safe to ignore the
   * return value and have these events be freed on destruction of the task.
   * However, if this is not the case, the caller will need to save the return
   * value and manually unregister the event with the task.
   *
   * @see UnregisterAsyncEvent()
   * @tparam function callback function to register with event
   * @param arg an argument to be passed to the callback function
   * @return pointer to the allocated event.
   */
  template <void (*function)(ev::async &event, int)>  // NOLINT
  ev::async *RegisterAsyncEvent(void *arg) {
    auto *event = new ev::async(*loop_);
    event->set<function>(arg);
    async_events_.insert(event);
    event->start();
    return event;
  }

  /**
   * @brief Updates the callback information for a registered event
   *
   * @tparam function callback function to register with event
   * @param event The registered event
   * @param fd The new file descriptor for the event to be assigned to
   * @param flags The new flags for the event
   * @param arg Argument to the callback function
   * @param timeout Timeout if any for the event
   */
  template <void (*function)(ev::io &event, int)>  // NOLINT
  void UpdateIoEvent(IoTimeoutEvent *event, int fd, uint16_t flags, void *arg, ev_tstamp timeout) {
    TERRIER_ASSERT(!(io_events_.find(event) == io_events_.end()), "Didn't find event");
    event->Stop();
    event->Set<function>(arg);
    event->Start(fd, flags, timeout);
  }

  /**
   * @brief Unregister the I/O event given. The event is no longer active and its
   * memory is freed
   *
   * The event pointer must be handed out from an earlier call to RegisterEvent.
   *
   * @see RegisterIoEvent()
   *
   * @param event the event to be freed
   */
  void UnregisterIoEvent(IoTimeoutEvent *event);

  /**
   * @brief Unregister the Async event given. The event is no longer active and its
   * memory is freed
   *
   * The event pointer must be handed out from an earlier call to RegisterEvent.
   *
   * @see RegisterAsyncEvent()
   *
   * @param event the event to be freed
   */
  void UnregisterAsyncEvent(ev::async *event);

  /**
   * In a loop, make this notifiable task wait and respond to incoming events
   */
  void EventLoop() {
    loop_->run(0);
    COMMON_LOG_TRACE("stop");
  }

  /**
   * Overrides DedicatedThreadTask entry point method
   */
  void RunTask() override { EventLoop(); }

  /**
   * Overrides DedicatedThreadTask exit point method
   */
  void Terminate() override { ExitLoop(); }

  /**
   * Exits the event loop
   */
  void ExitLoop() { terminate_->send(); }

 protected:
  /** Event loop for the current thread */
  std::unique_ptr<ev::loop_ref> loop_;

 private:
  /** Callback to terminate event loop */
  static void TerminateCallback(ev::async &event, int revents);  // NOLINT

  ev::async *terminate_;
  const int task_id_;

  // struct event and lifecycle management
  std::unordered_set<IoTimeoutEvent *> io_events_;
  std::unordered_set<ev::async *> async_events_;
};

}  // namespace terrier::common
