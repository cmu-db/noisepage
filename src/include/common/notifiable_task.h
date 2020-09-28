#pragma once


#include <unordered_set>

#include "common/dedicated_thread_task.h"
#include "common/event_util.h"
#include "common/macros.h"
#include "loggers/common_logger.h"

namespace terrier::common {

/**
 * Convenient MACRO to use a method as a libevent callback function. Example
 * usage:
 *
 * (..., METHOD_AS_CALLBACK(ConnectionDispatcherTask, DispatchConnection), obj)
 *
 * Would call DispatchConnection method on ConnectionDispatcherTask. obj must be
 * an instance
 * of ConnectionDispatcherTask. The method being invoked must have signature
 * void(int, short),
 * where int is the fd and short is the flags supplied by libevent.
 *
 */
#define METHOD_AS_CALLBACK(type, method) \
  [](int fd, int16_t flags, void *arg) { static_cast<type *>(arg)->method(fd, flags); }

/**
 * @brief NotifiableTasks can be configured to handle events with callbacks, and
 * executes within an event loop
 *
 * More specifically, NotifiableTasks are backed by libevent, and takes care of
 * memory management with the library.
 */
class NotifiableTask : public DedicatedThreadTask {
 public:
  /** Construct a new NotifiableTask instance with the specified task id. */
  explicit NotifiableTask(ev::loop_ref loop, int task_id);

  /** Destroy the NotifiableTask, deleting and freeing all of its registered events. */
  ~NotifiableTask() override;

  /** @return The unique ID assigned to the task. */
  size_t Id() const { return task_id_; }

  /**
   * @brief Register an event with the event base associated with this
   * notifiable task.
   *
   * After registration, the event firing will result in the callback registered
   * executing on the thread this task is running on. Certain events have the
   * same life cycle as the task itself, in which case it is safe to ignore the
   * return value and have these events be freed on destruction of the task.
   * However, if this is not the case, the caller will need to save the return
   * value and manually unregister the event with the task.
   * @see UnregisterEvent().
   *
   * @param fd the file descriptor or signal to be monitored, or -1. (if manual
   *         or time-based)
   * @param flags desired events to monitor: bitfield of EV_READ, EV_WRITE,
   *         EV_SIGNAL, EV_PERSIST, EV_ET.
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @param timeout the maximum amount of time to wait for an event, defaults to
   *        null which will wait forever
   * @return pointer to the allocated event.
   */
  template<void (*function)(ev::io &event, int)>
  ev::io *RegisterIoEvent(int fd, uint16_t flags, void *arg,
                                          const struct timeval *timeout = nullptr) {
    auto *event = new ev::io(loop_);
    event->set<function>(arg);
    io_events_.insert(event);
    event->start(fd, flags);
    return event;
  }

  template<void (*function)(ev::async &event, int)>
  ev::async *RegisterAsyncEvent(void *arg) {
    auto *event = new ev::async(loop_);
    event->set<function>(arg);
    async_events_.insert(event);
    event->start();
    return event;
  }

  /**
   * @brief Register an event that fires periodically based on the given time
   * interval.
   * This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param timeout period of wait between each firing of this event
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @return pointer to the allocated event.
   */
/*  struct event *RegisterPeriodicEvent(const struct timeval *timeout, event_callback_fn callback, void *arg) {
    return RegisterEvent(-1, EV_TIMEOUT | EV_PERSIST, callback, arg, timeout);
  }*/

  /**
   * @brief Register an event that can only be fired if someone calls
   * event_active on it manually.
   * This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @return pointer to the allocated event.
   */
/*  struct event *RegisterManualEvent(event_callback_fn callback, void *arg) {
    return RegisterEvent(-1, EV_PERSIST, callback, arg);
  }*/

  /**
   * @brief Updates the callback information for a registered event
   *
   * @param event The registered event
   * @param fd The new file descriptor for the event to be assigned to
   * @param flags The new flags for the event
   * @param callback The callback function for the event
   * @param arg Argument to the callback function
   * @param timeout Timeout if any for the event
   */
  template<void (*function)(ev::io &event, int)>
  void UpdateIoEvent(ev::io *event, int fd, uint16_t flags, void *arg, const struct timeval *timeout) {
    TERRIER_ASSERT(!(io_events_.find(event) == io_events_.end()), "Didn't find event");
    // TODO handle timeout
    event->stop();
    event->set<function>(arg);
    event->start(fd, flags);
  }

  /**
   * @brief Unregister the event given. The event is no longer active and its
   * memory is freed
   *
   * The event pointer must be handed out from an earlier call to RegisterEvent.
   *
   * @see RegisterEvent()
   *
   * @param event the event to be freed
   */
  void UnregisterIoEvent(ev::io *event);

  void UnregisterAsyncEvent(ev::async *event);

  /**
   * In a loop, make this notifiable task wait and respond to incoming events
   */
  void EventLoop() {
    loop_.run(0);
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
  void ExitLoop() {
    terminate_->send();
  }

  /**
   * Wrapper around ExitLoop() to conform to libevent callback signature
   */
  void ExitLoop(int, int16_t) { ExitLoop(); }  // NOLINT

 protected:
  ev::loop_ref loop_;
 private:
  static void TerminateCallback(ev::async &event, int revents);

  ev::async *terminate_;
  const int task_id_;

  // struct event and lifecycle management
  std::unordered_set<ev::io *> io_events_;
  std::unordered_set<ev::async *> async_events_;
};

}  // namespace terrier::common
