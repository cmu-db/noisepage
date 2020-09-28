#include "common/notifiable_task.h"

#include <cstring>

#include "common/event_util.h"
#include "loggers/common_logger.h"

namespace terrier::common {

NotifiableTask::NotifiableTask(int task_id) : task_id_(task_id) {
  //loop_ = EventUtil::EventBaseNew();
  // For exiting a loop




/*  RegisterManualEvent(
      [](int *//*unused*//*, int16_t *//*unused*//*, void *arg) {
        EventUtil::EventBaseLoopExit(static_cast<struct event_base *>(arg), nullptr);
      },
      loop_);*/
}

NotifiableTask::~NotifiableTask() {
  for (ev_io *event : events_) {
    //EventUtil::EventDel(event);
    //event_free(event);
    ev_io_stop(loop_, event);
    free(event);
  }
  ev_async_stop(loop_, terminate_);
  free(terminate_);
  //event_base_free(loop_);
  ev_loop_destroy(loop_);
}

ev_io *NotifiableTask::RegisterEvent(int fd, uint16_t flags, io_callback callback, void *arg,
                                            const struct timeval *timeout) {
  // TODO handle timeout
  auto *event = reinterpret_cast<ev_io *>(malloc(sizeof(ev_io)));
  event->data = arg;
  ev_io_init(event, callback, fd, flags);
  ev_io_start(loop_, event);
  /*struct event *event = event_new(loop_, fd, flags, callback, arg);
  events_.insert(event);
  EventUtil::EventAdd(event, timeout);*/
  return event;
}

void NotifiableTask::UnregisterEvent(ev_io *event) {
  auto it = events_.find(event);
  if (it == events_.end()) return;
  ev_io_stop(loop_, event);
//  if (event_del(event) == -1) {
//    COMMON_LOG_ERROR("Failed to delete event");
//    return;
//  }
//  event_free(event);
  events_.erase(event);
  free(event);
}

}  // namespace terrier::common
