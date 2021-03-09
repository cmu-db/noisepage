#include "self_driving/planning/pilot_thread.h"

namespace noisepage::selfdriving {
PilotThread::PilotThread(common::ManagedPointer<Pilot> pilot, std::chrono::microseconds pilot_interval,
                         std::chrono::microseconds forecaster_train_interval, bool pilot_planning)
    : pilot_(pilot),
      run_pilot_(true),
      pilot_paused_(!pilot_planning),
      pilot_interval_(pilot_interval),
      forecaster_train_interval_(forecaster_train_interval),
      forecaster_remain_period_(forecaster_train_interval),
      pilot_thread_(std::thread([this] { PilotThreadLoop(); })) {}

}  // namespace noisepage::selfdriving
