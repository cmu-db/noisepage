#include "brain/pilot/pilot_thread.h"

namespace noisepage::brain {
PilotThread::PilotThread(common::ManagedPointer<Pilot> pilot, std::chrono::microseconds pl_period, bool pilot_planning)
    : pilot_(pilot),
      run_pilot_(true),
      pilot_paused_(!pilot_planning),
      pilot_period_(pl_period),
      pilot_thread_(std::thread([this] { PilotThreadLoop(); })) {}

}  // namespace noisepage::brain
