#include "brain/pilot/pilot_thread.h"

namespace noisepage::brain {
PilotThread::PilotThread(common::ManagedPointer<Pilot> pilot,
                         std::chrono::microseconds pl_period, bool pilot_planning)
    : pl_(pilot),
      run_pl_(true),
      pl_paused_(!pilot_planning),
      pl_period_(pl_period),
      pl_thread_(std::thread([this] {
        PLThreadLoop();
      })) {}

}  // namespace noisepage::brain
