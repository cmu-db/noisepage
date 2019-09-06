#pragma once

#include <vector>

#include "common/macros.h"
#include "execution/util/timer.h"

namespace terrier::execution::util {

/**
 * Stage timer
 * @tparam ResolutionRatio resolution of the timer
 */
template <typename ResolutionRatio = std::milli>
class StageTimer {
 public:
  /**
   * Information about a stage.
   */
  class Stage {
   public:
    /**
     * Create a new stage with name @em name.
     * @param name The name of the stage.
     */
    explicit Stage(const char *name) noexcept : name_(name), time_(0) {}

    /**
     * Return the name of this stage.
     */
    const char *Name() const noexcept { return name_; }

    /**
     * Return the time this stage took in the configured resolution ratio.
     */
    double Time() const noexcept { return time_; }

   private:
    friend class StageTimer<ResolutionRatio>;

    void SetTime(const double time) { time_ = time; }

   private:
    // The name
    const char *const name_;
    // The time in this stage
    double time_;
  };

  /**
   * Construct.
   */
  StageTimer() { stages_.reserve(8); }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(StageTimer);

  /**
   * Enter a new stage with the given name.
   * @param stage_name The name of the stage we're entering.
   */
  void EnterStage(const char *stage_name) {
    stages_.emplace_back(stage_name);
    // Start the timer which will stop in a subsequent call to ExitStage()
    timer_.Start();
  }

  /**
   * Exit the current stage.
   */
  void ExitStage() {
    TERRIER_ASSERT(!stages_.empty(), "Missing call to EnterStage()");
    TERRIER_ASSERT(stages_.back().Time() == 0, "Duplicate call to ExitStage()");
    timer_.Stop();
    stages_.back().SetTime(timer_.Elapsed());
  }

  /**
   * Access information on all stages.
   */
  const std::vector<Stage> &GetStages() const { return stages_; }

 private:
  util::Timer<ResolutionRatio> timer_;
  std::vector<Stage> stages_;
};

}  // namespace terrier::execution::util
