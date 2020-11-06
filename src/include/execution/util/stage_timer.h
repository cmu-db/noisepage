#pragma once

#include <numeric>
#include <vector>

#include "common/macros.h"
#include "execution/util/timer.h"

namespace noisepage::execution::util {

/** Timer that supports multiple stages. */
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
    NOISEPAGE_ASSERT(!stages_.empty(), "Missing call to EnterStage()");
    NOISEPAGE_ASSERT(stages_.back().Time() == 0, "Duplicate call to ExitStage()");
    timer_.Stop();
    stages_.back().SetTime(timer_.GetElapsed());
  }

  /**
   * @return The total time across all stages.
   */
  double GetTotalElapsedTime() const {
    return std::accumulate(stages_.begin(), stages_.end(), double{0},
                           [](double c, const Stage &stage) { return c + stage.Time(); });
  }

  /**
   * @return A const view of information on all stages.
   */
  const std::vector<Stage> &GetStages() const { return stages_; }

 private:
  util::Timer<ResolutionRatio> timer_;
  std::vector<Stage> stages_;
};

}  // namespace noisepage::execution::util
