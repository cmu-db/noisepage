#pragma once

namespace noisepage::execution::compiler {

/** Settings to be applied when compiling the input. */
class CompilerSettings {
 public:
  /** Set whether TPL should be captured during compilation. */
  void SetShouldCaptureTPL(bool should_capture_tpl) { should_capture_tpl_ = should_capture_tpl; }
  /** Set whether TBC should be captured during compilation. */
  void SetShouldCaptureTBC(bool should_capture_tbc) { should_capture_tbc_ = should_capture_tbc; }

  /** @return True if TPL should be captured. */
  [[nodiscard]] bool ShouldCaptureTPL() const noexcept { return should_capture_tpl_; }
  /** @return True if TBC should be captured. */
  [[nodiscard]] bool ShouldCaptureTBC() const noexcept { return should_capture_tbc_; }

 private:
  bool should_capture_tpl_{false};
  bool should_capture_tbc_{false};
};

}  // namespace noisepage::execution::compiler
