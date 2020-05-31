#pragma once

#include <array>
#include <optional>
#include <string>
#include <variant>

#include "common/macros.h"

namespace terrier::execution {

// The list of all settings in the engine. The two arguments to the macro are both callback
// functions, but with different signatures.
//
// CONST     : Callback function assuming the setting is a constant hard-coded value.
//             Args: setting name, C/C++ primitive type, default value
// COMPUTED  : Callback function assuming default value is computed at runtime.
//             Args: setting name, C/C++ primitive type, provider function.
#define SETTINGS_LIST(CONST, COMPUTED)                                                             \
  /*                                                                                               \
   * When performing selections, rather than operating only on active elements                     \
   * in a TID list, it may be faster to apply the selection on ALL elements and                    \
   * quickly mask out unselected TIDS. Such an optimization removes branches                       \
   * from the loop and allows the compiler to auto-vectorize the operation.                        \
   * However, this optimization wins only when the selectivity of the input TID                    \
   * list is greater than a threshold value. This threshold can vary between                       \
   * platforms and data types. Thus, we derive the threshold at database startup                   \
   * once using the given function.                                                                \
   */                                                                                              \
  COMPUTED(SelectOptThreshold, double, DeriveOptimalFullSelectionThreshold)                        \
                                                                                                   \
  /*                                                                                               \
   * When performing arithmetic operations on vectors, this setting determines                     \
   * the minimum required vector selectivity before switching to a full-compute                    \
   * implementation. A full computation is one that ignores the selection vector                   \
   * or filtered TID list of the input vectors and blindly operators on all                        \
   * vector elements. Though wasteful, the algorithm is amenable to                                \
   * auto-vectorization by the compiler yielding better overall performance.                       \
   */                                                                                              \
  COMPUTED(ArithmeticFullComputeOptThreshold, double, DeriveOptimalArithmeticFullComputeThreshold) \
                                                                                                   \
  /*                                                                                               \
   * The frequency at which to sample statistics when adaptively reordering                        \
   * predicate clauses falling in the range [0.0, 1.0]. A low frequency incurs                     \
   * minimal runtime overhead, but is less reactive to changing distributions in                   \
   * the underlying data. A high re-sampling frequency is more adaptive, but                       \
   * incurs higher runtime overhead. Thus, there is a trade-off here.                              \
   */                                                                                              \
  CONST(AdaptivePredicateOrderSamplingFrequency, float, 0.1)                                       \
                                                                                                   \
  /*                                                                                               \
   * The minimum bit vector density before using a SIMD decoding algorithm.                        \
   */                                                                                              \
  COMPUTED(BitDensityThresholdForAVXIndexDecode, float,                                            \
           DeriveMinBitDensityThresholdForAvxIndexDecode)                                          \
                                                                                                   \
  /*                                                                                               \
   * Flag indicating if parallel execution is supported.                                           \
   */                                                                                              \
  CONST(ParallelQueryExecution, bool, true)

class Settings {
 public:
  // List of all settings
  enum class Name : uint32_t {
#define F(NAME, ...) NAME,
    SETTINGS_LIST(F, F)
#undef F
#define COUNT_OP(inst, ...) +1
        Last = SETTINGS_LIST(COUNT_OP, COUNT_OP)
#undef COUNT_OP
  };

  /**
   * Number of settings.
   */
  static constexpr uint32_t kNumSettings = static_cast<uint32_t>(Name::Last);

  /**
   * Setting values are stored as glorified unions.
   */
  using Value = std::variant<bool, int64_t, double, std::string>;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Settings);

  /**
   * Singleton instance access.
   * @return The only instance of the settings object.
   */
  static Settings *Instance() {
    static Settings kInstance;
    return &kInstance;
  }

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as a boolean, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  bool GetBool(Name name) const {
    TERRIER_ASSERT(name < Name::Last, "Invalid setting");
    return std::get<bool>(settings_[static_cast<uint32_t>(name)]);
  }

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as an integer, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  int64_t GetInt(Name name) const {
    TERRIER_ASSERT(name < Name::Last, "Invalid setting");
    return std::get<int64_t>(settings_[static_cast<uint32_t>(name)]);
  }

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as a floating-point number, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  double GetDouble(Name name) const {
    TERRIER_ASSERT(name < Name::Last, "Invalid setting");
    return std::get<double>(settings_[static_cast<uint32_t>(name)]);
  }

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as a string, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  std::string GetString(Name name) const {
    TERRIER_ASSERT(name < Name::Last, "Invalid setting");
    return std::get<std::string>(settings_[static_cast<uint32_t>(name)]);
  }

  /**
   * Set the value of the given setting to the provided value.
   * @warning This is not thread-safe.
   * @param name The name of the setting to set.
   * @param val The value to set.
   */
  void SetDouble(Name name, const double val) { settings_[static_cast<uint32_t>(name)] = val; }

 private:
  // Private to force singleton access
  Settings();

 private:
  // Container for all settings
  std::array<Value, kNumSettings> settings_;
};

}  // namespace terrier::execution
