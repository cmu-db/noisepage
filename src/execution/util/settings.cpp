#include "execution/util/settings.h"

#include <string>

#include "execution/util/cpu_info.h"

namespace terrier::execution {

namespace {

double DeriveOptimalFullSelectionThreshold(UNUSED_ATTRIBUTE Settings *settings, UNUSED_ATTRIBUTE CpuInfo *cpu_info) {
  // TODO(pmenon): Micro-benchmark to determine this at database startup for the current machine
  // TODO(pmenon): What about types?
  return 0.25;
}

double DeriveOptimalArithmeticFullComputeThreshold(UNUSED_ATTRIBUTE Settings *settings,
                                                   UNUSED_ATTRIBUTE CpuInfo *cpu_info) {
  // TODO(pmenon): Micro-benchmark to determine this at database startup for the current machine
  // TODO(pmenon): What about types?
  return 0.05;
}

double DeriveMinBitDensityThresholdForAvxIndexDecode(UNUSED_ATTRIBUTE Settings *settings,
                                                     UNUSED_ATTRIBUTE CpuInfo *cpu_info) {
  // TODO(pmenon): Micro-benchmark to determine this at database startup for the current machine
  return 0.15;
}

}  // namespace

Settings::Settings() {
  // First the constant setting values
#define CONST_SETTING(NAME, TYPE, VALUE) settings_[static_cast<uint32_t>(Settings::Name::NAME)] = VALUE;
#define COMPUTED_SETTING(...)
  SETTINGS_LIST(CONST_SETTING, COMPUTED_SETTING)
#undef CONST_SETTING
#undef COMPUTED_SETTING

  // Now the computed settings
#define CONST_SETTING(...)
#define COMPUTED_SETTING(NAME, TYPE, GEN_FN) \
  settings_[static_cast<uint32_t>(Settings::Name::NAME)] = GEN_FN(this, CpuInfo::Instance());
  SETTINGS_LIST(CONST_SETTING, COMPUTED_SETTING)
#undef CONST_SETTING
#undef COMPUTED_SETTING
}

}  // namespace terrier::execution
