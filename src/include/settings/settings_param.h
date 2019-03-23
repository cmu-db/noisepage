#pragma once

namespace terrier::settings {

enum class Param {
#define __SETTING_ENUM__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_ENUM__
};

}  // namespace terrier::settings
