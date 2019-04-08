#pragma once
#include <type/value.h>
#include <string>

namespace terrier::settings {

enum class Param {
#define __SETTING_ENUM__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_ENUM__
};

struct ParamInfo {
  std::string name;
  type::Value value;
  std::string desc;
  type::Value default_value;
  bool is_mutable;

  ParamInfo(std::string name, const type::Value &value, std::string desc, const type::Value &default_value,
            bool is_mutable)
      : name(std::move(name)),
        value(value),
        desc(std::move(desc)),
        default_value(default_value),
        is_mutable(is_mutable) {}
};

}  // namespace terrier::settings