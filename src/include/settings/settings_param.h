#pragma once
#include <string>
#include <type/value.h>

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

  ParamInfo(const std::string &name, const type::Value &value, const std::string &desc, const type::Value &default_value,
        bool is_mutable)
      : name(name), value(value), desc(desc), default_value(default_value),
        is_mutable(is_mutable) {}
};

}  // namespace terrier::settings