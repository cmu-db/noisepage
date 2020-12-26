#include <memory>
#include <string>
#include <utility>

// This allows the settings defined once to be used in different contexts.
// When __SETTING_GFLAGS_DEFINE__ is set,
//    setting definitions will be exposed through gflags definitions.
// When __SETTING_GFLAGS_DECLARE__ is set,
//    setting definitions will be exposed through gflags declarations.
// When __SETTING_DEFINE__ is set,
//    setting definitions will be exposed through definitions in settings_manager.
// When __SETTING_POPULATE__ is set,
//    setting definitions will be exposed through definitions in main function.
// When __SETTING_ENUM__ is set,
//    setting definitions will be exposed through Param.

#ifdef __SETTING_GFLAGS_DEFINE__

#ifdef SETTING_int
#undef SETTING_int
#endif
#ifdef SETTING_int64
#undef SETTING_int64
#endif
#ifdef SETTING_double
#undef SETTING_double
#endif
#ifdef SETTING_bool
#undef SETTING_bool
#endif
#ifdef SETTING_string
#undef SETTING_string
#endif

#define VALIDATOR_int(name, default_value)                                      \
  static bool Validate##name(const char *setting_name, int value) {             \
    if (FLAGS_##name == static_cast<int>(default_value)) {                      \
      return true;                                                              \
    }                                                                           \
    SETTINGS_LOG_ERROR("Value for {} has been set to {}", #name, FLAGS_##name); \
    return false;                                                               \
  }

#define VALIDATOR_int64(name, default_value)                                    \
  static bool Validate##name(const char *setting_name, int64_t value) {         \
    if (FLAGS_##name == static_cast<int64_t>(default_value)) {                  \
      return true;                                                              \
    }                                                                           \
    SETTINGS_LOG_ERROR("Value for {} has been set to {}", #name, FLAGS_##name); \
    return false;                                                               \
  }

#define VALIDATOR_double(name, default_value)                                   \
  static bool Validate##name(const char *setting_name, double value) {          \
    if (FLAGS_##name == static_cast<double>(default_value)) {                   \
      return true;                                                              \
    }                                                                           \
    SETTINGS_LOG_ERROR("Value for {} has been set to {}", #name, FLAGS_##name); \
    return false;                                                               \
  }

#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DEFINE_int32(name, default_value, description);                                                    \
  VALIDATOR_int(name, default_value);                                                                \
  DEFINE_validator(name, &Validate##name);

#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DEFINE_int64(name, default_value, description);                                                      \
  VALIDATOR_int64(name, default_value);                                                                \
  DEFINE_validator(name, &Validate##name);

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DEFINE_double(name, default_value, description);                                                      \
  VALIDATOR_double(name, default_value);                                                                \
  DEFINE_validator(name, &Validate##name);

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn) \
  DEFINE_bool(name, default_value, description);

#define SETTING_string(name, description, default_value, is_mutable, callback_fn) \
  DEFINE_string(name, default_value, description);
#endif

#ifdef __SETTING_GFLAGS_DECLARE__
#ifdef SETTING_int
#undef SETTING_int
#endif
#ifdef SETTING_int64
#undef SETTING_int64
#endif
#ifdef SETTING_double
#undef SETTING_double
#endif
#ifdef SETTING_bool
#undef SETTING_bool
#endif
#ifdef SETTING_string
#undef SETTING_string
#endif

#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DECLARE_int32(name);

#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DECLARE_int64(name);

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DECLARE_double(name);

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn) DECLARE_bool(name);

#define SETTING_string(name, description, default_value, is_mutable, callback_fn) DECLARE_string(name);
#endif

#ifdef __SETTING_VALIDATE__
#ifdef SETTING_int
#undef SETTING_int
#endif
#ifdef SETTING_int64
#undef SETTING_int64
#endif
#ifdef SETTING_double
#undef SETTING_double
#endif
#ifdef SETTING_bool
#undef SETTING_bool
#endif
#ifdef SETTING_string
#undef SETTING_string
#endif
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn)             \
  ValidateSetting(noisepage::settings::Param::name, {type::TypeId::INTEGER, execution::sql::Integer(min_value)}, \
                  {type::TypeId::INTEGER, execution::sql::Integer(max_value)});
#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn)          \
  ValidateSetting(noisepage::settings::Param::name, {type::TypeId::BIGINT, execution::sql::Integer(min_value)}, \
                  {type::TypeId::BIGINT, execution::sql::Integer(max_value)});

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn)    \
  ValidateSetting(noisepage::settings::Param::name, {type::TypeId::REAL, execution::sql::Real(min_value)}, \
                  {type::TypeId::REAL, execution::sql::Real(max_value)});

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn)                                      \
  ValidateSetting(noisepage::settings::Param::name, {type::TypeId::BOOLEAN, execution::sql::BoolVal(default_value)}, \
                  {type::TypeId::BOOLEAN, execution::sql::BoolVal(default_value)});

#define SETTING_string(name, description, default_value, is_mutable, callback_fn)              \
  {                                                                                            \
    std::string default_value_string{default_value};                                           \
    auto string_val = execution::sql::ValueUtil::CreateStringVal(default_value_string);        \
    auto default_value_cve = std::make_unique<parser::ConstantValueExpression>(                \
        type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));                \
    ValidateSetting(noisepage::settings::Param::name, *default_value_cve, *default_value_cve); \
  }
#endif

#ifdef __SETTING_ENUM__
#ifdef SETTING_int
#undef SETTING_int
#endif
#ifdef SETTING_int64
#undef SETTING_int64
#endif
#ifdef SETTING_double
#undef SETTING_double
#endif
#ifdef SETTING_bool
#undef SETTING_bool
#endif
#ifdef SETTING_string
#undef SETTING_string
#endif
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn) name,

#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn) name,

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) name,

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn) name,

#define SETTING_string(name, description, default_value, is_mutable, callback_fn) name,
#endif

#ifdef __SETTING_POPULATE__
#ifdef SETTING_int
#undef SETTING_int
#endif
#ifdef SETTING_int64
#undef SETTING_int64
#endif
#ifdef SETTING_double
#undef SETTING_double
#endif
#ifdef SETTING_bool
#undef SETTING_bool
#endif
#ifdef SETTING_string
#undef SETTING_string
#endif
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn)               \
  param_map.emplace(                                                                                               \
      noisepage::settings::Param::name,                                                                            \
      noisepage::settings::ParamInfo(#name, {type::TypeId::INTEGER, execution::sql::Integer(FLAGS_##name)},        \
                                     description, {type::TypeId::INTEGER, execution::sql::Integer(default_value)}, \
                                     is_mutable, min_value, max_value, &callback_fn));

#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn)            \
  param_map.emplace(                                                                                              \
      noisepage::settings::Param::name,                                                                           \
      noisepage::settings::ParamInfo(#name, {type::TypeId::BIGINT, execution::sql::Integer(FLAGS_##name)},        \
                                     description, {type::TypeId::BIGINT, execution::sql::Integer(default_value)}, \
                                     is_mutable, min_value, max_value, &callback_fn));

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn)                \
  param_map.emplace(                                                                                                   \
      noisepage::settings::Param::name,                                                                                \
      noisepage::settings::ParamInfo(#name, {type::TypeId::REAL, execution::sql::Real(FLAGS_##name)}, description,     \
                                     {type::TypeId::REAL, execution::sql::Real(default_value)}, is_mutable, min_value, \
                                     max_value, &callback_fn));

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn)                                    \
  param_map.emplace(                                                                                               \
      noisepage::settings::Param::name,                                                                            \
      noisepage::settings::ParamInfo(#name, {type::TypeId::BOOLEAN, execution::sql::BoolVal(FLAGS_##name)},        \
                                     description, {type::TypeId::BOOLEAN, execution::sql::BoolVal(default_value)}, \
                                     is_mutable, 0, 0, &callback_fn));

#define SETTING_string(name, description, default_value, is_mutable, callback_fn)                                \
  {                                                                                                              \
    const std::string_view value_string{FLAGS_##name};                                                           \
    auto string_val = execution::sql::ValueUtil::CreateStringVal(value_string);                                  \
                                                                                                                 \
    const std::string_view default_value_string{default_value};                                                  \
    auto default_value_string_val = execution::sql::ValueUtil::CreateStringVal(default_value_string);            \
                                                                                                                 \
    param_map.emplace(                                                                                           \
        noisepage::settings::Param::name,                                                                        \
        noisepage::settings::ParamInfo(                                                                          \
            #name, {type::TypeId::VARCHAR, string_val.first, std::move(string_val.second)}, description,         \
            {type::TypeId::VARCHAR, default_value_string_val.first, std::move(default_value_string_val.second)}, \
            is_mutable, 0, 0, &callback_fn));                                                                    \
  }

#endif
