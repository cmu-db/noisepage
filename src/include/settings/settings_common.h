// This allows the settings defined once to be used in different contexts.
// When __SETTING_GFLAGS_DEFINE__ is set,
//    setting definitions will be exposed through glfags definitions.
// When __SETTING_GFLAGS_DECLARE__ is set,
//    setting definitions will be exposed through glfags declarations.
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
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  ValidateSetting(terrier::settings::Param::name,                                                    \
                  {type::TypeId::INTEGER, std::make_unique<execution::sql::Integer>(min_value)},     \
                  {type::TypeId::INTEGER, std::make_unique<execution::sql::Integer>(max_value)});
#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  ValidateSetting(terrier::settings::Param::name,                                                      \
                  {type::TypeId::BIGINT, std::make_unique<execution::sql::Integer>(min_value)},        \
                  {type::TypeId::BIGINT, std::make_unique<execution::sql::Integer>(max_value)});

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  ValidateSetting(terrier::settings::Param::name,                                                       \
                  {type::TypeId::DECIMAL, std::make_unique<execution::sql::Real>(min_value)},           \
                  {type::TypeId::DECIMAL, std::make_unique<execution::sql::Real>(max_value)});

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn)                      \
  ValidateSetting(terrier::settings::Param::name,                                                    \
                  {type::TypeId::BOOLEAN, std::make_unique<execution::sql::BoolVal>(default_value)}, \
                  {type::TypeId::BOOLEAN, std::make_unique<execution::sql::BoolVal>(default_value)});

#define SETTING_string(name, description, default_value, is_mutable, callback_fn)                                      \
  std::string default_value_string{default_value};                                                                     \
  std::unique_ptr<parser::ConstantValueExpression> default_value_cve;                                                  \
  if (default_value_string.length() <= execution::sql::StringVal::InlineThreshold()) {                                 \
    auto default_value_stringval =                                                                                     \
        std::make_unique<execution::sql::StringVal>(default_value_string.c_str(), default_value_string.length());      \
    default_value_cve = std::make_unique<parser::ConstantValueExpression>(                                             \
        type::TypeId::VARCHAR, std::move(default_value_stringval), nullptr);                                           \
  } else {                                                                                                             \
    /* TODO(Matt): smarter allocation? */                                                                              \
    auto *const buffer = common::AllocationUtil::AllocateAligned(default_value_string.length());                       \
    std::memcpy(buffer, default_value_string.c_str(), default_value_string.length());                                  \
    auto default_value_stringval = std::make_unique<execution::sql::StringVal>(reinterpret_cast<const char *>(buffer), \
                                                                               default_value_string.length());         \
    default_value_cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR,                       \
                                                                          std::move(default_value_stringval), buffer); \
  }                                                                                                                    \
  ValidateSetting(terrier::settings::Param::name, *default_value_cve, *default_value_cve);
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
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn)          \
  param_map.emplace(terrier::settings::Param::name,                                                           \
                    terrier::settings::ParamInfo(                                                             \
                        #name,                                                                                \
                        std::make_unique<parser::ConstantValueExpression>(                                    \
                            type::TypeId::INTEGER, std::make_unique<execution::sql::Integer>(FLAGS_##name)),  \
                        description,                                                                          \
                        std::make_unique<parser::ConstantValueExpression>(                                    \
                            type::TypeId::INTEGER, std::make_unique<execution::sql::Integer>(default_value)), \
                        is_mutable, min_value, max_value, &callback_fn));

#define SETTING_int64(name, description, default_value, min_value, max_value, is_mutable, callback_fn)       \
  param_map.emplace(terrier::settings::Param::name,                                                          \
                    terrier::settings::ParamInfo(                                                            \
                        #name,                                                                               \
                        std::make_unique<parser::ConstantValueExpression>(                                   \
                            type::TypeId::BIGINT, std::make_unique<execution::sql::Integer>(FLAGS_##name)),  \
                        description,                                                                         \
                        std::make_unique<parser::ConstantValueExpression>(                                   \
                            type::TypeId::BIGINT, std::make_unique<execution::sql::Integer>(default_value)), \
                        is_mutable, min_value, max_value, &callback_fn));

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn)               \
  param_map.emplace(                                                                                                  \
      terrier::settings::Param::name,                                                                                 \
      terrier::settings::ParamInfo(#name,                                                                             \
                                   std::make_unique<parser::ConstantValueExpression>(                                 \
                                       type::TypeId::DECIMAL, std::make_unique<execution::sql::Real>(FLAGS_##name)),  \
                                   description,                                                                       \
                                   std::make_unique<parser::ConstantValueExpression>(                                 \
                                       type::TypeId::DECIMAL, std::make_unique<execution::sql::Real>(default_value)), \
                                   is_mutable, min_value, max_value, &callback_fn));

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn)                               \
  param_map.emplace(terrier::settings::Param::name,                                                           \
                    terrier::settings::ParamInfo(                                                             \
                        #name,                                                                                \
                        std::make_unique<parser::ConstantValueExpression>(                                    \
                            type::TypeId::BOOLEAN, std::make_unique<execution::sql::BoolVal>(FLAGS_##name)),  \
                        description,                                                                          \
                        std::make_unique<parser::ConstantValueExpression>(                                    \
                            type::TypeId::BOOLEAN, std::make_unique<execution::sql::BoolVal>(default_value)), \
                        is_mutable, 0, 0, &callback_fn));

#define SETTING_string(name, description, default_value, is_mutable, callback_fn)                                      \
  std::string value_string{FLAGS_##name};                                                                              \
  std::unique_ptr<parser::ConstantValueExpression> value_cve;                                                          \
  if (value_string.length() <= execution::sql::StringVal::InlineThreshold()) {                                         \
    auto value_stringval = std::make_unique<execution::sql::StringVal>(value_string.c_str(), value_string.length());   \
    value_cve =                                                                                                        \
        std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, std::move(value_stringval), nullptr); \
  } else {                                                                                                             \
    /* TODO(Matt): smarter allocation? */                                                                              \
    auto *const buffer = common::AllocationUtil::AllocateAligned(value_string.length());                               \
    std::memcpy(buffer, value_string.c_str(), value_string.length());                                                  \
    auto value_stringval =                                                                                             \
        std::make_unique<execution::sql::StringVal>(reinterpret_cast<const char *>(buffer), value_string.length());    \
    value_cve =                                                                                                        \
        std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, std::move(value_stringval), buffer);  \
  }                                                                                                                    \
                                                                                                                       \
  std::string default_value_string{default_value};                                                                     \
  std::unique_ptr<parser::ConstantValueExpression> default_value_cve;                                                  \
  if (default_value_string.length() <= execution::sql::StringVal::InlineThreshold()) {                                 \
    auto default_value_stringval =                                                                                     \
        std::make_unique<execution::sql::StringVal>(default_value_string.c_str(), default_value_string.length());      \
    default_value_cve = std::make_unique<parser::ConstantValueExpression>(                                             \
        type::TypeId::VARCHAR, std::move(default_value_stringval), nullptr);                                           \
  } else {                                                                                                             \
    /* TODO(Matt): smarter allocation? */                                                                              \
    auto *const buffer = common::AllocationUtil::AllocateAligned(default_value_string.length());                       \
    std::memcpy(buffer, default_value_string.c_str(), default_value_string.length());                                  \
    auto default_value_stringval = std::make_unique<execution::sql::StringVal>(reinterpret_cast<const char *>(buffer), \
                                                                               default_value_string.length());         \
    default_value_cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR,                       \
                                                                          std::move(default_value_stringval), buffer); \
  }                                                                                                                    \
                                                                                                                       \
  param_map.emplace(terrier::settings::Param::name,                                                                    \
                    terrier::settings::ParamInfo(#name, std::move(value_cve), description,                             \
                                                 std::move(default_value_cve), is_mutable, 0, 0, &callback_fn));

#endif
