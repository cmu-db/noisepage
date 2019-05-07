// This allows the settings defined once to be used in different contexts.
// When __SETTING_GFLAGS_DEFINE__ is set,
//    setting definitions will be exposed through glfags definitions.
// When __SETTING_GFLAGS_DECLARE__ is set,
//    setting definitions will be exposed through glfags declarations.
// When __SETTING_DEFINE__ is set,
//    setting definitions will be exposed through defitions in settings_manager.
// When __SETTING_POPULATE__ is set,
//    setting definitions will be exposed through definitions in main function.
// When __SETTING_ENUM__ is set,
//    setting definitions will be exposed through Param.

#ifdef __SETTING_GFLAGS_DEFINE__

#ifdef SETTING_int
#undef SETTING_int
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

#define VALIDATOR_int(name, default_value)                                                      \
  static bool Validate##name(const char *setting_name, int value) {                             \
    if (FLAGS_##name == static_cast<int>(default_value)) {                                      \
      return true;                                                                              \
    }                                                                                           \
    std::cerr << "Value for \"" << #name << "\" has been set to " << FLAGS_##name << std::endl; \
    return false;                                                                               \
  }

#define VALIDATOR_double(name, default_value)                                                   \
  static bool Validate##name(const char *setting_name, double value) {                          \
    if (FLAGS_##name == static_cast<double>(default_value)) {                                   \
      return true;                                                                              \
    }                                                                                           \
    std::cerr << "Value for \"" << #name << "\" has been set to " << FLAGS_##name << std::endl; \
    return false;                                                                               \
  }

#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DEFINE_int32(name, default_value, description);                                                    \
  VALIDATOR_int(name, default_value);                                                                \
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

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  DECLARE_double(name);

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn) DECLARE_bool(name);

#define SETTING_string(name, description, default_value, is_mutable, callback_fn) DECLARE_string(name);
#endif

#ifdef __SETTING_VALIDATE__
#ifdef SETTING_int
#undef SETTING_int
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
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn)  \
  ValidateSetting(terrier::settings::Param::name, type::TransientValueFactory::GetInteger(min_value), \
                  type::TransientValueFactory::GetInteger(max_value), &callback_fn);

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) \
  ValidateSetting(terrier::settings::Param::name, type::TransientValueFactory::GetDecimal(min_value),   \
                  type::TransientValueFactory::GetDecimal(max_value), &callback_fn);

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn)                           \
  ValidateSetting(terrier::settings::Param::name, type::TransientValueFactory::GetBoolean(default_value), \
                  type::TransientValueFactory::GetBoolean(default_value), &callback_fn);

#define SETTING_string(name, description, default_value, is_mutable, callback_fn)                         \
  ValidateSetting(terrier::settings::Param::name, type::TransientValueFactory::GetVarChar(default_value), \
                  type::TransientValueFactory::GetVarChar(default_value), &callback_fn);
#endif

#ifdef __SETTING_ENUM__
#ifdef SETTING_int
#undef SETTING_int
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

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn) name,

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn) name,

#define SETTING_string(name, description, default_value, is_mutable, callback_fn) name,
#endif

#ifdef __SETTING_POPULATE__
#ifdef SETTING_int
#undef SETTING_int
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
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable, callback_fn)                   \
  param_map.emplace(                                                                                                   \
      terrier::settings::Param::name,                                                                                  \
      terrier::settings::ParamInfo(#name, terrier::type::TransientValueFactory::GetInteger(FLAGS_##name), description, \
                                   terrier::type::TransientValueFactory::GetInteger(default_value), is_mutable,        \
                                   min_value, max_value));

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable, callback_fn)                \
  param_map.emplace(                                                                                                   \
      terrier::settings::Param::name,                                                                                  \
      terrier::settings::ParamInfo(#name, terrier::type::TransientValueFactory::GetDecimal(FLAGS_##name), description, \
                                   terrier::type::TransientValueFactory::GetDecimal(default_value), is_mutable,        \
                                   min_value, max_value));

#define SETTING_bool(name, description, default_value, is_mutable, callback_fn)                             \
  param_map.emplace(terrier::settings::Param::name,                                                         \
                    terrier::settings::ParamInfo(                                                           \
                        #name, terrier::type::TransientValueFactory::GetBoolean(FLAGS_##name), description, \
                        terrier::type::TransientValueFactory::GetBoolean(default_value), is_mutable, 0, 0));

#define SETTING_string(name, description, default_value, is_mutable, callback_fn)                           \
  param_map.emplace(terrier::settings::Param::name,                                                         \
                    terrier::settings::ParamInfo(                                                           \
                        #name, terrier::type::TransientValueFactory::GetVarChar(FLAGS_##name), description, \
                        terrier::type::TransientValueFactory::GetVarChar(default_value), is_mutable, 0, 0));
#endif
