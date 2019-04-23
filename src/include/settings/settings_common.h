// This allows the settings defined once to be used in different contexts.
// When __SETTING_GFLAGS_DEFINE__ is set,
//    setting definitions will be exposed through gflags definitions.
// When __SETTING_GFLAGS_DECLARE__ is set,
//    setting definitions will be exposed through gflags declarations.
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

#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable) \
  DEFINE_int32(name, default_value, description);

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable) \
  DEFINE_double(name, default_value, description);

#define SETTING_bool(name, description, default_value, is_mutable) DEFINE_bool(name, default_value, description);

#define SETTING_string(name, description, default_value, is_mutable) DEFINE_string(name, default_value, description);
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

#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable) DECLARE_int32(name);

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable) DECLARE_double(name);

#define SETTING_bool(name, description, default_value, is_mutable) DECLARE_bool(name);

#define SETTING_string(name, description, default_value, is_mutable) DECLARE_string(name);
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
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable)                                \
  param_map.emplace(                                                                                                   \
      terrier::settings::Param::name,                                                                                  \
      terrier::settings::ParamInfo(#name, terrier::type::TransientValueFactory::GetInteger(FLAGS_##name), description, \
                                   terrier::type::TransientValueFactory::GetInteger(default_value), is_mutable));

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable)                             \
  param_map.emplace(                                                                                                   \
      terrier::settings::Param::name,                                                                                  \
      terrier::settings::ParamInfo(#name, terrier::type::TransientValueFactory::GetDecimal(FLAGS_##name), description, \
                                   terrier::type::TransientValueFactory::GetDecimal(default_value), is_mutable));

#define SETTING_bool(name, description, default_value, is_mutable)                                                     \
  param_map.emplace(                                                                                                   \
      terrier::settings::Param::name,                                                                                  \
      terrier::settings::ParamInfo(#name, terrier::type::TransientValueFactory::GetBoolean(FLAGS_##name), description, \
                                   terrier::type::TransientValueFactory::GetBoolean(default_value), is_mutable));

#define SETTING_string(name, description, default_value, is_mutable)                                                   \
  param_map.emplace(                                                                                                   \
      terrier::settings::Param::name,                                                                                  \
      terrier::settings::ParamInfo(#name, terrier::type::TransientValueFactory::GetVarchar(FLAGS_##name), description, \
                                   terrier::type::TransientValueFactory::GetVarchar(default_value), is_mutable));
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
#define SETTING_int(name, description, default_value, min_value, max_value, is_mutable) name,

#define SETTING_double(name, description, default_value, min_value, max_value, is_mutable) name,

#define SETTING_bool(name, description, default_value, is_mutable) name,

#define SETTING_string(name, description, default_value, is_mutable) name,
#endif
