#pragma once

#include <common/macros.h>
namespace terrier::settings {

using callback_fn = void (*)(void *, void *);

/*
 * SettingCallback:
 * Stores singletons and settings callbacks.
 * All settings callbacks must be implemented as static function in this class.
 */

class SettingsCallback {
 public:
  static void EmptyCallback(void *old_value UNUSED_ATTRIBUTE,
                            void *new_value UNUSED_ATTRIBUTE);
  static void MaxConnectionsCallback(void *old_value UNUSED_ATTRIBUTE,
                                     void *new_value UNUSED_ATTRIBUTE);

 private:
};

}  // namespace terrier::settings