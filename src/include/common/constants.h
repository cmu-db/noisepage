#pragma once

#include <cstdint>
namespace terrier {
/**
 * Declare all system-level constants that cannot change at runtime here.
 *
 * To make testing easy though, it is still preferable that these are "injected"
 * i.e. explicitly taken in at construction time and given to the object from
 * a top level program (e.g. a unit test, main.cpp), instead of referred directly
 * in code.
 */
struct Constants {
  /**
   * Block size.
   */
  static const uint32_t BLOCK_SIZE = 1048576u;  // Should only ever be a power of 2.
};
}  // namespace terrier
