#pragma once

namespace noisepage::execution::vm {

/**
 * An enumeration capturing different execution methods and optimization levels.
 */
enum class ExecutionMode : uint8_t {
  // Always execute in interpreted mode
  Interpret,
  // Execute in interpreted mode, but trigger a compilation asynchronously. As
  // compiled code becomes available, seamlessly swap it in and execute mixed
  // interpreter and compiled code.
  Adaptive,
  // Compile and generate all machine code before executing the function
  Compiled
};

}  // namespace noisepage::execution::vm
