#pragma once

#include <memory>

namespace noisepage::common {

/**
 * A SanctionedSharedPtr is literally a shared pointer in every possible way.
 *
 * SanctionedSharedPtr exists to declare at a code-level that
 *   1. I have thought deeply about ownership:
 *      - Who owns the pointee?
 *      - When is the pointee created?
 *      - When should the pointee be freed?
 *   2. I have thought deeply about memory management:
 *      - I am not creating a shared pointer to "fix a memory leak" by hiding the leak under the carpet.
 *      - I am creating a shared pointer because I genuinely believe that two or more objects need ownership of the
 *        memory at the same instance in time.
 *      - I certify that there are no cycles of shared pointers.
 *   3. I am convinced that there is literally no other possible way to achieve my task except with a shared pointer.
 *
 * If and only if you can sign your name to the above points, then feel free to use the SanctionedSharedPtr.
 * Please include the answers to the above questions where you include the SanctionedSharedPtr or in a README.
 * Otherwise, please open a discussion for your issue.
 *
 * If you are using a SanctionedSharedPtr as a temporary hack, please fill in the above questions as far as is
 * possible now and open a GitHub issue to keep track of it in the future.
 *
 * If you are reviewing code, please do not let anything merge using either shared_ptr or SanctionedSharedPtr
 * without pressing hard on the above points. This is to prevent NoisePage from going the way of Peloton.
 *
 * @tparam Underlying The underlying type being wrapped by the shared pointer.
 */
template <class Underlying>
class SanctionedSharedPtr {
 public:
  /** A shared pointer. Watch out! */
  typedef std::shared_ptr<Underlying> Ptr;  // NOLINT
};
}  // namespace noisepage::common
