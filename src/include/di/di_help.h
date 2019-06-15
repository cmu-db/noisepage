#pragma once
#include "boost/di/di.h"
#include "common/managed_pointer.h"
#include "common/macros.h"

#define DECLARE_ANNOTATION(name) static constexpr auto name = [] {}
namespace terrier::di {
// Effectively merges the boost::di namespace with terrier-specific helpers and wrappers
using namespace boost::di;
/*
 * This policy ensures that no default values is used, and all parameters being injected are bound
 */
// TODO(Tianyu): I believe this will just ensure there is at least a bind clause for anything injected.
// Does't matter whether it's in() or to().
class StrictBinding : public di::config {
 public:
  /**
   * @param ... vararg input
   * @return struct binding policy
   */
  static auto policies(...) noexcept {
    using namespace di::policies;
    using namespace di::policies::operators;
    return di::make_policies(constructible(is_bound<di::_>{}));
  }
};

/**
 * Custom scope for boost::di that corresponds a module in the terrier system.
 *
 * This module injects objects with lifetime the same as the injector. All injected object
 * share the same instance if injected from the same injector. Think of this as a singleton
 * that has the lifetime of the injector instead of the process.
 */
class TerrierModule {
 public:
  template <class TExpected, class TGiven>
  class scope {
    /**
     * Custom wrapper that specifically allows implicit casting to terrier-approved types.
     * We allow ManagedPointers, raw pointers and constant references to modules
     */
    class custom_wrapper {
     public:
      /**
       * @param object underlying object
       */
      custom_wrapper(TExpected *object) : wrapped(object) {}  // NOLINT

      /**
       * @tparam I target managed pointer's underlying type
       * @return cast to managed pointer
       */
      template <class I, __BOOST_DI_REQUIRES(di::aux::is_convertible<TExpected *, I *>::value) = 0>
      inline operator common::ManagedPointer<I>() const noexcept {  // NOLINT
        return common::ManagedPointer<I>(wrapped);
      }

      /**
       * @return cast to raw pointer
       */
      inline operator TExpected *() const noexcept { return wrapped; }  // NOLINT

      /**
       * @return cast to constant reference
       */
      inline operator const TExpected &() const noexcept {  // NOLINT
        return *wrapped;
      }

     private:
      TExpected *wrapped;
    };

   public:
    // TODO(Tianyu): Not sure about this. This is the referrable flag used for boost::di's singleton scope.
    template <class T_, class>
    using is_referable = typename di::wrappers::shared<di::scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    static custom_wrapper try_create(const TProvider &);

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    custom_wrapper create(const TProvider &provider) {
      if (object_ == nullptr) object_ = std::unique_ptr<TGiven>(provider.get());
      return object_.get();
    }

   private:
    // TODO(Tianyu): In case we ever call this multi-threaded, it will not be same. Although I doubt
    // we will need to do this if we only inject components without starting any threads.  If that
    // is not a case, slapping a lock around it should work just fine without any performance impact.
    // (create is only called at the start when constructors are called for large modules)
    std::unique_ptr<TGiven> object_ = nullptr;
  };
};

/**
 * Custom scope for boost::di that corresponds a module disabled in the terrier system. It will
 * always inject nullptr for pointer dependencies
 */
class DisabledModule {
 public:
  template <class TExpected, class TGiven>
  class scope {
    /**
     * Custom wrapper that specifically allows implicit casting to terrier-approved types.
     * We allow ManagedPointers, raw pointers and constant references to modules
     */
    class custom_wrapper {
     public:
      /**
       * @tparam I target managed pointer's underlying type
       * @return cast to managed pointer
       */
      template <class I, __BOOST_DI_REQUIRES(di::aux::is_convertible<TExpected *, I *>::value) = 0>
      inline operator common::ManagedPointer<I>() const noexcept {  // NOLINT
        return common::ManagedPointer<I>(nullptr);
      }

      /**
       * @return cast to raw pointer
       */
      inline operator TExpected *() const noexcept { return nullptr; }  // NOLINT
    };

   public:
    // TODO(Tianyu): Not sure about this. This is the referrable flag used for boost::di's singleton scope.
    template <class T_, class>
    using is_referable = typename di::wrappers::shared<di::scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    static custom_wrapper try_create(const TProvider &);

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    custom_wrapper create(const TProvider &provider) { return {}; }
  };
};

/**
 * Use this as the scope object to use for TerrierModule.
 *
 * Pretty much always, you should use this as the default scope over boost:di provided ones.
 */
static TerrierModule UNUSED_ATTRIBUTE terrier_module{};
static DisabledModule UNUSED_ATTRIBUTE disabled_module{};

}  // namespace terrier::di