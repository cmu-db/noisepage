#pragma once
#include <memory>

#include "boost/di/di.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
// This is a simplifying macro for
// https://boost-experimental.github.io/di/user_guide/index.html#BOOST_DI_INJECT_TRAITS
//
// To use this macro, declare in a class:
// DECLARE_ANNOTATION(WIDTH)
// DECLARE_ANNOTATION(HEIGHT)
// Then, mark the constructor with macro:
// BOOST_DI_INJECT(A, (named = WIDTH) int width, (named = HEIGHT) int height)
//
// Now the two values can be differentiated during binding with the .named() clause.
#define DECLARE_ANNOTATION(name) static constexpr auto name = [] {};

// Force boost::di to treat ManagedPointer as a smart pointer
BOOST_DI_NAMESPACE_BEGIN
namespace aux {
/**
 * Remove ManagedPointer from given type
 * @tparam T input type
 */
template <class T>
struct remove_smart_ptr<terrier::common::ManagedPointer<T>> {
  /**
   * Re-constructed type
   */
  using type = T;
};

/**
 * Construct underlying type from ManagedPointer
 * @tparam T input type
 */
template <class T>
struct deref_type<terrier::common::ManagedPointer<T>> {
  /**
   * Re-constructed type
   */
  using type = remove_qualifiers_t<typename deref_type<T>::type>;
};
}  // namespace aux
BOOST_DI_NAMESPACE_END

namespace terrier::di {
// Effectively merges the boost::di namespace with terrier-specific helpers and wrappers
using namespace boost::di;  // NOLINT

/**
 * Test if a type is named
 * @tparam T the type to test
 */
template <class T>
struct named : policies::detail::type_op {  // NOLINT
  /**
   * see boost::di doc https://boost-experimental.github.io/di/user_guide/index.html#policies
   * @tparam TArg
   */
  template <class TArg>
  struct apply : aux::integral_constant<bool, !aux::is_same<no_name, typename TArg::name>::value> {};  // NOLINT
};

/**
 * This policy ensures that no default values is used, and all parameters being injected are bound
 * see https://boost-experimental.github.io/di/user_guide/index.html#policies
 */
// TODO(Tianyu): I believe this will just ensure there is at least a bind clause for anything injected.
// Does't matter whether it's in() or to().
class StrictBindingPolicy : public di::config {
 public:
  /**
   * @param ... vararg input
   * @return strict binding policy
   */
  static auto policies(...) noexcept {  // NOLINT
    using namespace policies;           // NOLINT
    return make_policies(constructible(is_bound<_>{}));
  }
};

/**
 * This policy ensures that all named values is bound. It is okay if some values are default.
 * see https://boost-experimental.github.io/di/user_guide/index.html#policies
 */
class TestBindingPolicy : public config {
 public:
  /**
   * @param ... vararg input
   * @return strict binding policy
   */
  static auto policies(...) noexcept {    // NOLINT
    using namespace policies;             // NOLINT
    using namespace policies::operators;  // NOLINT
    // Unnamed unbound variables are most likely not
    return di::make_policies(constructible(is_bound<_>{} || !named<_>{}));
  }
};

/**
 * Custom wrapper type that allows conversion to *, ManagedPtr and const &, in accordance
 * with the terrier code base
 * @tparam TExpected the exposed type to be injected (interface type)
 * @tparam TGiven the underlying implementation type
 */
template <class TExpected, class TGiven>
class TerrierWrapper {
 public:
  /**
   * @param object underlying object
   */
  TerrierWrapper(TExpected *object) : wrapped(object) {}  // NOLINT

  /**
   * @tparam I target managed pointer's underlying type
   * @return cast to managed pointer
   */
  template <class I, __BOOST_DI_REQUIRES(aux::is_convertible<TExpected *, I *>::value) = 0>
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
  TExpected *wrapped;  // NOLINT
};

/**
 * Custom scope for boost::di that corresponds a module in the terrier system.
 *
 * This module injects objects with lifetime the same as the injector. All injected object
 * share the same instance if injected from the same injector. Think of this as a singleton
 * that has the lifetime of the injector instead of the process.
 *
 * see https://boost-experimental.github.io/di/user_guide/index.html#scopes
 */
class TerrierSharedModule {
 public:
  /**
   * Implementation of the scope. see boost::di doc
   * @tparam TExpected
   * @tparam TGiven
   */
  template <class TExpected, class TGiven>
  class scope {  // NOLINT
   public:
    // TODO(Tianyu): Not sure if this is relevant as we prohibit injection of non-const references.
    // This is the referrable flag used for boost::di's singleton scope.
    /**
     * See https://boost-experimental.github.io/di/user_guide/index.html#scopes
     */
    template <class T_, class>
    using is_referable = typename wrappers::shared<scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see https://boost-experimental.github.io/di/user_guide/index.html#scopes
     */
    template <class, class, class TProvider>
    static TerrierWrapper<TExpected, TGiven> try_create(const TProvider &);  // NOLINT

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see https://boost-experimental.github.io/di/user_guide/index.html#scopes
     */
    template <class, class, class TProvider>
    TerrierWrapper<TExpected, TGiven> create(const TProvider &provider) {  // NOLINT
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
 * Custom scope for boost::di that corresponds a module in the terrier system.
 *
 * This module injects objects with lifetime the same as the injector. All injected object
 * share the same instance if injected from the same injector. Think of this as a singleton
 * that has the lifetime of the injector instead of the process.
 *
 * see https://boost-experimental.github.io/di/user_guide/index.html#scopes
 */
class TerrierSingleton {
 public:
  /**
   * Implementation of the scope. see boost::di doc
   * @tparam TExpected
   * @tparam TGiven
   */
  template <class TExpected, class TGiven>
  class scope {  // NOLINT
   public:
    // TODO(Tianyu): Not sure if this is relevant as we prohibit injection of non-const references.
    // This is the referrable flag used for boost::di's singleton scope.
    /**
     * See https://boost-experimental.github.io/di/user_guide/index.html#scopes
     */
    template <class T_, class>
    using is_referable = typename wrappers::shared<scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see https://boost-experimental.github.io/di/user_guide/index.html#scopes
     */
    template <class, class, class TProvider>
    static TerrierWrapper<TExpected, TGiven> try_create(const TProvider &);  // NOLINT

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see https://boost-experimental.github.io/di/user_guide/index.html#scopes
     */
    template <class, class, class TProvider>
    TerrierWrapper<TExpected, TGiven> create(const TProvider &provider) {  // NOLINT
      static auto object(provider.get(type_traits::stack{}));
      return &object;
    }
  };
};

/**
 * Injects nullptr for any component requiring a T * for bound T's. This is useful to mark a component as disabled
 * in the tests
 *
 * see https://boost-experimental.github.io/di/user_guide/index.html#scopes
 */
class DisabledModule {
 public:
  /**
   * Implementation of the scope. see boost::di doc
   * @tparam TExpected
   * @tparam TGiven
   */
  template <class TExpected, class TGiven>
  class scope {  // NOLINT
    /**
     * Custom wrapper that specifically allows implicit casting to terrier-approved types from
     * nullptr. We allow ManagedPointers, raw pointers.
     */
    class TerrierDisabledWrapper {
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
    /**
     * See boost::di doc
     */
    template <class T_, class>
    using is_referable = typename di::wrappers::shared<di::scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    static TerrierDisabledWrapper try_create(const TProvider &);  // NOLINT

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    TerrierDisabledWrapper create(const TProvider &provider) {  // NOLINT
      return {};
    }
  };
};

/**
 * Binds a type under the shared module scope.
 * Pretty much always, you should use this as the default scope over boost:di provided ones.
 */
static TerrierSharedModule UNUSED_ATTRIBUTE terrier_shared_module{};
/**
 * Disabled scope that injects nullptr
 */
static DisabledModule UNUSED_ATTRIBUTE disabled{};
/**
 * Singleton that has lifetime of the entire app (static). This is useful for e.g. random number generator
 * across tests
 */
static TerrierSingleton UNUSED_ATTRIBUTE terrier_singleton{};
}  // namespace terrier::di
