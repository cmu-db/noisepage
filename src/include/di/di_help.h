#pragma once
#include "boost/di/di.h"
#include "common/macros.h"
#include "common/managed_pointer.h"

#define DECLARE_ANNOTATION(name) static constexpr auto name = [] {}
namespace terrier::di {
// Effectively merges the boost::di namespace with terrier-specific helpers and wrappers
using namespace boost::di;

/**
 * Test if a type is named
 * @tparam T the type to test
 */
template <class T>
struct named : di::policies::detail::type_op {
  /**
   * see boost::di doc
   * @tparam TArg
   */
  template <class TArg>
  struct apply : di::aux::integral_constant<bool, !di::aux::is_same<di::no_name, typename TArg::name>::value> {};
};

/*
 * This policy ensures that no default values is used, and all parameters being injected are bound
 */
// TODO(Tianyu): I believe this will just ensure there is at least a bind clause for anything injected.
// Does't matter whether it's in() or to().
class StrictBindingPolicy : public di::config {
 public:
  /**
   * @param ... vararg input
   * @return strict binding policy
   */
  static auto policies(...) noexcept {
    using namespace di::policies;
    return di::make_policies(constructible(is_bound<di::_>{}));
  }
};

/*
 * This policy ensures that all named values is bound. It is okay if some values are default.
 */
class TestBindingPolicy : public di::config {
 public:
  /**
   * @param ... vararg input
   * @return strict binding policy
   */
  static auto policies(...) noexcept {
    using namespace di::policies;
    using namespace di::policies::operators;
    // Unnamed unbound variables are most likely not
    return di::make_policies(constructible(is_bound<di::_>{} || !named<di::_>{}));
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

/**
 * Custom scope for boost::di that corresponds a module in the terrier system.
 *
 * This module injects objects with lifetime the same as the injector. All injected object
 * share the same instance if injected from the same injector. Think of this as a singleton
 * that has the lifetime of the injector instead of the process.
 */
class TerrierSharedModule {
 public:
  template <class TExpected, class TGiven>
  class scope {
   public:
    // TODO(Tianyu): Not sure about this. This is the referrable flag used for boost::di's singleton scope.
    template <class T_, class>
    using is_referable = typename di::wrappers::shared<di::scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    static TerrierWrapper<TExpected, TGiven> try_create(const TProvider &);

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    TerrierWrapper<TExpected, TGiven> create(const TProvider &provider) {
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
 */
class TerrierSingleton {
 public:
  template <class TExpected, class TGiven>
  class scope {
   public:
    // TODO(Tianyu): Not sure about this. This is the referrable flag used for boost::di's singleton scope.
    template <class T_, class>
    using is_referable = typename di::wrappers::shared<di::scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    static TerrierWrapper<TExpected, TGiven> try_create(const TProvider &);

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    TerrierWrapper<TExpected, TGiven> create(const TProvider &provider) {
      static auto object(provider.get(di::type_traits::stack{}));
      return &object;
    }
  };
};

class DisabledModule {
 public:
  template <class TExpected, class TGiven>
  class scope {
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
    template <class T_, class>
    using is_referable = typename di::wrappers::shared<di::scopes::singleton, TExpected &>::template is_referable<T_>;

    /**
     * @tparam TProvider provider type
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    static TerrierDisabledWrapper try_create(const TProvider &);

    /**
     * @tparam TProvider provider type
     * @param provider provider
     * @return see boost::di doc
     */
    template <class, class, class TProvider>
    TerrierDisabledWrapper create(const TProvider &provider) {
      return {};
    }
  };
};

/**
 * Use this as the scope object to use for TerrierModule.
 *
 * Pretty much always, you should use this as the default scope over boost:di provided ones.
 */
static TerrierSharedModule UNUSED_ATTRIBUTE terrier_shared_module{};
static DisabledModule UNUSED_ATTRIBUTE disabled{};
static TerrierSingleton UNUSED_ATTRIBUTE terrier_singleton{};
}  // namespace terrier::di