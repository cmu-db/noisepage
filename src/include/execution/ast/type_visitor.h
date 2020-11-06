#pragma once

#include "execution/ast/type.h"

namespace noisepage::execution::ast {

/**
 * Base class for TPL type hierarchy visitors. Uses the Curiously Recurring Template Pattern (CRTP)
 * to avoid overhead of virtual function dispatch. Made possible because we keep a static,
 * macro-based list of all possible TPL types.
 *
 * Derived classes parameterize TypeVisitor with itself, e.g.:
 *
 * @code
 * class Derived : public TypeVisitor<Derived> {
 *   ...
 * }
 * @endcode
 *
 * All type visitations will get forwarded to the derived class, if they are implemented, and falls
 * back to this base class otherwise. To easily define visitors for all nodes, use the TYPE_LIST()
 * macro providing a function generator argument.
 */
template <typename Subclass, typename RetType = void>
class TypeVisitor {
 public:
#define DISPATCH(Type) return this->Impl()->Visit##Type(static_cast<const Type *>(type));

  /**
   * Begin type traversal at the given type node.
   * @param type The type to begin traversal at.
   * @return Template-specific return type.
   */
  RetType Visit(const Type *type) {
#define GEN_VISIT_CASE(TypeClass) \
  case Type::TypeId::TypeClass:   \
    DISPATCH(TypeClass)

    // Main switch
    switch (type->GetTypeId()) {
      TYPE_LIST(GEN_VISIT_CASE)
      default:
        UNREACHABLE("Impossible node type");
    }

#undef GEN_VISIT_CASE
  }

#define GEN_VISIT_TYPE(TypeClass) \
  RetType Visit##TypeClass(const TypeClass *type) { DISPATCH(TypeClass); }
  TYPE_LIST(GEN_VISIT_TYPE)
#undef GEN_VISIT_TYPE
#undef DISPATCH

 protected:
  /** The implementation of this class. */
  Subclass *Impl() { return static_cast<Subclass *>(this); }
};

}  // namespace noisepage::execution::ast
