#pragma once

#include "execution/ast/type.h"

namespace terrier::execution::ast {

/**
 * Generic visitor for type hierarchies.
 */
template <typename Impl, typename RetType = void>
class TypeVisitor {
 public:
#define DISPATCH(Type) return static_cast<Impl *>(this)->Visit##Type(static_cast<const Type *>(type));

  /**
   * Visits an arbitrary type, i.e., begins type traversal at the given type node.
   * @param type The type to visit (begin traversal at).
   * @return Template-specific return type, i.e., return value of the visitor (usually void).
   */
  RetType Visit(const Type *type) {
    switch (type->GetTypeId()) {
      default: {
        llvm_unreachable("Impossible node type");
      }
#define T(TypeClass)            \
  case Type::TypeId::TypeClass: \
    DISPATCH(TypeClass)
        TYPE_LIST(T)
#undef T
    }
  }

  /**
   * Visitor for an abstract type node, which does nothing.
   * @param type The type to visit.
   * @return Default return type (usually void), a no-arg constructed return.
   */
  RetType VisitType(UNUSED_ATTRIBUTE const Type *type) { return RetType(); }

#define T(Type) \
  RetType Visit##Type(const Type *type) { DISPATCH(Type); }
  TYPE_LIST(T)
#undef T

#undef DISPATCH
};

}  // namespace terrier::execution::ast
