#pragma once

#include "execution/ast/type.h"

namespace terrier::execution::ast {

/**
 * Generic visitor for type hierarchies
 */
template <typename Impl, typename RetType = void>
class TypeVisitor {
 public:
#define DISPATCH(Type) return static_cast<Impl *>(this)->Visit##Type(static_cast<const Type *>(type));

  /**
   * Visits an arbitrary type
   * @param type type to visit
   * @return return value of the visitor (usually void)
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
   * Visitor for an abstract type, which does nothing
   * @param type type to visit
   * @return default return type (usually void)
   */
  RetType VisitType(UNUSED_ATTRIBUTE const Type *type) { return RetType(); }

#define T(Type) \
  RetType Visit##Type(const Type *type) { DISPATCH(Type); }
  TYPE_LIST(T)
#undef T

#undef DISPATCH
};

}  // namespace terrier::execution::ast
