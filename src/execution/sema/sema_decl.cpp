#include "execution/sema/sema.h"

#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace terrier::execution::sema {

void Sema::VisitVariableDecl(ast::VariableDecl *node) {
  if (CurrentScope()->LookupLocal(node->Name()) != nullptr) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kVariableRedeclared, node->Name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value
  TERRIER_ASSERT(node->HasTypeDecl() || node->HasInitialValue(),
                 "Variable has neither a type declaration or an initial "
                 "expression. This should have been caught during parsing.");

  ast::Type *declared_type = nullptr;
  ast::Type *initializer_type = nullptr;

  if (node->HasTypeDecl()) {
    declared_type = Resolve(node->TypeRepr());
  }

  if (node->HasInitialValue()) {
    initializer_type = Resolve(node->Initial());
  }

  if (declared_type == nullptr && initializer_type == nullptr) {
    return;
  }

  // If both are provided, check assignment
  if (declared_type != nullptr && initializer_type != nullptr) {
    ast::Expr *init = node->Initial();
    if (!CheckAssignmentConstraints(declared_type, &init)) {
      GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidAssignment, declared_type, initializer_type);
      return;
    }
    // If the check applied an implicit cast, reset the initializing expression
    if (init != node->Initial()) {
      node->SetInitial(init);
    }
  }

  // The type should be resolved now
  CurrentScope()->Declare(node->Name(), (declared_type != nullptr ? declared_type : initializer_type));
}

void Sema::VisitFieldDecl(ast::FieldDecl *node) { Visit(node->TypeRepr()); }

void Sema::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Resolve just the function type (not the body of the function)
  auto *func_type = Resolve(node->TypeRepr());

  if (func_type == nullptr) {
    return;
  }

  // Make declaration available
  CurrentScope()->Declare(node->Name(), func_type);

  // Now resolve the whole function
  Resolve(node->Function());
}

void Sema::VisitStructDecl(ast::StructDecl *node) {
  auto *struct_type = Resolve(node->TypeRepr());

  if (struct_type == nullptr) {
    return;
  }

  CurrentScope()->Declare(node->Name(), struct_type);
}

}  // namespace terrier::execution::sema
