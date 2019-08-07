#include "execution/sema/sema.h"

#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace terrier::execution::sema {

void Sema::VisitVariableDecl(ast::VariableDecl *node) {
  if (current_scope()->LookupLocal(node->name()) != nullptr) {
    error_reporter()->Report(node->position(), ErrorMessages::kVariableRedeclared, node->name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value
  TPL_ASSERT(node->HasTypeDecl() || node->HasInitialValue(),
             "Variable has neither a type declaration or an initial "
             "expression. This should have been caught during parsing.");

  ast::Type *declared_type = nullptr;
  ast::Type *initializer_type = nullptr;

  if (node->HasTypeDecl()) {
    declared_type = Resolve(node->type_repr());
  }

  if (node->HasInitialValue()) {
    initializer_type = Resolve(node->initial());
  }

  if (declared_type == nullptr && initializer_type == nullptr) {
    return;
  }

  // If both are provided, check assignment
  if (declared_type != nullptr && initializer_type != nullptr) {
    ast::Expr *init = node->initial();
    if (!CheckAssignmentConstraints(declared_type, &init)) {
      error_reporter()->Report(node->position(), ErrorMessages::kInvalidAssignment, declared_type, initializer_type);
      return;
    }
    // If the check applied an implicit cast, reset the initializing expression
    if (init != node->initial()) {
      node->set_initial(init);
    }
  }

  // The type should be resolved now
  current_scope()->Declare(node->name(), (declared_type != nullptr ? declared_type : initializer_type));
}

void Sema::VisitFieldDecl(ast::FieldDecl *node) { Visit(node->type_repr()); }

void Sema::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Resolve just the function type (not the body of the function)
  auto *func_type = Resolve(node->type_repr());

  if (func_type == nullptr) {
    return;
  }

  // Make declaration available
  current_scope()->Declare(node->name(), func_type);

  // Now resolve the whole function
  Resolve(node->function());
}

void Sema::VisitStructDecl(ast::StructDecl *node) {
  auto *struct_type = Resolve(node->type_repr());

  if (struct_type == nullptr) {
    return;
  }

  current_scope()->Declare(node->name(), struct_type);
}

}  // namespace terrier::execution::sema
