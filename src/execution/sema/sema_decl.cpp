#include <llvm/ADT/DenseSet.h>

#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/sema.h"

namespace terrier::execution::sema {

void Sema::VisitVariableDecl(ast::VariableDecl *node) {
  if (GetCurrentScope()->LookupLocal(node->Name()) != nullptr) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kVariableRedeclared, node->Name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value.
  TERRIER_ASSERT(node->HasTypeDecl() || node->HasInitialValue(),
                 "Variable has neither a type declaration or an initial expression. This should have "
                 "been caught during parsing.");

  ast::Type *declared_type = nullptr, *initializer_type = nullptr;

  if (node->HasTypeDecl()) {
    declared_type = Resolve(node->TypeRepr());
  }

  if (node->HasInitialValue()) {
    initializer_type = Resolve(node->Initial());
  }

  // If neither are resolved, it's an error.
  if (declared_type == nullptr && initializer_type == nullptr) {
    return;
  }

  if (declared_type != nullptr && initializer_type != nullptr) {
    // If both type declarations are provided, check assignment.
    ast::Expr *init = node->Initial();
    if (!CheckAssignmentConstraints(declared_type, &init)) {
      GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidAssignment, declared_type, initializer_type);
      return;
    }
    // If the check applied an implicit cast, reset the initializing expression.
    if (init != node->Initial()) {
      node->SetInitial(init);
    }
  } else if (initializer_type != nullptr) {
    // Both type declarations are not provided, but the initial value has a
    // resolved type. Let's check it now.
    if (initializer_type->IsNilType()) {
      GetErrorReporter()->Report(node->Position(), ErrorMessages::kUseOfUntypedNil);
      return;
    }
  }

  ast::Type *resolved_type = (declared_type != nullptr ? declared_type : initializer_type);
  GetCurrentScope()->Declare(node->Name(), resolved_type);
}

void Sema::VisitFieldDecl(ast::FieldDecl *node) { Visit(node->TypeRepr()); }

namespace {

// Return true if the given list of field declarations contains a duplicate name. If so, the 'dup'
// output parameter is set to the offending field. Otherwise, return false;
bool HasDuplicatesNames(const util::RegionVector<ast::FieldDecl *> &fields, const ast::FieldDecl **dup) {
  llvm::SmallDenseSet<ast::Identifier, 32> seen;
  for (const auto *field : fields) {
    // Attempt to insert into the set. If the insertion succeeds it's a unique
    // name. If the insertion fails, it's a duplicate name.
    const bool inserted = seen.insert(field->Name()).second;
    if (!inserted) {
      *dup = field;
      return true;
    }
  }
  return false;
}

}  // namespace

void Sema::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Resolve just the function type (not the body of the function).
  ast::Type *func_type = Resolve(node->TypeRepr());

  if (func_type == nullptr) {
    return;
  }

  // Check for duplicate parameter names.
  if (const ast::FieldDecl *dup = nullptr;
      HasDuplicatesNames(node->TypeRepr()->As<ast::FunctionTypeRepr>()->Parameters(), &dup)) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kDuplicateArgName, dup->Name(), node->Name());
    return;
  }

  // Make declaration available.
  GetCurrentScope()->Declare(node->Name(), func_type);

  // Now resolve the whole function.
  Resolve(node->Function());
}

void Sema::VisitStructDecl(ast::StructDecl *node) {
  ast::Type *struct_type = Resolve(node->TypeRepr());

  if (struct_type == nullptr) {
    return;
  }

  // Check for duplicate fields.
  if (const ast::FieldDecl *dup = nullptr;
      HasDuplicatesNames(node->TypeRepr()->As<ast::StructTypeRepr>()->Fields(), &dup)) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kDuplicateStructFieldName, dup->Name(), node->Name());
    return;
  }

  // Make the declaration available.
  GetCurrentScope()->Declare(node->Name(), struct_type);
}

}  // namespace terrier::execution::sema
