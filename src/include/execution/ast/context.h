#pragma once

#include <memory>

#include "llvm/ADT/StringRef.h"

#include "execution/ast/builtins.h"
#include "execution/ast/identifier.h"
#include "execution/ast/type.h"
#include "execution/util/region.h"
#include "type/type_id.h"

namespace tpl {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace ast {

class AstNodeFactory;
class Type;

/**
 * Ast Context. Stores info about current ast.
 */
class Context {
 public:
  /**
   * Create a context
   * @param region The region to allocate memory from
   * @param error_reporter The diagnostic error reporter
   */
  Context(util::Region *region, sema::ErrorReporter *error_reporter);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Context);

  /**
   * Destructor
   */
  ~Context();

  /**
   * Return @em str as a unique string in this context
   * @param str The input string
   * @return A uniqued (interned) version of the string in this context
   */
  Identifier GetIdentifier(llvm::StringRef str);

  /**
   * Is the type with name identifier a builtin type?
   * @return A non-null pointer to the Type if a valid builtin; null otherwise
   */
  Type *LookupBuiltinType(Identifier identifier) const;

  /**
   * Is the function with name identifier a builtin function?
   * @param identifier The name of the function to check
   * @param builtin If non-null, set to the appropriate builtin enumeration
   * @return True if the function name is that of a builtin; false otherwise
   */
  bool IsBuiltinFunction(Identifier identifier, Builtin *builtin = nullptr) const;

  /**
   * Get the identifier of a builtin function
   * @param builtin builtin to find
   * @return the identifier of the builtin function
   */
  ast::Identifier GetBuiltinFunction(Builtin builtin);

  /**
   * Get the identifier of a builtin type
   * @param kind kind of the builtin to find
   * @return the identifier of the builtin
   */
  ast::Identifier GetBuiltinType(BuiltinType::Kind kind);

  // -------------------------------------------------------
  // Simple accessors
  // -------------------------------------------------------

  struct Implementation;
  /**
   * @return the implementation
   */
  Implementation *impl() const { return impl_.get(); }

  /**
   * @return the ast node factory
   */
  AstNodeFactory *node_factory() const { return node_factory_.get(); }

  /**
   * @return the error reporter
   */
  sema::ErrorReporter *error_reporter() const { return error_reporter_; }

  /**
   * @return the region used for allocation
   */
  util::Region *region() const { return region_; }

 private:
  // Region allocator for all Ast objects this context needs
  util::Region *region_;

  // Error reporter
  sema::ErrorReporter *error_reporter_;

  // The factory used for Ast nodes
  std::unique_ptr<AstNodeFactory> node_factory_;

  // Pimpl
  std::unique_ptr<Implementation> impl_;
};

}  // namespace ast
}  // namespace tpl
