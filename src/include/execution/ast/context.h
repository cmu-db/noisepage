#pragma once

#include <llvm/ADT/StringRef.h>

#include <memory>

#include "execution/ast/builtins.h"
#include "execution/ast/identifier.h"
#include "execution/ast/type.h"
#include "execution/util/region.h"
#include "type/type_id.h"

namespace noisepage::execution {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace ast {

class AstNodeFactory;
class Type;

/**
 * A Context serves as a container that creates and owns all AST nodes during parsing and semantic
 * analysis. Contexts should not be shared across threads; they are meant to be as close as
 * possible to thread-local storage during compilation. This means that type/node/identifier pointer
 * equality cannot be relied on across different Context's; pointers doled out by this context is
 * safe.
 *
 * Because a Context owns all types, nodes, and identifiers, all these structures are destroyed when
 * the context is destroyed.
 */
class Context {
 public:
  /**
   * Create a Context that uses the injected @em error_reporter to report errors.
   * @param region The region to allocate memory from.
   * @param error_reporter The diagnostic error reporter.
   */
  Context(util::Region *region, sema::ErrorReporter *error_reporter);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Context);

  /**
   * Destructor.
   */
  ~Context();

  /**
   * Return a unique and context-owned version of the provided string. Identifiers are any string
   * that appear in TPL source code. Identical strings will map to the same AST Identifier object.
   * @param str The input string.
   * @return A uniqued (interned) version of the string in this context.
   */
  Identifier GetIdentifier(llvm::StringRef str);

  /**
   * Lookup a builtin type with name @em name in the TPL type system.
   * @return A non-null pointer to the Type if @em name is a valid builtin type; null otherwise.
   */
  Type *LookupBuiltinType(Identifier name) const;

  /**
   * Is the function with name @em name a builtin TPL function?
   * @param name The name of the function to check.
   * @param[out] builtin If non-null, set to the appropriate builtin enumeration.
   * @return True if the @em name is a builtin function; false otherwise.
   */
  bool IsBuiltinFunction(Identifier name, Builtin *builtin = nullptr) const;

  /**
   * Get the identifier of a builtin function
   * @param builtin builtin to find
   * @return the identifier of the builtin function
   */
  Identifier GetBuiltinFunction(Builtin builtin);

  /**
   * Get the identifier of a builtin type
   * @param kind kind of the builtin to find
   * @return the identifier of the builtin
   */
  Identifier GetBuiltinType(BuiltinType::Kind kind);

  // -------------------------------------------------------
  // Simple accessors
  // -------------------------------------------------------

  /**
   * PIMPL type.
   */
  struct Implementation;

  /**
   * @return The internal opaque implementation.
   */
  Implementation *Impl() const { return impl_.get(); }

  /**
   * @return The AST node factory.
   */
  AstNodeFactory *GetNodeFactory() const { return node_factory_.get(); }

  /**
   * @return The error reporter for this context.
   */
  sema::ErrorReporter *GetErrorReporter() const { return error_reporter_; }

  /**
   * @return The memory region this context uses to perform ALL allocations.
   */
  util::Region *GetRegion() const { return region_; }

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
}  // namespace noisepage::execution
