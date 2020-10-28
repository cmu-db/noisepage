#pragma once

#include <llvm/ADT/SmallVector.h>

#include <memory>
#include <string>
#include <vector>

#include "common/macros.h"
#include "execution/ast/ast_fwd.h"
#include "execution/compiler/executable_query.h"
#include "execution/util/region_containers.h"

namespace noisepage::execution::vm {
class Module;
}  // namespace noisepage::execution::vm

namespace noisepage::execution::compiler {

/**
 * A container for code in a single TPL file.
 */
class ExecutableQueryFragmentBuilder {
  friend class CodeGen;

 public:
  /**
   * Create a new TPL code container.
   * @param ctx The AST context to use.
   */
  explicit ExecutableQueryFragmentBuilder(ast::Context *ctx);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY(ExecutableQueryFragmentBuilder);

  /**
   * Register the given struct in this container.
   * @param decl The struct declaration.
   */
  void DeclareStruct(ast::StructDecl *decl) { structs_.push_back(decl); }

  /**
   * Declare the given function in this fragment. The provided function will not be marked for
   * direct invocation, but is available to other step functions for use.
   * @param decl The function declaration.
   */
  void DeclareFunction(ast::FunctionDecl *decl) { functions_.push_back(decl); }

  /**
   * Register all structure declarations in the provided container.
   * @param decls The list of structures to declare in this container.
   */
  template <template <typename> typename Container>
  void DeclareAll(const Container<ast::StructDecl *> &decls) {
    structs_.reserve(structs_.size() + decls.size());
    structs_.insert(structs_.end(), decls.begin(), decls.end());
  }

  /**
   * Register all functions in the provided container.
   * @param decls The list of functions to declare.
   */
  template <template <typename> typename Container>
  void DeclareAll(const Container<ast::FunctionDecl *> &decls) {
    functions_.reserve(functions_.size() + decls.size());
    functions_.insert(functions_.end(), decls.begin(), decls.end());
  }

  /**
   * Register the given function in this container;
   * @param decl The function declaration.
   */
  void RegisterStep(ast::FunctionDecl *decl);

  /**
   * Compile the code in the container.
   * @return True if the compilation was successful; false otherwise.
   */
  std::unique_ptr<ExecutableQuery::Fragment> Compile();

  /**
   * Add the teardown function to the query.
   * @param teardown_fn The teardown function to be added.
   */
  void AddTeardownFn(ast::FunctionDecl *teardown_fn) { teardown_fn_.push_back(teardown_fn); }

 private:
  // The AST context used to generate the TPL ast
  ast::Context *ctx_;
  // The list of all functions and structs.
  llvm::SmallVector<ast::StructDecl *, 16> structs_;
  llvm::SmallVector<ast::FunctionDecl *, 16> functions_;
  // The list of function steps in the fragment.
  std::vector<std::string> step_functions_;

  std::vector<ast::FunctionDecl *> teardown_fn_;
};

}  // namespace noisepage::execution::compiler
