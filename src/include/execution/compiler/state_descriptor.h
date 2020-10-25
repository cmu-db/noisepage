#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "common/macros.h"
#include "execution/ast/ast_fwd.h"
#include "execution/ast/identifier.h"

namespace noisepage::execution::compiler {

class CodeGen;

/**
 * Encapsulates some state in a TPL struct. Typically there is a "build" phase where operators may
 * declare named entries through DeclareStateEntry(), after which the state is "sealed" marking it
 * as frozen. After the state has been sealed, it is immutable.
 *
 * Accessing the state is done through opaque identifiers returned through DeclareStructEntry(). It
 * is not possible, nor should it ever be possible, to reference a state member through name. This
 * is because StateManager is allowed to rename the entries it contains to ensure uniqueness.
 */
class StateDescriptor {
 public:
  /** An instance of this state descriptor. */
  using InstanceProvider = std::function<ast::Expr *(CodeGen *)>;

  /**
   * Reference to an entry in a given state.
   */
  class Entry {
   public:
    /**
     * An invalid entry.
     */
    Entry() = default;

    /**
     * A reference to an entry in the provide state instance.
     * @param desc The state descriptor instance.
     * @param member The entry slot.
     */
    Entry(StateDescriptor *desc, ast::Identifier member) : desc_(desc), member_(member) {}

    /**
     * @return True if this entry reference is valid; false otherwise.
     */
    bool IsValid() const { return desc_ != nullptr; }

    /**
     * @return The value of entry in its state.
     */
    ast::Expr *Get(CodeGen *codegen) const;

    /**
     * @return A pointer to this entry in its state.
     */
    ast::Expr *GetPtr(CodeGen *codegen) const;

    /**
     * @return The byte offset of this entry from the state it belongs to.
     */
    ast::Expr *OffsetFromState(CodeGen *codegen) const;

   private:
    // The state.
    StateDescriptor *desc_{nullptr};
    // The member in the state to reference.
    ast::Identifier member_{};
  };

  /**
   * Create a new empty state using the provided name for the final constructed TPL type. The
   * provided state accessor can be used to load an instance of this state in a given context.
   * @param type_name The name to give the final constructed type for this state.
   * @param access A generic accessor to an instance of this state, used to access state elements.
   */
  StateDescriptor(ast::Identifier type_name, InstanceProvider access);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(StateDescriptor);

  /**
   * Declare a state entry with the provided name and type in the execution runtime query state.
   * @param codegen The code generation instance.
   * @param name The name of the element.
   * @param type_repr The TPL type representation of the element.
   * @return The slot where the inserted state exists.
   */
  Entry DeclareStateEntry(CodeGen *codegen, const std::string &name, ast::Expr *type_repr);

  /**
   * Seal the state and build the final structure. After this point, additional state elements
   * cannot be added.
   * @param codegen The code generation instance.
   * @return The finalized structure declaration.
   */
  ast::StructDecl *ConstructFinalType(CodeGen *codegen);

  /**
   * @return The query state pointer from the current code generation context.
   */
  ast::Expr *GetStatePointer(CodeGen *codegen) const { return access_(codegen); }

  /**
   * @return The name of this state's constructed TPL type.
   */
  ast::Identifier GetTypeName() const { return name_; }

  /**
   * @return The finalized type of the runtime query state; null if the state hasn't been finalized.
   */
  ast::StructDecl *GetType() const { return state_type_; }

  /**
   * @return The size of the constructed state type, in bytes. This is only possible
   */
  std::size_t GetSize() const;

 private:
  // Metadata for a single state entry.
  struct SlotInfo {
    // The unique name of the element in the state.
    ast::Identifier name_;
    // The type representation for the state.
    ast::Expr *type_repr_;
    // Constructor.
    SlotInfo(ast::Identifier name, ast::Expr *type_repr) : name_(name), type_repr_(type_repr) {}
  };

 private:
  // The name of the state type.
  ast::Identifier name_;
  // State access object.
  InstanceProvider access_;
  // All state metadata
  std::vector<SlotInfo> slots_;
  // The finalized type
  ast::StructDecl *state_type_;
};

}  // namespace noisepage::execution::compiler
