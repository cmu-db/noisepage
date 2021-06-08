#include "binder/cte/structured_statement.h"

#include <algorithm>
#include <numeric>

#include "binder/cte/context_sensitive_table_ref.h"
#include "binder/cte/lexical_scope.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"
#include "parser/update_statement.h"

namespace noisepage::binder::cte {

// ----------------------------------------------------------------------------
// Construction
// ----------------------------------------------------------------------------

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::SelectStatement> root) {
  BuildContext context{};

  // Construct the root scope
  root_scope_ = std::make_unique<LexicalScope>(context.NextScopeId(), 0UL, LexicalScope::GLOBAL_SCOPE);

  for (const auto &table_ref : root->GetSelectWith()) {
    // Create a new scope for the temporary table definition
    auto scope = std::make_unique<LexicalScope>(context.NextScopeId(), root_scope_->Depth() + 1, root_scope_.get());

    // Add the scope and the corresponding reference to the enclosing scope
    root_scope_->AddReference(
        std::make_unique<ContextSensitiveTableRef>(table_ref, RefType::WRITE, root_scope_.get(), scope.get()));
    root_scope_->AddEnclosedScope(std::move(scope));

    // Visit the nested scope
    BuildFromVisit(table_ref, root_scope_->EnclosedScopes().back().get(), &context);
  }

  if (root->HasSelectTable()) {
    // Insert the target table for the SELECT
    root_scope_->AddReference(
        std::make_unique<ContextSensitiveTableRef>(root->GetSelectTable(), RefType::READ, root_scope_.get()));
  }

  // Flatten the hierarchy
  FlattenTo(root_scope_.get(), &flat_scopes_);

  NOISEPAGE_ASSERT(InvariantsSatisfied(*root_scope_), "Broken Invariant");
}

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::InsertStatement> root) {}

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::UpdateStatement> root) {}

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::DeleteStatement> root) {}

StructuredStatement::~StructuredStatement() = default;

void StructuredStatement::BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select, LexicalScope *scope,
                                         StructuredStatement::BuildContext *context) {
  // Recursively consider nested table references
  for (const auto &table_ref : select->GetSelectWith()) {
    // Create a new scope for the temporary table definition
    auto new_scope = std::make_unique<LexicalScope>(context->NextScopeId(), scope->Depth() + 1, scope);

    // Add the scope and the corresponding reference to the enclosing scope
    scope->AddReference(std::make_unique<ContextSensitiveTableRef>(table_ref, RefType::WRITE, scope, new_scope.get()));
    scope->AddEnclosedScope(std::move(new_scope));

    // Visit the nested scope
    BuildFromVisit(table_ref, scope->EnclosedScopes().back().get(), context);
  }

  if (select->HasSelectTable()) {
    // Insert the target table for the SELECT
    scope->AddReference(std::make_unique<ContextSensitiveTableRef>(select->GetSelectTable(), RefType::READ, scope));
  }
}

void StructuredStatement::BuildFromVisit(common::ManagedPointer<parser::TableRef> table_ref, LexicalScope *scope,
                                         StructuredStatement::BuildContext *context) {
  // Recursively consider the SELECT statement(s) that define this temporary table
  if (table_ref->HasSelect()) {
    BuildFromVisit(table_ref->GetSelect(), scope, context);
    if (table_ref->GetSelect()->HasUnionSelect()) {
      BuildFromVisit(table_ref->GetSelect()->GetUnionSelect(), scope, context);
    }
  }
}

bool StructuredStatement::InvariantsSatisfied(const LexicalScope &scope) {
  // Ensure that there is a one-to-one mapping between WRITE
  // table references in this scope and enclosed scopes;
  // the WRITE table references are what define enclosed scopes!
  const auto satisfied = scope.WriteRefCount() == scope.EnclosedScopes().size();
  return satisfied &&
         std::all_of(scope.EnclosedScopes().cbegin(), scope.EnclosedScopes().cend(),
                     [](const std::unique_ptr<LexicalScope> &scope) { return InvariantsSatisfied(*scope); });
}

// ----------------------------------------------------------------------------
// Queries
// ----------------------------------------------------------------------------

std::size_t StructuredStatement::RefCount() const {
  return std::transform_reduce(flat_scopes_.cbegin(), flat_scopes_.cend(), 0UL, std::plus{},
                               [](const LexicalScope *s) { return s->References().size(); });
}

std::size_t StructuredStatement::RefCount(const RefDescriptor &ref) const {
  return ReadRefCount(ref) + WriteRefCount(ref);
}

std::size_t StructuredStatement::ReadRefCount() const { return RefCountWithType(RefType::READ); }

std::size_t StructuredStatement::ReadRefCount(const RefDescriptor &ref) const { return RefCount(ref, RefType::READ); }

std::size_t StructuredStatement::WriteRefCount() const { return RefCountWithType(RefType::WRITE); }

std::size_t StructuredStatement::WriteRefCount(const RefDescriptor &ref) const { return RefCount(ref, RefType::WRITE); }

std::size_t StructuredStatement::RefCountWithType(RefType type) const {
  std::size_t count = 0;
  for (const auto *scope : flat_scopes_) {
    count += std::count_if(scope->References().cbegin(), scope->References().cend(),
                           [=](const std::unique_ptr<ContextSensitiveTableRef> &r) { return r->Type() == type; });
  }
  return count;
}

std::size_t StructuredStatement::ScopeCount() const { return flat_scopes_.size(); }

bool StructuredStatement::HasReadRef(const StructuredStatement::RefDescriptor &ref) const {
  return RefCount(ref, RefType::READ) > 0UL;
}

bool StructuredStatement::HasWriteRef(const StructuredStatement::RefDescriptor &ref) const {
  return RefCount(ref, RefType::WRITE) > 0UL;
}

bool StructuredStatement::HasRef(const StructuredStatement::RefDescriptor &ref) const {
  return HasReadRef(ref) || HasWriteRef(ref);
}

std::size_t StructuredStatement::RefCount(const StructuredStatement::RefDescriptor &ref, RefType type) const {
  const auto &alias = std::get<0>(ref);
  const auto &depth = std::get<1>(ref);
  const auto &position = std::get<2>(ref);

  std::size_t count = 0;
  for (const auto *scope : flat_scopes_) {
    if (scope->Depth() == depth) {
      for (auto it = scope->References().cbegin(); it != scope->References().cend(); ++it) {
        const std::size_t pos = std::distance(scope->References().cbegin(), it);
        if (pos == position) {
          const auto &table_ref = *it;
          if (table_ref->Table()->GetAlias() == alias && table_ref->Type() == type) {
            ++count;
          }
        }
      }
    }
  }
  return count;
}

LexicalScope &StructuredStatement::RootScope() { return *root_scope_; }

const LexicalScope &StructuredStatement::RootScope() const { return *root_scope_; }

std::vector<ContextSensitiveTableRef *> StructuredStatement::MutableReferences() {
  std::vector<ContextSensitiveTableRef *> references{};
  for (auto *scope : flat_scopes_) {
    std::transform(scope->References().begin(), scope->References().end(), std::back_inserter(references),
                   [](std::unique_ptr<ContextSensitiveTableRef> &r) { return r.get(); });
  }
  return references;
}

std::vector<const ContextSensitiveTableRef *> StructuredStatement::References() const {
  std::vector<const ContextSensitiveTableRef *> references{};
  for (const auto *scope : flat_scopes_) {
    std::transform(scope->References().cbegin(), scope->References().cend(), std::back_inserter(references),
                   [](const std::unique_ptr<ContextSensitiveTableRef> &r) { return r.get(); });
  }
  return references;
}

void StructuredStatement::FlattenTo(LexicalScope *root, std::vector<LexicalScope *> *result) {
  // Recursively visit each nested scope
  for (auto &enclosed_scope : root->EnclosedScopes()) {
    FlattenTo(enclosed_scope.get(), result);
  }
  result->push_back(root);
}

}  // namespace noisepage::binder::cte
