#include "binder/cte/structured_statement.h"

#include <algorithm>
#include <numeric>

#include "binder/cte/lexical_scope.h"
#include "binder/cte/typed_table_ref.h"
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
  root_scope_ = std::make_unique<LexicalScope>(context.NextScopeId(), 0UL);

  for (const auto &table_ref : root->GetSelectWith()) {
    // Add the table reference to its enclosing scope
    root_scope_->AddReference(TypedTableRef{table_ref, RefType::WRITE});
    // Create a new scope for the temporary table definition
    root_scope_->AddEnclosedScope(LexicalScope{context.NextScopeId(), root_scope_->Depth() + 1});
    // Visit the nested scope
    BuildFromVisit(table_ref, &root_scope_->EnclosedScopes().back(), &context);
  }

  if (root->HasSelectTable()) {
    // Insert the target table for the SELECT
    root_scope_->AddReference(TypedTableRef{root->GetSelectTable(), RefType::READ});
  }

  // Flatten the hierarchy
  FlattenTo(root_scope_.get(), &flat_scopes_);
}

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::InsertStatement> root) {}

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::UpdateStatement> root) {}

StructuredStatement::StructuredStatement(common::ManagedPointer<parser::DeleteStatement> root) {}

StructuredStatement::~StructuredStatement() = default;

void StructuredStatement::BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select, LexicalScope *scope,
                                         StructuredStatement::BuildContext *context) {
  // Recursively consider nested table references
  for (const auto &table_ref : select->GetSelectWith()) {
    // Add the table reference to its enclosing scope
    scope->AddReference(TypedTableRef{table_ref, RefType::WRITE});
    // Create a new scope for the temporary table definition
    scope->AddEnclosedScope(LexicalScope{context->NextScopeId(), scope->Depth() + 1});
    // Visit the nested scope
    BuildFromVisit(table_ref, &scope->EnclosedScopes().back(), context);
  }

  if (select->HasSelectTable()) {
    // Insert the target table for the SELECT
    scope->AddReference(TypedTableRef{select->GetSelectTable(), RefType::READ});
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

// ----------------------------------------------------------------------------
// Queries
// ----------------------------------------------------------------------------

std::size_t StructuredStatement::RefCount() const {
  return std::transform_reduce(flat_scopes_.cbegin(), flat_scopes_.cend(), 0UL, std::plus{},
                               [](const LexicalScope *s) { return s->References().size(); });
}

std::size_t StructuredStatement::ReadRefCount() const {
  std::size_t count = 0;
  for (const auto *scope : flat_scopes_) {
    count += std::count_if(scope->References().cbegin(), scope->References().cend(),
                           [](const TypedTableRef &r) { return r.Type() == RefType::READ; });
  }
  return count;
}

std::size_t StructuredStatement::WriteRefCount() const {
  std::size_t count = 0;
  for (const auto *scope : flat_scopes_) {
    count += std::count_if(scope->References().cbegin(), scope->References().cend(),
                           [](const TypedTableRef &r) { return r.Type() == RefType::WRITE; });
  }
  return count;
}

std::size_t StructuredStatement::ScopeCount() const { return flat_scopes_.size(); }

bool StructuredStatement::HasReadRef(const StructuredStatement::RefDescriptor &ref) const {
  return HasRef(ref, RefType::READ);
}

bool StructuredStatement::HasWriteRef(const StructuredStatement::RefDescriptor &ref) const {
  return HasRef(ref, RefType::WRITE);
}

bool StructuredStatement::HasRef(const StructuredStatement::RefDescriptor &ref) const {
  return HasRef(ref, RefType::READ) || HasRef(ref, RefType::WRITE);
}

bool StructuredStatement::HasRef(const StructuredStatement::RefDescriptor &ref, RefType type) const {
  const auto &alias = std::get<0>(ref);
  const auto &depth = std::get<1>(ref);
  const auto &position = std::get<2>(ref);
  for (const auto *scope : flat_scopes_) {
    if (scope->Depth() == depth) {
      for (auto it = scope->References().cbegin(); it != scope->References().cend(); ++it) {
        const std::size_t pos = std::distance(scope->References().cbegin(), it);
        if (pos == position) {
          const auto &table_ref = *it;
          if (table_ref.Table()->GetAlias() == alias && table_ref.Type() == type) {
            return true;
          }
        }
      }
    }
  }
  // Not found
  return false;
}

LexicalScope &StructuredStatement::RootScope() { return *root_scope_; }

const LexicalScope &StructuredStatement::RootScope() const { return *root_scope_; }

void StructuredStatement::FlattenTo(const LexicalScope *root, std::vector<const LexicalScope *> *result) {
  // Recursively visit each nested scope
  for (const auto &enclosed_scope : root->EnclosedScopes()) {
    FlattenTo(&enclosed_scope, result);
  }
  result->push_back(root);
}

}  // namespace noisepage::binder::cte
