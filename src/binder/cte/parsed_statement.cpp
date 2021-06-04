#include "binder/cte/parsed_statement.h"

#include <algorithm>
#include <numeric>

#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"
#include "parser/update_statement.h"

namespace noisepage::binder::cte {

ParsedStatement::ParsedStatement(common::ManagedPointer<parser::SelectStatement> root) {
  // As a first step, we build up a collection of the "raw" dependencies in the statement
  BuildContext context{};

  const auto scope = context.NextScopeId();
  AddScope(scope, 0UL);

  if (root->HasSelectTable()) {
    // Insert the target table for the SELECT
    const auto next_id = context.NextId();
    AddRef({next_id, RefType::READ, scope, END_POSITION, root->GetSelectTable()});
  }

  const auto select_with = root->GetSelectWith();
  for (const auto &table_ref : select_with) {
    const auto next_id = context.NextId();
    const std::size_t position =
        std::distance(select_with.cbegin(), std::find(select_with.cbegin(), select_with.cend(), table_ref));
    // Add the new context-sensitive table reference
    AddRef({next_id, RefType::WRITE, scope, position, table_ref});

    // Add a dependency for the root select on the WITH table
    if (root->HasSelectTable()) {
      auto &ref = GetRef({root->GetSelectTable()->GetAlias(), scopes_.at(scope), END_POSITION});
      ref.AddDependency(next_id);
    }

    // Recursively consider nested table references
    BuildFromVisit(table_ref, next_id, scope, scopes_[scope], position, &context);
  }
}

ParsedStatement::ParsedStatement(common::ManagedPointer<parser::InsertStatement> root) {}

ParsedStatement::ParsedStatement(common::ManagedPointer<parser::UpdateStatement> root) {}

ParsedStatement::ParsedStatement(common::ManagedPointer<parser::DeleteStatement> root) {}

std::size_t StructuredStatement::RefCount() const { return references_.size(); }

std::size_t StructuredStatement::ReadRefCount() const {
  return std::count_if(references_.cbegin(), references_.cend(),
                       [](const ContextSensitiveTableRef &r) { return r.Type() == RefType::READ; });
}

std::size_t StructuredStatement::WriteRefCount() const {
  return std::count_if(references_.cbegin(), references_.cend(),
                       [](const ContextSensitiveTableRef &r) { return r.Type() == RefType::WRITE; });
}

std::size_t StructuredStatement::DependencyCount() const {
  return std::transform_reduce(references_.cbegin(), references_.cend(), 0, std::plus{},
                               [](const ContextSensitiveTableRef &r) { return r.Dependencies().size(); });
}

std::size_t StructuredStatement::ScopeCount() const { return scopes_.size(); }

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
  auto it = std::find_if(references_.cbegin(), references_.cend(), [&](const ContextSensitiveTableRef &r) {
    return r.Table()->GetAlias() == alias && scopes_.at(r.Scope()) == depth && r.Position() == position &&
           r.Type() == type;
  });
  return it != references_.cend();
}

bool StructuredStatement::HasDependency(const StructuredStatement::RefDescriptor &src,
                                        const StructuredStatement::RefDescriptor &dst) const {
  if (!HasRef(src) || !HasRef(dst)) {
    return false;
  }
  const auto &src_ref = GetRef(src);
  const auto &dst_ref = GetRef(dst);
  return src_ref.Dependencies().count(dst_ref.Id()) > 0;
}

ContextSensitiveTableRef &StructuredStatement::GetRef(const RefDescriptor &ref) {
  const auto &alias = std::get<0>(ref);
  const auto &depth = std::get<1>(ref);
  const auto &position = std::get<2>(ref);
  auto it = std::find_if(references_.begin(), references_.end(), [&](const ContextSensitiveTableRef &r) {
    return r.Table()->GetAlias() == alias && scopes_.at(r.Scope()) == depth && r.Position() == position;
  });
  NOISEPAGE_ASSERT(it != references_.end(), "Use of GetRef() assumes the reference is present");
  return *it;
}

const ContextSensitiveTableRef &StructuredStatement::GetRef(const StructuredStatement::RefDescriptor &ref) const {
  const auto &alias = std::get<0>(ref);
  const auto &depth = std::get<1>(ref);
  const auto &position = std::get<2>(ref);
  auto it = std::find_if(references_.cbegin(), references_.cend(), [&](const ContextSensitiveTableRef &r) {
    return r.Table()->GetAlias() == alias && scopes_.at(r.Scope()) == depth && r.Position() == position;
  });
  NOISEPAGE_ASSERT(it != references_.cend(), "Use of GetRef() assumes the reference is present");
  return *it;
}

ContextSensitiveTableRef &StructuredStatement::GetRef(std::size_t id) {
  auto it = std::find_if(references_.begin(), references_.end(),
                         [=](const ContextSensitiveTableRef &r) { return r.Id() == id; });
  NOISEPAGE_ASSERT(it != references_.end(), "Use of GetRef() assumes the reference is present");
  return *it;
}

const ContextSensitiveTableRef &StructuredStatement::GetRef(std::size_t id) const {
  auto it = std::find_if(references_.cbegin(), references_.cend(),
                         [=](const ContextSensitiveTableRef &r) { return r.Id() == id; });
  NOISEPAGE_ASSERT(it != references_.cend(), "Use of GetRef() assumes the reference is present");
  return *it;
}

std::vector<std::size_t> StructuredStatement::Identifiers() const {
  std::vector<std::size_t> identifiers{};
  identifiers.reserve(references_.size());
  std::transform(references_.cbegin(), references_.cend(), std::back_inserter(identifiers),
                 [](const ContextSensitiveTableRef &r) { return r.Id(); });
  return identifiers;
}

void StructuredStatement::BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select, const std::size_t id,
                                         const std::size_t scope, std::size_t depth, const std::size_t position,
                                         StructuredStatement::BuildContext *context) {
  // NOTE: The `id` parameter to this function is the identifier for the
  // table reference that is (at least in part) defined by this SELECT

  if (select->HasSelectTable()) {
    // Insert the target table for the SELECT
    const auto next_id = context->NextId();
    AddRef({next_id, RefType::READ, scope, END_POSITION, select->GetSelectTable()});

    // Add this table as a dependency of the containing table reference
    auto table =
        std::find_if(references_.begin(), references_.end(), [=](ContextSensitiveTableRef &r) { return r.Id() == id; });
    NOISEPAGE_ASSERT(table != references_.cend(), "Broken Invariant");
    (*table).AddDependency(next_id);
  }

  // Recursively consider nested table references
  const auto select_with = select->GetSelectWith();
  for (const auto &table_ref : select_with) {
    const auto next_id = context->NextId();
    const std::size_t next_position =
        std::distance(select_with.cbegin(), std::find(select_with.cbegin(), select_with.cend(), table_ref));
    // Add the new context-sensitive table reference
    AddRef({next_id, RefType::WRITE, scope, next_position, table_ref});

    // Add a dependency for the SELECT on the WITH table
    if (select->HasSelectTable()) {
      auto &ref = GetRef({select->GetSelectTable()->GetAlias(), scopes_.at(scope), END_POSITION});
      ref.AddDependency(next_id);
    }

    // Recursively consider nested table references
    BuildFromVisit(table_ref, DONT_CARE_ID, scope, depth, next_position, context);
  }
}

void StructuredStatement::BuildFromVisit(common::ManagedPointer<parser::TableRef> table_ref, const std::size_t id,
                                         const std::size_t scope, std::size_t depth, const std::size_t position,
                                         StructuredStatement::BuildContext *context) {
  // NOTE: The `id` parameter to this function is the identifier
  // associated with the table reference considered in this invocation

  // Recursively consider the SELECT statement(s) that define this temporary table
  if (table_ref->HasSelect()) {
    // Enter a new scope to handle the nested SELECT
    const auto new_scope = context->NextScopeId();
    AddScope(new_scope, depth + 1);

    // Visit the SELECT and UNION SELECT (if present)
    const auto select = table_ref->GetSelect();
    BuildFromVisit(select, id, new_scope, scopes_[new_scope], position, context);
    if (select->HasUnionSelect()) {
      BuildFromVisit(select->GetUnionSelect(), id, new_scope, scopes_[new_scope], position, context);
    }
  }
}

void StructuredStatement::AddRef(const ContextSensitiveTableRef &ref) { references_.emplace_back(ref); }

void StructuredStatement::AddRef(ContextSensitiveTableRef &&ref) { references_.emplace_back(std::move(ref)); }

void StructuredStatement::AddScope(const std::size_t scope, const std::size_t depth) {
  NOISEPAGE_ASSERT(scopes_.find(scope) == scopes_.cend(), "Attempt to add duplicate scope");
  scopes_[scope] = depth;
}

const std::vector<ContextSensitiveTableRef> &StructuredStatement::References() const { return references_; }

const std::unordered_map<std::size_t, std::size_t> &StructuredStatement::Scopes() const { return scopes_; }

}  // namespace noisepage::binder::cte
