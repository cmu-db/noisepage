#include "binder/binder_cte_util.h"

#include <algorithm>
#include <iostream>
#include <numeric>

#include "common/graph.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"
#include "parser/update_statement.h"

namespace noisepage::binder {
namespace detail {
// ----------------------------------------------------------------------------
// ContextSensitiveTableRef
// ----------------------------------------------------------------------------

ContextSensitiveTableRef::ContextSensitiveTableRef(const std::size_t id, const RefType type, const std::size_t depth,
                                                   const std::size_t position,
                                                   common::ManagedPointer<parser::TableRef> table)
    : id_{id}, type_{type}, depth_{depth}, position_{position}, table_{table} {}

void ContextSensitiveTableRef::AddDependency(const std::size_t id) { dependencies_.insert(id); }
}  // namespace detail

// ----------------------------------------------------------------------------
// TableDependencyGraph
// ----------------------------------------------------------------------------

TableDependencyGraph::TableDependencyGraph(common::ManagedPointer<parser::SelectStatement> root) {
  BuildContext context{};
  const auto select_with = root->GetSelectWith();
  for (const auto &table_ref : select_with) {
    const auto next_id = context.NextId();
    const auto position =
        std::distance(select_with.cbegin(), std::find(select_with.cbegin(), select_with.cend(), table_ref));
    // Add the new context-sensitive table reference
    graph_.emplace_back(next_id, detail::RefType::WRITE, 0UL, position, table_ref);
    // Recursively consider nested table references
    BuildFromVisit(table_ref, next_id, 0UL, position, &context);
  }
}

TableDependencyGraph::TableDependencyGraph(common::ManagedPointer<parser::InsertStatement> root) {}

TableDependencyGraph::TableDependencyGraph(common::ManagedPointer<parser::UpdateStatement> root) {}

TableDependencyGraph::TableDependencyGraph(common::ManagedPointer<parser::DeleteStatement> root) {}

std::size_t TableDependencyGraph::Order() const { return graph_.size(); }

std::size_t TableDependencyGraph::Size() const {
  return std::transform_reduce(graph_.cbegin(), graph_.cend(), 0UL, std::plus{},
                               [](const detail::ContextSensitiveTableRef &r) { return r.Dependencies().size(); });
}

bool TableDependencyGraph::HasVertex(const Vertex &vertex) const {
  return std::any_of(graph_.cbegin(), graph_.cend(), [&vertex](const detail::ContextSensitiveTableRef &r) {
    return r.Table()->GetAlias() == vertex.Alias() && r.Depth() == vertex.Depth() && r.Position() == vertex.Position();
  });
}

bool TableDependencyGraph::HasEdge(const Edge &edge) const {
  if (!HasVertex(edge.Source()) || !HasVertex(edge.Destination())) {
    return false;
  }
  auto *src = GetVertex(edge.Source());
  auto *dst = GetVertex(edge.Destination());
  return src->Dependencies().count(dst->Id()) > 0;
}

std::vector<std::size_t> TableDependencyGraph::Identifiers() const {
  std::vector<std::size_t> identifiers{};
  identifiers.reserve(graph_.size());
  std::transform(graph_.cbegin(), graph_.cend(), std::back_inserter(identifiers),
                 [](const detail::ContextSensitiveTableRef &r) { return r.Id(); });
  return identifiers;
}

void TableDependencyGraph::TableDependencyGraph::BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select,
                                                                const std::size_t id, const std::size_t depth,
                                                                const std::size_t position,
                                                                TableDependencyGraph::BuildContext *context) {
  // NOTE: The `id` parameter to this function is the identifier for the
  // table reference that is (at least in part) defined by this SELECT

  if (select->HasSelectTable()) {
    // Insert the target table for the SELECT
    const auto next_id = context->NextId();
    // The enclosing TableRef increments the depth as we descend into
    // this function, but we consider the SELECT target at the same
    // scope as the enclosing table reference
    const auto next_depth = depth - 1;
    graph_.emplace_back(next_id, detail::RefType::READ, next_depth, position, select->GetSelectTable());

    // Add this table as a dependency of the containing table reference
    auto table =
        std::find_if(graph_.begin(), graph_.end(), [=](detail::ContextSensitiveTableRef &r) { return r.Id() == id; });
    NOISEPAGE_ASSERT(table != graph_.cend(), "Broken Invariant");
    (*table).AddDependency(next_id);
  }

  // Recursively consider nested table references
  const auto select_with = select->GetSelectWith();
  for (const auto &table_ref : select_with) {
    const auto next_id = context->NextId();
    const auto next_position =
        std::distance(select_with.cbegin(), std::find(select_with.cbegin(), select_with.cend(), table_ref));
    // Add the new context-sensitive table reference
    graph_.emplace_back(next_id, detail::RefType::WRITE, depth, next_position, table_ref);
    // Recursively consider nested table references
    BuildFromVisit(table_ref, 0UL, depth, next_position, context);
  }
}

void TableDependencyGraph::BuildFromVisit(common::ManagedPointer<parser::TableRef> table_ref, const std::size_t id,
                                          const std::size_t depth, const std::size_t position,
                                          TableDependencyGraph::BuildContext *context) {
  // NOTE: The `id` parameter to this function is the identifier
  // associated with the table reference considered in this invocation

  // Recursively consider the SELECT statement(s) that define this temporary table
  if (table_ref->HasSelect()) {
    const auto select = table_ref->GetSelect();
    BuildFromVisit(select, id, depth + 1, position, context);
    if (select->HasUnionSelect()) {
      BuildFromVisit(select->GetUnionSelect(), id, depth + 1, position, context);
    }
  }
}

bool TableDependencyGraph::CheckAll() const { return true; }

bool TableDependencyGraph::CheckForwardReferences() const { return true; }

bool TableDependencyGraph::CheckMutualRecursion() const { return true; }

bool TableDependencyGraph::CheckNestedScopes() const {
  // Checking for nested-scope dependencies boils down to
  // checking each edge in the graph and verifying that it
  // is NEVER the case that the source vertex has a depth
  // that is strictly less than the destination

  for (const auto &src : graph_) {
    for (const auto id : src.Dependencies()) {
      const auto equivalence_class = WriteEquivalenceClassFor(id);
      // NOTE: std::all_of() is vacuously true for an empty container
      if (equivalence_class.size() > 0UL &&
          std::all_of(equivalence_class.cbegin(), equivalence_class.cend(),
                      [&](const std::size_t i) { return src.Depth() < GetRefWithId(i).Depth(); })) {
        return false;
      }
    }
  }
  return true;
}

const detail::ContextSensitiveTableRef *TableDependencyGraph::GetVertex(const Vertex &vertex) const {
  auto it = std::find_if(graph_.begin(), graph_.end(), [&vertex](const detail::ContextSensitiveTableRef &r) {
    return r.Table()->GetAlias() == vertex.Alias() && r.Depth() == vertex.Depth() && r.Position() == vertex.Position();
  });
  return (it == graph_.end()) ? nullptr : std::addressof(*it);
}

std::pair<const detail::ContextSensitiveTableRef *, const detail::ContextSensitiveTableRef *>
TableDependencyGraph::GetEdge(const Edge &edge) const {
  return std::make_pair(GetVertex(edge.Source()), GetVertex(edge.Destination()));
}

const detail::ContextSensitiveTableRef &TableDependencyGraph::GetRefWithId(std::size_t id) const {
  const auto it = std::find_if(graph_.cbegin(), graph_.cend(),
                               [=](const detail::ContextSensitiveTableRef &r) { return r.Id() == id; });
  NOISEPAGE_ASSERT(it != graph_.cend(), "Broken Invariant");
  return *it;
}

std::vector<std::size_t> TableDependencyGraph::ReadEquiavlenceClassFor(const std::size_t id) const { return {}; }

std::vector<std::size_t> TableDependencyGraph::WriteEquivalenceClassFor(const std::size_t id) const {
  const auto &ref = GetRefWithId(id);
  std::vector<std::size_t> equivalence_class{};
  for (const auto &table_ref : graph_) {
    if (table_ref.Table() == ref.Table() && table_ref.Type() == detail::RefType::WRITE) {
      equivalence_class.push_back(table_ref.Id());
    }
  }
  return equivalence_class;
}

std::vector<const detail::ContextSensitiveTableRef *> TableDependencyGraph::ReadReferences() const {
  std::vector<const detail::ContextSensitiveTableRef *> refs{};
  for (const auto &ref : graph_) {
    if (ref.Type() == detail::RefType::READ) {
      refs.push_back(&ref);
    }
  }
  return refs;
}

/** @return The set fo write references from the underlying graph */
std::vector<const detail::ContextSensitiveTableRef *> TableDependencyGraph::WriteReferences() const {
  std::vector<const detail::ContextSensitiveTableRef *> refs{};
  for (const auto &ref : graph_) {
    if (ref.Type() == detail::RefType::WRITE) {
      refs.push_back(&ref);
    }
  }
  return refs;
}

// ----------------------------------------------------------------------------
// Top-Level, Exported Functions
// ----------------------------------------------------------------------------

std::vector<common::ManagedPointer<parser::TableRef>> BinderCteUtil::GetSelectWithOrder(
    common::ManagedPointer<parser::SelectStatement> select_statement) {
  return {};
}

std::vector<common::ManagedPointer<parser::TableRef>> BinderCteUtil::GetInsertWithOrder(
    common::ManagedPointer<parser::InsertStatement> insert_statement) {
  throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... INSERT Not Implemeneted");
}

std::vector<common::ManagedPointer<parser::TableRef>> BinderCteUtil::GetUpdateWithOrder(
    common::ManagedPointer<parser::UpdateStatement> update_statement) {
  throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... UPDATE Not Implemeneted");
}

std::vector<common::ManagedPointer<parser::TableRef>> BinderCteUtil::GetDeleteWithOrder(
    common::ManagedPointer<parser::DeleteStatement> delete_statement) {
  throw NOT_IMPLEMENTED_EXCEPTION("Statement Dependency Analysis for WITH ... DELETE Not Implemeneted");
}
}  // namespace noisepage::binder
