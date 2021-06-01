#include "common/graph_algorithm.h"

#include <algorithm>
#include <functional>
#include <stack>

#include "common/graph.h"
#include "common/macros.h"

namespace noisepage::common::graph {

// ----------------------------------------------------------------------------
// graph::Isomorphic
// ----------------------------------------------------------------------------

bool Isomorphic(const Graph &lhs, const Graph &rhs) {
  auto lhs_nodes = lhs.VertexSet();
  auto rhs_nodes = rhs.VertexSet();
  std::sort(lhs_nodes.begin(), lhs_nodes.end());
  std::sort(rhs_nodes.begin(), rhs_nodes.end());

  // Ensure that the vertex set of each adjacency list is equivalent
  std::vector<std::size_t> difference{};
  std::set_symmetric_difference(lhs_nodes.cbegin(), lhs_nodes.cend(), rhs_nodes.cbegin(), rhs_nodes.cend(),
                                std::back_inserter(difference));
  if (!difference.empty()) {
    return false;
  }

  // Ensure the adjacencies for each node are equivalent
  for (const auto &id : lhs_nodes) {
    const auto &lhs_edges = lhs.AdjacenciesFor(id);
    const auto &rhs_edges = rhs.AdjacenciesFor(id);
    std::set_symmetric_difference(lhs_edges.cbegin(), lhs_edges.cend(), rhs_edges.cbegin(), rhs_edges.cend(),
                                  std::back_inserter(difference));
    if (!difference.empty()) {
      return false;
    }
  }

  // All edge sets are equivalent
  return true;
}

// ----------------------------------------------------------------------------
// graph::HasCycle
// ----------------------------------------------------------------------------

namespace detail {
/**
 * Encodes the state of individual nodes in cycle detection algorithm.
 */
enum class NodeState {
  UNVISITED,
  IN_PROGRESS,
  VISITED,
};

/**
 * @brief Determine if the DFS tree rooted at the current top of `to_visit` contains a cycle.
 * @param graph The graph in which the search is performed
 * @param node_states A map from transaction ID to the current state of the transaction in the search
 * @param to_visit A stack of the transaction IDs that remain to be visited
 * @param youngest_txn The out-parameter used to return the transaction ID of
 * the youngest transaction in the first cycle found in the graph, if present
 */
bool HasCycleFrom(const Graph &graph, std::unordered_map<std::size_t, NodeState> *node_states,
                  std::stack<std::size_t> *to_visit) {
  // TODO(Kyle): This is gross, sorting the vertices WITHIN the graph...
  const auto id = to_visit->top();
  std::vector<std::size_t> adjacent{graph.AdjacenciesFor(id).cbegin(), graph.AdjacenciesFor(id).cend()};
  std::sort(adjacent.begin(), adjacent.end());

  for (const auto &vertex : adjacent) {
    const auto state = (*node_states)[vertex];
    if (state == NodeState::IN_PROGRESS) {
      return true;
    }

    if (state == NodeState::UNVISITED) {
      to_visit->push(vertex);
      (*node_states)[vertex] = NodeState::IN_PROGRESS;
      if (HasCycleFrom(graph, node_states, to_visit)) {
        return true;
      }
    }
  }

  (*node_states)[to_visit->top()] = NodeState::VISITED;
  to_visit->pop();

  // No cycles found rooted at this vertex!
  return false;
}
}  // namespace detail

bool HasCycle(const Graph &graph) {
  // Initialize vertex states
  std::unordered_map<std::size_t, detail::NodeState> node_states{};
  for (const auto &id : graph.VertexSet()) {
    node_states[id] = detail::NodeState::UNVISITED;
  }

  // Ensure that we always search in order of vertex ID (increasing)
  // TODO(Kyle): May or may not actually need this functionality
  const auto vertex_set = graph.VertexSet();
  std::vector<std::size_t> to_search{vertex_set.cbegin(), vertex_set.cend()};
  std::sort(to_search.begin(), to_search.end());

  for (const auto &vertex : to_search) {
    if (node_states[vertex] == detail::NodeState::UNVISITED) {
      std::stack<std::size_t> to_visit{};
      to_visit.push(vertex);
      if (HasCycleFrom(graph, &node_states, &to_visit)) {
        return true;
      }
    }
  }

  // No cycles present!
  return false;
}

// ----------------------------------------------------------------------------
// graph::TopologicalSort
// ----------------------------------------------------------------------------

namespace detail {
/**
 * Denotes the visitation state of a node in graph traversal for topological sort.
 * NOTE: The semantics of node color are adapted from the description in CLRS.
 */
enum class NodeColor { WHITE, GRAY, BLACK };

/**
 * Helper function for recursive depth-first graph traversal.
 * @param graph The graph in which traversal is performed
 * @param vertex The vertex that is currently being visited
 * @param finishing_times The finishing times data structure
 * @param colors The colors data structure
 * @param time The time at which visitiation of the current vertex was initiated
 */
static void FinishingTimeDFSVisit(const Graph &graph, std::size_t vertex,
                                  std::unordered_map<std::size_t, std::size_t> *finishing_times,
                                  std::unordered_map<std::size_t, NodeColor> *colors, const std::size_t time) {
  (*colors)[vertex] = NodeColor::GRAY;
  for (const auto &adjacent : graph.AdjacenciesFor(vertex)) {
    if ((*colors)[adjacent] == NodeColor::WHITE) {
      FinishingTimeDFSVisit(graph, adjacent, finishing_times, colors, time + 1);
    }
  }
  (*colors)[vertex] = NodeColor::BLACK;
  (*finishing_times)[vertex] = time + 2;
}

/**
 * Perform "finishing-time depth-first search" on graph `graph`.
 *
 * NOTE: This is a special-case implementation of the DFS algorithm
 * from CLRS because we only care about finishing time for the
 * purposes of computing a topological sort. It might be better practice
 * to write a general-purpose DFS, but this is a more expedient solution.
 *
 * @param graph The input graph
 * @return A map that denotes finishing time for each vertex in `graph`
 *  Node ID -> Finishing Time
 */
static std::unordered_map<std::size_t, std::size_t> FinishingTimeDFS(const Graph &graph) {
  // The data structure we will populate
  std::unordered_map<std::size_t, std::size_t> finishing_times{};

  // Temporary data structure for the traversal
  std::unordered_map<std::size_t, NodeColor> colors{};
  for (const auto &vertex : graph.VertexSet()) {
    colors[vertex] = NodeColor::WHITE;
  }

  std::size_t time = 0;
  for (const auto &vertex : graph.VertexSet()) {
    if (colors[vertex] == NodeColor::WHITE) {
      FinishingTimeDFSVisit(graph, vertex, &finishing_times, &colors, time);
    }
  }

  return finishing_times;
}
}  // namespace detail

std::vector<std::size_t> TopologicalSort(const Graph &graph) {
  NOISEPAGE_ASSERT(!HasCycle(graph), "Graph must be acyclic to compute a topological sort");

  // Compute the finishing time for each node in the graph
  const auto finishing_times = detail::FinishingTimeDFS(graph);
  NOISEPAGE_ASSERT(finishing_times.size() == graph.Order(), "Broken Invariant");

  // Compute the topological sort from finishing times;
  // topological sort is determined by reverse order of finishing time
  const auto vertex_set = graph.VertexSet();
  std::vector<std::size_t> topological_sort{vertex_set.cbegin(), vertex_set.cend()};
  std::sort(topological_sort.begin(), topological_sort.end(),
            [&finishing_times](const std::size_t &a_id, const std::size_t &b_id) {
              return finishing_times.at(a_id) > finishing_times.at(b_id);
            });
  return topological_sort;
}

}  // namespace noisepage::common::graph
