#include "common/graph_algorithm.h"

#include <algorithm>

#include "common/graph.h"

namespace noisepage::common::graph {

namespace detail {
/**
 * Encodes the state of individual nodes in graph traversal algorithms.
 */
enum class NodeState {
  UNVISITED,
  IN_PROGRESS,
  VISITED,
};
}  // namespace detail

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

// namespace detail {

// /**
//  * @brief Determine if the DFS tree rooted at the current top of `to_visit` contains a cycle.
//  * @param graph The graph in which the search is performed
//  * @param node_states A map from transaction ID to the current state of the transaction in the search
//  * @param to_visit A stack of the transaction IDs that remain to be visited
//  * @param youngest_txn The out-parameter used to return the transaction ID of
//  * the youngest transaction in the first cycle found in the graph, if present
//  */
// bool HasCycleFrom(std::unordered_map<txn_id_t, std::vector<txn_id_t>> *graph,
//                                   std::unordered_map<txn_id_t, NodeState> *node_states, std::stack<txn_id_t>
//                                   *to_visit, txn_id_t *youngest_txn) {
//   auto &adjacent = (*graph)[to_visit->top()];
//   std::sort(adjacent.begin(), adjacent.end());
//   for (const auto &txn : adjacent) {
//     const auto state = (*node_states)[txn];
//     if (NodeState::IN_PROGRESS == state) {
//       *youngest_txn = YoungestTransactionInCycle(to_visit, txn);
//       return true;
//     }

//     if (NodeState::UNVISITED == state) {
//       to_visit->push(txn);
//       (*node_states)[txn] = NodeState::IN_PROGRESS;
//       if (DFSTreeHasCycle(graph, node_states, to_visit, youngest_txn)) {
//         return true;
//       }
//     }
//   }

//   (*node_states)[to_visit->top()] = NodeState::VISITED;
//   to_visit->pop();

//   // no cycles found in this DFS tree
//   return false;
// }
// }  // namespace detail

bool HasCycle(const Graph &graph) {
  // Initialize vertex states
  // std::unordered_map<std::size_t, detail::NodeState> node_states{};
  // for (const auto &id : graph.VertexSet()) {
  //   node_states[id] = detail::NodeState::UNVISITED;
  // }

  // // ensure that we always search in order of transaction ID (increasing)
  // std::vector<std::size_t> to_search{};
  // to_search.reserve();
  // std::transform(graph->cbegin(), graph->cend(), std::back_inserter(to_search), [](const auto &p) { return p.first;
  // }); std::sort(to_search.begin(), to_search.end());

  // for (const auto &txn : to_search) {
  //   if (NodeState::UNVISITED == node_states[txn]) {
  //     std::stack<txn_id_t> to_visit{};
  //     to_visit.push(txn);
  //     if (DFSTreeHasCycle(graph, &node_states, &to_visit, txn_id)) {
  //       return true;
  //     }
  //   }
  // }

  // no cycles found
  return false;
}

}  // namespace noisepage::common::graph
