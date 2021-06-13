#pragma once

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace noisepage::common {

/**
 * The Graph class represents a simple, generic, directed
 * graph abstraction. The class is intended for use as an
 * intermediary in common graph-related problem solving.
 *
 * Typical usage might look something like:
 *  1. Translate your problem to the Graph domain
 *  2. Run one or more of the algorithms in graph_algorithm.h
 *  3. Translate the solution in the Graph domain back
 *     to your specific problem domain
 */
class Graph {
 public:
  /**
   * Construct a new, empty graph.
   */
  Graph() = default;

  /**
   * Add a vertex to the graph.
   * @param id The vertex identifier
   */
  void AddVertex(std::size_t id);

  /**
   * Add an edge to the graph.
   * @param src_id The source vertex identifier
   * @param dst_id The destination vertex identifier
   */
  void AddEdge(std::size_t src_id, std::size_t dst_id);

  /** @return The order of the graph (cardinality of the vertex set) */
  std::size_t Order() const;

  /** @return The size of the graph (cardinality of the edge set) */
  std::size_t Size() const;

  /**
   * Compute the vertex set for this graph instance.
   * The returned vertex set is unordered.
   *
   * NOTE: This member constructs a new container for
   * the returned vertex set; use sparingly.
   *
   * @return The vertex set
   */
  std::vector<std::size_t> VertexSet() const;

  /**
   * Compute the edge set for this graph instance.
   * The returned edge set is unordered.
   *
   * NOTE: This member constructs a new container for
   * the returned vertex set; use sparingly.
   *
   * @return The edge set
   */
  std::vector<std::pair<std::size_t, std::size_t>> EdgeSet() const;

  /**
   * Get the adjacent vertices for the vertex identified by `id`.
   * @pre The vertex identified by `id` is present in the graph
   * @param id The identifier for the vertex of interest
   * @return An immutable reference to the set of adjacent vertices
   */
  const std::unordered_set<std::size_t> &AdjacenciesFor(std::size_t id) const;

  /**
   * Determine if this Graph is isomorphic to `rhs`.
   * @param rhs The other Graph instance
   */
  bool operator==(const Graph &rhs) const;

  /**
   * Determine if this Graph is NOT isomorphic to `rhs`.
   * @param rhs The other Graph instance
   */
  bool operator!=(const Graph &rhs) const;

  /**
   * Construct a new Graph instance from the given edge set.
   * @param edge_set The edge set from which to construct the graph
   * @return The new graph
   */
  static Graph FromEdgeSet(const std::vector<std::pair<std::size_t, std::size_t>> &edge_set);

 private:
  /**
   * The underlying representation for the graph.
   */
  std::unordered_map<std::size_t, std::unordered_set<std::size_t>> graph_;
};

}  // namespace noisepage::common
