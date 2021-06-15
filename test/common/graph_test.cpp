#include "common/graph.h"

#include "test_util/test_harness.h"

namespace noisepage {

using common::Graph;

class GraphTest : public TerrierTest {};

// ----------------------------------------------------------------------------
// Test Utilities
// ----------------------------------------------------------------------------

/**
 * Determine if two vertex sets are equal.
 * @param a Input vertex set
 * @param b Input vertex set
 * @return `true` if the vertex sets are equal, `false` otherwise
 */
static bool VertexSetsEqual(const std::vector<std::size_t> &a, const std::vector<std::size_t> &b) {
  std::vector<std::size_t> local_a{a.cbegin(), a.cend()};
  std::vector<std::size_t> local_b{b.cbegin(), b.cend()};
  std::sort(local_a.begin(), local_a.end());
  std::sort(local_b.begin(), local_b.end());
  return (local_a.size() == local_b.size()) && std::equal(local_a.cbegin(), local_a.cend(), local_a.cbegin());
}

/**
 * Determine if two edge sets are equal.
 * @param a Input edge set
 * @param b Input edge set
 * @return `true` if the edge sets are equal, `false` otherwise
 */
static bool EdgeSetsEqual(const std::vector<std::pair<std::size_t, std::size_t>> &a,
                          const std::vector<std::pair<std::size_t, std::size_t>> &b) {
  std::vector<std::pair<std::size_t, std::size_t>> local_a{a.cbegin(), a.cend()};
  std::vector<std::pair<std::size_t, std::size_t>> local_b{b.cbegin(), b.cend()};
  std::sort(local_a.begin(), local_a.end());
  std::sort(local_b.begin(), local_b.end());
  return (local_a.size() == local_b.size()) && std::equal(local_a.cbegin(), local_a.cend(), local_b.cbegin());
}

// ----------------------------------------------------------------------------
// Graph Construction
// ----------------------------------------------------------------------------

TEST_F(GraphTest, Construction0) {
  Graph g{};

  std::vector<std::size_t> expected_vertex_set{};
  std::vector<std::pair<std::size_t, std::size_t>> expected_edge_set{};

  EXPECT_EQ(g.Order(), 0UL);
  EXPECT_EQ(g.Size(), 0UL);

  EXPECT_TRUE(VertexSetsEqual(g.VertexSet(), expected_vertex_set));
  EXPECT_TRUE(EdgeSetsEqual(g.EdgeSet(), expected_edge_set));
}

TEST_F(GraphTest, Construction1) {
  Graph g{};

  g.AddEdge(0, 1);

  std::vector<std::size_t> expected_vertex_set{0, 1};
  std::vector<std::pair<std::size_t, std::size_t>> expected_edge_set{{0, 1}};

  EXPECT_EQ(g.Order(), 2UL);
  EXPECT_EQ(g.Size(), 1UL);

  EXPECT_TRUE(VertexSetsEqual(g.VertexSet(), expected_vertex_set));
  EXPECT_TRUE(EdgeSetsEqual(g.EdgeSet(), expected_edge_set));
}

TEST_F(GraphTest, Construction2) {
  Graph g{};

  g.AddEdge(0, 1);
  g.AddEdge(1, 0);
  g.AddVertex(2);

  std::vector<std::size_t> expected_vertex_set{0, 1, 2};
  std::vector<std::pair<std::size_t, std::size_t>> expected_edge_set{{0, 1}, {1, 0}};

  EXPECT_EQ(g.Order(), 3UL);
  EXPECT_EQ(g.Size(), 2UL);

  EXPECT_TRUE(VertexSetsEqual(g.VertexSet(), expected_vertex_set));
  EXPECT_TRUE(EdgeSetsEqual(g.EdgeSet(), expected_edge_set));
}

TEST_F(GraphTest, Construction3) {
  const auto a = Graph::FromEdgeSet({{0, 1}, {1, 2}});

  Graph b{};
  b.AddEdge(0, 1);
  b.AddEdge(1, 2);

  EXPECT_EQ(a.Order(), 3UL);
  EXPECT_EQ(a.Size(), 2UL);

  EXPECT_EQ(b.Order(), 3UL);
  EXPECT_EQ(b.Size(), 2UL);

  EXPECT_EQ(a, b);
}

TEST_F(GraphTest, Construction4) {
  const auto g = Graph::FromEdgeSet({{0, 1}, {1, 2}});

  std::vector<std::size_t> expected_vertex_set{0, 1, 2};
  std::vector<std::pair<std::size_t, std::size_t>> expected_edge_set{{0, 1}, {1, 2}};

  EXPECT_EQ(g.Order(), 3UL);
  EXPECT_EQ(g.Size(), 2UL);

  EXPECT_TRUE(VertexSetsEqual(g.VertexSet(), expected_vertex_set));
  EXPECT_TRUE(EdgeSetsEqual(g.EdgeSet(), expected_edge_set));
}

// ----------------------------------------------------------------------------
// Graph Eqaulity
// ----------------------------------------------------------------------------

TEST_F(GraphTest, Equality0) {
  Graph a{};
  Graph b{};
  EXPECT_EQ(a, b);
}

TEST_F(GraphTest, Equality1) {
  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddEdge(1, 2);

  EXPECT_EQ(a, b);
}

TEST_F(GraphTest, Equality2) {
  // Distinct edge sets

  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddEdge(1, 2);
  b.AddEdge(0, 2);

  EXPECT_NE(a, b);
}

TEST_F(GraphTest, Equality3) {
  // Distinct vertex sets

  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddEdge(1, 2);
  b.AddVertex(3);

  EXPECT_NE(a, b);
}

}  // namespace noisepage
