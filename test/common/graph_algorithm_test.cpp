#include "common/graph_algorithm.h"

#include <numeric>

#include "common/graph.h"
#include "test_util/test_harness.h"

namespace noisepage {

using common::Graph;
namespace graph = common::graph;

class GraphAlgorithmTest : public TerrierTest {};

// ----------------------------------------------------------------------------
// graph::Isomorphic
// ----------------------------------------------------------------------------

TEST_F(GraphAlgorithmTest, Isomorphic0) {
  Graph a{};
  Graph b{};
  EXPECT_TRUE(graph::Isomorphic(a, b));
}

TEST_F(GraphAlgorithmTest, Isomorphic1) {
  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddEdge(1, 2);

  EXPECT_TRUE(graph::Isomorphic(a, b));
}

TEST_F(GraphAlgorithmTest, Isomorphic2) {
  Graph a{};
  a.AddEdge(0, 1);
  a.AddEdge(1, 2);

  Graph b{};
  b.AddEdge(0, 1);
  b.AddVertex(2);

  EXPECT_FALSE(graph::Isomorphic(a, b));
}

// ----------------------------------------------------------------------------
// graph::HasCycle
// ----------------------------------------------------------------------------

TEST_F(GraphAlgorithmTest, HasCycle0) {
  Graph g{};
  EXPECT_FALSE(graph::HasCycle(g));
}

TEST_F(GraphAlgorithmTest, HasCycle1) {
  Graph g{};
  g.AddEdge(0, 1);
  g.AddEdge(1, 2);
  EXPECT_FALSE(graph::HasCycle(g));
}

TEST_F(GraphAlgorithmTest, HasCycle2) {
  Graph g{};
  g.AddEdge(0, 1);
  g.AddEdge(1, 0);
  EXPECT_TRUE(graph::HasCycle(g));
}

TEST_F(GraphAlgorithmTest, HasCycle3) {
  Graph g{};
  g.AddEdge(0, 1);
  g.AddEdge(1, 2);
  g.AddEdge(2, 0);
  EXPECT_TRUE(graph::HasCycle(g));
}

TEST_F(GraphAlgorithmTest, HasCycle4) {
  static constexpr const std::size_t graph_order = 10;

  Graph g{};
  for (std::size_t i = 0; i < graph_order; ++i) {
    g.AddEdge(i, i + 1);
  }
  EXPECT_FALSE(graph::HasCycle(g));
}

TEST_F(GraphAlgorithmTest, HasCycle5) {
  static constexpr const std::size_t graph_order = 10;

  Graph g{};
  for (std::size_t i = 0; i < graph_order; ++i) {
    g.AddEdge(i, i + 1);
  }
  g.AddEdge(graph_order, 0);
  EXPECT_TRUE(graph::HasCycle(g));
}

// ----------------------------------------------------------------------------
// graph::TopologicalSort
// ----------------------------------------------------------------------------

TEST_F(GraphAlgorithmTest, TopologicalSort0) {
  Graph g{};
  const auto order = graph::TopologicalSort(g);
  EXPECT_EQ(0UL, order.size());
}

TEST_F(GraphAlgorithmTest, TopologicalSort1) {
  Graph g{};
  g.AddEdge(0, 1);

  const std::vector<std::size_t> expected{1, 0};

  const auto order = graph::TopologicalSort(g);
  EXPECT_EQ(2UL, order.size());
  EXPECT_EQ(expected, order);
}

TEST_F(GraphAlgorithmTest, TopologicalSort2) {
  Graph g{};
  g.AddEdge(0, 1);
  g.AddEdge(1, 2);

  const std::vector<std::size_t> expected{2, 1, 0};

  const auto order = graph::TopologicalSort(g);
  EXPECT_EQ(3UL, order.size());
  EXPECT_EQ(expected, order);
}

TEST_F(GraphAlgorithmTest, TopologicalSort3) {
  static constexpr const std::size_t graph_order = 10;

  Graph g{};
  for (std::size_t i = 0; i < graph_order - 1; ++i) {
    g.AddEdge(i, i + 1);
  }

  std::vector<std::size_t> expected(graph_order);
  std::iota(expected.begin(), expected.end(), 0);
  std::reverse(expected.begin(), expected.end());

  const auto order = graph::TopologicalSort(g);
  EXPECT_EQ(graph_order, order.size());
  EXPECT_EQ(expected, order);
}

}  // namespace noisepage
