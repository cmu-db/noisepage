# In-place Parallel Super Scalar Samplesort (IPS⁴o)

This is the implementation of the algorithm presented in the [eponymous paper](https://arxiv.org/abs/1705.02257),
which contains an in-depth description of its inner workings, as well as an extensive experimental performance evaluation.
Here's the abstract:

> We present a sorting algorithm that works in-place, executes in parallel, is
> cache-efficient, avoids branch-mispredictions, and performs work O(n log n) for
> arbitrary inputs with high probability. The main algorithmic contributions are
> new ways to make distribution-based algorithms in-place: On the practical side,
> by using coarse-grained block-based permutations, and on the theoretical side,
> we show how to eliminate the recursion stack. Extensive experiments show that
> our algorithm IPS⁴o scales well on a variety of multi-core machines. We
> outperform our closest in-place competitor by a factor of up to 3. Even as
> a sequential algorithm, we are up to 1.5 times faster than the closest
> sequential competitor, BlockQuicksort.

## Usage

```C++
#include "ips4o.hpp"

// sort sequentially
ips4o::sort(begin, end[, comparator])

// sort in parallel (uses OpenMP if available, std::thread otherwise)
ips4o::parallel::sort(begin, end[, comparator])
```

Make sure to compile with C++14 support. Currently, the code does not compile on Windows.

For the parallel algorithm, you need to enable either OpenMP (`-fopenmp`) or C++ threads (e.g., `-pthread`).
You also need a CPU that supports 16-byte compare-and-exchange instructions.
If you get undefined references to `__atomic_fetch_add_16`, either set your CPU correctly (e.g., `-march=native`),
  enable the instructions explicitly (`-mcx16`), or try linking against GCC's libatomic (`-latomic`).
