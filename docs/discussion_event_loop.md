# Discussion Doc: Event Loop

## Overview

We use an event loop to handle various types of events in the network layer. This is a very common design pattern for
handling asynchronous event. If you'd like to learn more about it some good keywords to search for are: "Event Loop", "
Event Based Programming", "Event Driven Programming".

## I/O Events

- **Connection Requests**: The main thread will check for incoming connections requests and dispatch them a worker
  thread.
- **Handling Connections**: The worker threads will listen for incoming queries over the connection, execute them, and
  respond back to the client. There is also a timeout event that will close these connections if there's been no
  activity for a certain amount of time.

## Signal Events

- **SIGHUP**: a SIGHUP signal will terminate the server process.

## Timeout Events

- **Handling Connections**: Idle connections will be terminated after a certain amount of time. This is tightly coupled
  with the I/O event related to handling connections.

## Manual/Async Events

These events are manually triggered by our code.

- **Terminate Conection**: When connections are terminated a manual event is triggered to close the connection and clean
  up all associated resources.

## Libevent vs Libev

**Libevent**: https://libevent.org/

**Libev**: http://software.schmorp.de/pkg/libev.html

Currently we use Libevent as our Event Loop implementation. We have also looked into using Libev and may transition to
Libev in the future. Below is a comparison between the two libraries.

### Language

Libevent is implemented in C. Libev is implemented in C, but has wrapper C++ classes and APIs.

### Usage

Libevent is more "one size fits all" compared to libev. There is a single event type that can watch for any kind of
event. There is also a single event loop type that you can start and stop on a per thread basis. Libev on the other hand
has a different event type for every kind of event. So if you want a single event to watch for two things (like an I/O
event and a timer event) at the same time, then you'll have to create your own custom composite event. Additionally
Libev has two different event loop types. One "default" event loop, which there can only ever be one of, and another
"non-default" event loop, which there can be any number of. In practice this isn't such a huge deal, for example in
NoisePage the connection dispatcher thread would be the default event loop while all the connection handling threads
would be non-default event loops.

This makes libevent easier to use than libev, however it ends up using more memory and according the designer of libev
it makes libevent
slower. [This Stack Overflow post](https://stackoverflow.com/questions/9433864/whats-the-difference-between-libev-and-libevent)
was answered by the creator of libev and describes the differences in design philosophy.

### Performance

Running the tpcc and noop oltpbenchmarks against the current master of NoisePage and an implementation that switches
from libevent to libev for 60 seconds, we get the following results (Note: each benchmark was run twice):

| Libev vs Libevent | Benchmark | Mean Latency (milliseconds) | Throughput (requests/second)
| --- | --- | --- | --- |
| Libev | NoOp | 50.1565409803912 | 3398.930926973485 |
| Libev | NoOp | 50.15175110158765 | 3173.42297782733 |
| Libevent | NoOp | 50.14538746950166 | 3231.0416656357247 |
| Libevent | NoOp | 50.294910261202624 | 3254.812815029675 |
| Libev | TPCC | 1.325 | 753.367 | 
| Libev | TPCC | 1.396 | 714.883 |
| Libevent | TPCC | 1.295 | 770.867 |
| Libevent | TPCC | 1.344 | 743.033 |

These were run on dev8.db.pdl.local.cmu.edu. There doesn't seem to be a clear winner between the two from these results.

Libev has it done it's own benchmarking comparing itself against libevent that can be found
[here](http://libev.schmorp.de/bench.html). According to these result Libev only starts being noticeably faster when
there are 10s of thousands of watched file descriptors, and the difference is measure in microseconds and nanoseconds.
So it makes sense that we didn't notice a difference in the OLTP Benchmarks which is measured in milliseconds and don't
have enough file descriptors.

### NoisePage POC Implementation
[Joe Koshakow's Libev Implementation](https://github.com/jkosh44/noisepage/tree/libev-666)