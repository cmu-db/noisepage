# Valgrind

Valgrind is useful for performing dynamic analysis. Valgrind tools can automatically detect many memory management and threading bugs, and profile Terrier in detail.

## Memory Leaks

Here's the command for using [memcheck](http://valgrind.org/docs/manual/mc-manual.html) for detecting common memory errors.

	valgrind --trace-children=yes --track-origins=yes terrier -port 15721

	valgrind --trace-children=yes --track-origins=yes ./debug/bitmap_test

## Profiling

Here's the command for using [callgrind](http://valgrind.org/docs/manual/cl-manual.html) for profiling Terrier.

        valgrind --tool=callgrind --trace-children=yes terrier &> /dev/null &

You can use [kcachegrind](http://kcachegrind.sourceforge.net/html/Home.html) for viewing the collected profile files (i.e. the `callgrind.out.*` files).

        kcachegrind

## Profiling a portion of execution

Suppose you want the Callgrind graph for a feature you implemented but the graph is getting skewed by other heavier options such as Inserts, you can turn profiling on/off during runtime through the following steps - 

First Launch Valgrind and turn off instrumentation at start


    valgrind --tool=callgrind --trace-children=yes --instr-atstart=no terrier -port 15721

When you want to start instrumentation, execute the following command on another shell window

       callgrind_control -i on

Once you're done instrumenting, you can turn it off by running

       callgrind_control -i off

You can turn on/off instrumentation multiple times within the same Valgrind session.