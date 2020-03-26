/* Copyright (C) 2003,2004 Andi Kleen, SuSE Labs.
   Test/demo program for libnuma. This is also a more or less useful benchmark
   of the NUMA characteristics of your machine. It benchmarks most possible
   NUMA policy memory configurations with various benchmarks.
   Compile standalone with cc -O2 numademo.c -o numademo -lnuma -lm

   numactl is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public
   License as published by the Free Software Foundation; version
   2.

   numactl is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should find a copy of v2 of the GNU General Public License somewhere
   on your Linux system; if not, write to the Free Software Foundation,
   Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA */
#define _GNU_SOURCE 1
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/time.h>
#include "numa.h"
#ifdef HAVE_STREAM_LIB
#include "stream_lib.h"
#endif
#ifdef HAVE_MT
#include "mt.h"
#endif
#ifdef HAVE_CLEAR_CACHE
#include "clearcache.h"
#else
static inline void clearcache(void *a, unsigned size) {}
#endif
#define FRACT_NODES 8
#define FRACT_MASKS 32
int fract_nodes;
int *node_to_use;
unsigned long msize;

/* Should get this from cpuinfo, but on !x86 it's not there */
enum {
	CACHELINESIZE = 64,
};

enum test {
	MEMSET = 0,
	MEMCPY,
	FORWARD,
	BACKWARD,
	STREAM,
	RANDOM2,
	PTRCHASE,
} thistest;

char *delim = " ";
int force;
int regression_testing=0;

char *testname[] = {
	"memset",
	"memcpy",
	"forward",
	"backward",
#ifdef HAVE_STREAM_LIB
	"stream",
#endif
#ifdef HAVE_MT
	"random2",
#endif
	"ptrchase",
	NULL,
};

void output(char *title, char *result)
{
	if (!isspace(delim[0]))
		printf("%s%s%s\n", title,delim, result);
	else
		printf("%-42s%s\n", title, result);
}

#ifdef HAVE_STREAM_LIB
void do_stream(char *name, unsigned char *mem)
{
	int i;
	char title[100], buf[100];
	double res[STREAM_NRESULTS];
	stream_verbose = 0;
	clearcache(mem, msize);
	stream_init(mem);
	stream_test(res);
	sprintf(title, "%s%s%s", name, delim, "STREAM");
	buf[0] = '\0';
	for (i = 0; i < STREAM_NRESULTS; i++) {
		if (buf[0])
			strcat(buf,delim);
		sprintf(buf+strlen(buf), "%s%s%.2f%sMB/s",
			stream_names[i], delim, res[i], delim);
	}
	output(title, buf);
	clearcache(mem, msize);
}
#endif

/* Set up a randomly distributed list to fool prefetchers */
union node {
	union node *next;
	struct {
		unsigned nexti;
		unsigned val;
	};
};

static int cmp_node(const void *ap, const void *bp)
{
	union node *a = (union node *)ap;
	union node *b = (union node *)bp;
	return a->val - b->val;
}

void **ptrchase_init(unsigned char *mem)
{
	long i;
	union node *nodes = (union node *)mem;
	long nmemb = msize / sizeof(union node);
	srand(1234);
	for (i = 0; i < nmemb; i++) {
		nodes[i].val = rand();
		nodes[i].nexti = i + 1;
	}
	qsort(nodes, nmemb, sizeof(union node), cmp_node);
	for (i = 0; i < nmemb; i++) {
		union node *n = &nodes[i];
		n->next = n->nexti >= nmemb ? NULL : &nodes[n->nexti];
	}
	return (void **)nodes;
}

static inline unsigned long long timerfold(struct timeval *tv)
{
	return tv->tv_sec * 1000000ULL + tv->tv_usec;
}

#define LOOPS 10

void memtest(char *name, unsigned char *mem)
{
	long k;
	struct timeval start, end, res;
	unsigned long long max, min, sum, r;
	int i;
	char title[128], result[128];

	if (!mem) {
		fprintf(stderr,
		"Failed to allocate %lu bytes of memory. Test \"%s\" exits.\n",
			msize, name);
		return;
	}

#ifdef HAVE_STREAM_LIB
	if (thistest == STREAM) {
		do_stream(name, mem);
		goto out;
	}
#endif

	max = 0;
	min = ~0UL;
	sum = 0;

	/*
	 * Note:  0th pass allocates the pages, don't measure
	 */
	for (i = 0; i < LOOPS+1; i++) {
		clearcache(mem, msize);
		switch (thistest) {
		case PTRCHASE:
		{
			void **ptr;
			ptr = ptrchase_init(mem);
			gettimeofday(&start,NULL);
			while (*ptr)
				ptr = (void **)*ptr;
			gettimeofday(&end,NULL);
			/* Side effect to trick the optimizer */
			*ptr = "bla";
			break;
		}

		case MEMSET:
			gettimeofday(&start,NULL);
			memset(mem, 0xff, msize);
			gettimeofday(&end,NULL);
			break;

		case MEMCPY:
			gettimeofday(&start,NULL);
			memcpy(mem, mem + msize/2, msize/2);
			gettimeofday(&end,NULL);
			break;

		case FORWARD:
			/* simple kernel to just fetch cachelines and write them back.
			   will trigger hardware prefetch */
			gettimeofday(&start,NULL);
			for (k = 0; k < msize; k+=CACHELINESIZE)
				mem[k]++;
			gettimeofday(&end,NULL);
			break;

		case BACKWARD:
			gettimeofday(&start,NULL);
			for (k = msize-5; k > 0; k-=CACHELINESIZE)
				mem[k]--;
			gettimeofday(&end,NULL);
			break;

#ifdef HAVE_MT
		case RANDOM2:
		{
			unsigned * __restrict m = (unsigned *)mem;
			unsigned max = msize / sizeof(unsigned);
			unsigned mask;

			mt_init();
			mask = 1;
			while (mask < max)
				mask = (mask << 1) | 1;
			/*
			 * There's no guarantee all memory is touched, but
			 * we assume (hope) that the distribution of the MT
			 * is good enough to touch most.
			 */
			gettimeofday(&start,NULL);
			for (k = 0; k < max; k++) {
				unsigned idx = mt_random() & mask;
				if (idx >= max)
					idx -= max;
				m[idx]++;
			}
			gettimeofday(&end,NULL);
		}

#endif
		default:
			break;
		}

		if (!i)
			continue;  /* don't count allocation pass */

		timersub(&end, &start, &res);
		r = timerfold(&res);
		if (r > max) max = r;
		if (r < min) min = r;
		sum += r;
	}
	sprintf(title, "%s%s%s", name, delim, testname[thistest]);
#define H(t) (((double)msize) / ((double)t))
#define D3 delim,delim,delim
	sprintf(result, "Avg%s%.2f%sMB/s%sMax%s%.2f%sMB/s%sMin%s%.2f%sMB/s",
		delim,
		H(sum/LOOPS),
		D3,
		H(min),
		D3,
		H(max),
		delim);
#undef H
#undef D3
	output(title,result);

#ifdef HAVE_STREAM_LIB
 out:
#endif
	/* Just to make sure that when we switch CPUs that the old guy
	   doesn't still keep it around. */
	clearcache(mem, msize);

	numa_free(mem, msize);
}

int popcnt(unsigned long val)
{
	int i = 0, cnt = 0;
	while (val >> i) {
		if ((1UL << i) & val)
			cnt++;
		i++;
	}
	return cnt;
}

int max_node, numnodes;

int get_node_list(void)
{
        int a, got_nodes = 0;
        long long free_node_sizes;

        numnodes = numa_num_configured_nodes();
        node_to_use = (int *)malloc(numnodes * sizeof(int));
        max_node = numa_max_node();
        for (a = 0; a <= max_node; a++) {
                if (numa_node_size(a, &free_node_sizes) > 0)
                        node_to_use[got_nodes++] = a;
        }
        if(got_nodes != numnodes)
                return -1;
        return 0;
}

void test(enum test type)
{
	unsigned long mask;
	int i, k;
	char buf[512];
	struct bitmask *nodes;

	nodes = numa_allocate_nodemask();
	thistest = type;

	if (regression_testing) {
		printf("\nTest %s doing 1 of %d nodes and 1 of %d masks.\n",
			testname[thistest], fract_nodes, FRACT_MASKS);
	}

	memtest("memory with no policy", numa_alloc(msize));
	memtest("local memory", numa_alloc_local(msize));

	memtest("memory interleaved on all nodes", numa_alloc_interleaved(msize));
	for (i = 0; i < numnodes; i++) {
		if (regression_testing && (node_to_use[i] % fract_nodes)) {
		/* for regression testing (-t) do only every eighth node */
			continue;
		}
		sprintf(buf, "memory on node %d", node_to_use[i]);
		memtest(buf, numa_alloc_onnode(msize, node_to_use[i]));
	}

	for (mask = 1, i = 0; mask < (1UL<<numnodes); mask++, i++) {
		int w;
		char buf2[20];
		if (popcnt(mask) == 1)
			continue;
		if (regression_testing && (i > 50)) {
			break;
		}
		if (regression_testing && (i % FRACT_MASKS)) {
		/* for regression testing (-t)
			do only every 32nd mask permutation */
			continue;
		}
		numa_bitmask_clearall(nodes);
		for (w = 0; mask >> w; w++) {
			if ((mask >> w) & 1)
				numa_bitmask_setbit(nodes, w);
		}

		sprintf(buf, "memory interleaved on");
		for (k = 0; k < numnodes; k++)
			if ((1UL<<node_to_use[k]) & mask) {
				sprintf(buf2, " %d", node_to_use[k]);
				strcat(buf, buf2);
			}
		memtest(buf, numa_alloc_interleaved_subset(msize, nodes));
	}

	for (i = 0; i < numnodes; i++) {
		if (regression_testing && (node_to_use[i] % fract_nodes)) {
		/* for regression testing (-t) do only every eighth node */
			continue;
		}
		printf("setting preferred node to %d\n", node_to_use[i]);
		numa_set_preferred(node_to_use[i]);
		memtest("memory without policy", numa_alloc(msize));
	}

	numa_set_interleave_mask(numa_all_nodes_ptr);
	memtest("manual interleaving to all nodes", numa_alloc(msize));

	if (numnodes > 0) {
		numa_bitmask_clearall(nodes);
		numa_bitmask_setbit(nodes, 0);
		numa_bitmask_setbit(nodes, 1);
		numa_set_interleave_mask(nodes);
		memtest("manual interleaving on node 0/1", numa_alloc(msize));
		printf("current interleave node %d\n", numa_get_interleave_node());
	}

	numa_bitmask_free(nodes);

	numa_set_interleave_mask(numa_no_nodes_ptr);

	nodes = numa_allocate_nodemask();

	for (i = 0; i < numnodes; i++) {
		int oldhn = numa_preferred();

		if (regression_testing && (node_to_use[i] % fract_nodes)) {
		/* for regression testing (-t) do only every eighth node */
			continue;
		}
		numa_run_on_node(node_to_use[i]);
		printf("running on node %d, preferred node %d\n",node_to_use[i], oldhn);

		memtest("local memory", numa_alloc_local(msize));

		memtest("memory interleaved on all nodes",
			numa_alloc_interleaved(msize));

		if (numnodes >= 2) {
			numa_bitmask_clearall(nodes);
			numa_bitmask_setbit(nodes, 0);
			numa_bitmask_setbit(nodes, 1);
			memtest("memory interleaved on node 0/1",
				numa_alloc_interleaved_subset(msize, nodes));
		}

		for (k = 0; k < numnodes; k++) {
			if (node_to_use[k] == node_to_use[i])
				continue;
			if (regression_testing && (node_to_use[k] % fract_nodes)) {
			/* for regression testing (-t)
				do only every eighth node */
				continue;
			}
			sprintf(buf, "alloc on node %d", node_to_use[k]);
			numa_bitmask_clearall(nodes);
			numa_bitmask_setbit(nodes, node_to_use[k]);
			numa_set_membind(nodes);
			memtest(buf, numa_alloc(msize));
			numa_set_membind(numa_all_nodes_ptr);
		}

		numa_set_localalloc();
		memtest("local allocation", numa_alloc(msize));

		numa_set_preferred(node_to_use[(i + 1) % numnodes]);
		memtest("setting wrong preferred node", numa_alloc(msize));
		numa_set_preferred(node_to_use[i]);
		memtest("setting correct preferred node", numa_alloc(msize));
		numa_set_preferred(-1);
		if (!delim[0])
			printf("\n\n\n");
	}
	numa_bitmask_free(nodes);
	/* numa_run_on_node_mask is not tested */
}

void usage(void)
{
	int i;
	printf("usage: numademo [-S] [-f] [-c] [-e] [-t] msize[kmg] {tests}\nNo tests means run all.\n");
	printf("-c output CSV data. -f run even without NUMA API. -S run stupid tests. -e exit on error\n");
	printf("-t regression test; do not run all node combinations\n");
	printf("valid tests:");
	for (i = 0; testname[i]; i++)
		printf(" %s", testname[i]);
	putchar('\n');
	exit(1);
}

/* duplicated to make numademo standalone */
long memsize(char *s)
{
	char *end;
	long length = strtoul(s,&end,0);
	switch (toupper(*end)) {
	case 'G': length *= 1024;  /*FALL THROUGH*/
	case 'M': length *= 1024;  /*FALL THROUGH*/
	case 'K': length *= 1024; break;
	}
	return length;
}

int main(int ac, char **av)
{
	int simple_tests = 0;

	while (av[1] && av[1][0] == '-') {
		ac--;
		switch (av[1][1]) {
		case 'c':
			delim = ",";
			break;
		case 'f':
			force = 1;
			break;
		case 'S':
			simple_tests = 1;
			break;
		case 'e':
			numa_exit_on_error = 1;
			numa_exit_on_warn = 1;
			break;
		case 't':
			regression_testing = 1;
			break;
		default:
			usage();
			break;
		}
		++av;
	}

	if (!av[1])
		usage();

	if (numa_available() < 0) {
		printf("your system does not support the numa API.\n");
		if (!force)
			exit(1);
	}
	if(get_node_list()){
		fprintf(stderr, "Configured Nodes does not match available memory nodes\n");
		exit(1);
	}

	printf("%d nodes available\n", numnodes);
	fract_nodes = (((numnodes-1)/8)*2) + FRACT_NODES;

	if (numnodes <= 3)
		regression_testing = 0; /* set -t auto-off for small systems */

	msize = memsize(av[1]);

	if (!msize)
		usage();

#ifdef HAVE_STREAM_LIB
	stream_setmem(msize);
#endif

	if (av[2] == NULL) {
		test(MEMSET);
		test(MEMCPY);
		if (simple_tests) {
			test(FORWARD);
			test(BACKWARD);
		}
#ifdef HAVE_MT
		test(RANDOM2);
#endif
#ifdef HAVE_STREAM_LIB
		test(STREAM);
#endif
		if (msize >= sizeof(union node)) {
			test(PTRCHASE);
		} else {
			fprintf(stderr, "You must set msize at least %lu bytes for ptrchase test.\n",
				sizeof(union node));
			exit(1);
		}
	} else {
		int k;
		for (k = 2; k < ac; k++) {
			int i;
			int found = 0;
			for (i = 0; testname[i]; i++) {
				if (!strcmp(testname[i],av[k])) {
					test(i);
					found = 1;
					break;
				}
			}
			if (!found) {
				fprintf(stderr,"unknown test `%s'\n", av[k]);
				usage();
			}
		}
	}
	free(node_to_use);
	return 0;
}
