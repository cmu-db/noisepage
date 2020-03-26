/* Copyright (C) 2003,2004,2005 Andi Kleen, SuSE Labs.
   Command line NUMA policy control.

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
#define _GNU_SOURCE
#include <getopt.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include "numa.h"
#include "numaif.h"
#include "numaint.h"
#include "util.h"
#include "shm.h"

#define CPUSET 0
#define ALL 1

int exitcode;

struct option opts[] = {
	{"all", 0, 0, 'a'},
	{"interleave", 1, 0, 'i' },
	{"preferred", 1, 0, 'p' },
	{"cpubind", 1, 0, 'c' },
	{"cpunodebind", 1, 0, 'N' },
	{"physcpubind", 1, 0, 'C' },
	{"membind", 1, 0, 'm'},
	{"show", 0, 0, 's' },
	{"localalloc", 0,0, 'l'},
	{"hardware", 0,0,'H' },

	{"shm", 1, 0, 'S'},
	{"file", 1, 0, 'f'},
	{"offset", 1, 0, 'o'},
	{"length", 1, 0, 'L'},
	{"strict", 0, 0, 't'},
	{"shmmode", 1, 0, 'M'},
	{"dump", 0, 0, 'd'},
	{"dump-nodes", 0, 0, 'D'},
	{"shmid", 1, 0, 'I'},
	{"huge", 0, 0, 'u'},
	{"touch", 0, 0, 'T'},
	{"verify", 0, 0, 'V'}, /* undocumented - for debugging */
	{ 0 }
};

void usage(void)
{
	fprintf(stderr,
		"usage: numactl [--all | -a] [--interleave= | -i <nodes>] [--preferred= | -p <node>]\n"
		"               [--physcpubind= | -C <cpus>] [--cpunodebind= | -N <nodes>]\n"
		"               [--membind= | -m <nodes>] [--localalloc | -l] command args ...\n"
		"       numactl [--show | -s]\n"
		"       numactl [--hardware | -H]\n"
		"       numactl [--length | -l <length>] [--offset | -o <offset>] [--shmmode | -M <shmmode>]\n"
		"               [--strict | -t]\n"
		"               [--shmid | -I <id>] --shm | -S <shmkeyfile>\n"
		"               [--shmid | -I <id>] --file | -f <tmpfsfile>\n"
		"               [--huge | -u] [--touch | -T] \n"
		"               memory policy | --dump | -d | --dump-nodes | -D\n"
		"\n"
		"memory policy is --interleave | -i, --preferred | -p, --membind | -m, --localalloc | -l\n"
		"<nodes> is a comma delimited list of node numbers or A-B ranges or all.\n"
		"Instead of a number a node can also be:\n"
		"  netdev:DEV the node connected to network device DEV\n"
		"  file:PATH  the node the block device of path is connected to\n"
		"  ip:HOST    the node of the network device host routes through\n"
		"  block:PATH the node of block device path\n"
		"  pci:[seg:]bus:dev[:func] The node of a PCI device\n"
		"<cpus> is a comma delimited list of cpu numbers or A-B ranges or all\n"
		"all ranges can be inverted with !\n"
		"all numbers and ranges can be made cpuset-relative with +\n"
		"the old --cpubind argument is deprecated.\n"
		"use --cpunodebind or --physcpubind instead\n"
		"<length> can have g (GB), m (MB) or k (KB) suffixes\n");
	exit(1);
}

void usage_msg(char *msg, ...)
{
	va_list ap;
	va_start(ap,msg);
	fprintf(stderr, "numactl: ");
	vfprintf(stderr, msg, ap);
	putchar('\n');
	usage();
	va_end(ap);
}

void show_physcpubind(void)
{
	int ncpus = numa_num_configured_cpus();

	for (;;) {
		struct bitmask *cpubuf;

		cpubuf = numa_bitmask_alloc(ncpus);

		if (numa_sched_getaffinity(0, cpubuf) < 0) {
			if (errno == EINVAL && ncpus < 1024*1024) {
				ncpus *= 2;
				continue;
			}
			err("sched_get_affinity");
		}
		printmask("physcpubind", cpubuf);
		break;
	}
}

void show(void)
{
	unsigned long prefnode;
	struct bitmask *membind, *interleave, *cpubind;
	unsigned long cur;
	int policy;

	if (numa_available() < 0) {
		show_physcpubind();
		printf("No NUMA support available on this system.\n");
		exit(1);
	}

	cpubind = numa_get_run_node_mask();

	prefnode = numa_preferred();
	interleave = numa_get_interleave_mask();
	membind = numa_get_membind();
	cur = numa_get_interleave_node();

	policy = 0;
	if (get_mempolicy(&policy, NULL, 0, 0, 0) < 0)
		perror("get_mempolicy");

	printf("policy: %s\n", policy_name(policy));

	printf("preferred node: ");
	switch (policy) {
	case MPOL_PREFERRED:
		if (prefnode != -1) {
			printf("%ld\n", prefnode);
			break;
		}
		/*FALL THROUGH*/
	case MPOL_DEFAULT:
		printf("current\n");
		break;
	case MPOL_INTERLEAVE:
		printf("%ld (interleave next)\n",cur);
		break;
	case MPOL_BIND:
		printf("%d\n", find_first(membind));
		break;
	}
	if (policy == MPOL_INTERLEAVE) {
		printmask("interleavemask", interleave);
		printf("interleavenode: %ld\n", cur);
	}
	show_physcpubind();
	printmask("cpubind", cpubind);  // for compatibility
	printmask("nodebind", cpubind);
	printmask("membind", membind);
}

char *fmt_mem(unsigned long long mem, char *buf)
{
	if (mem == -1L)
		sprintf(buf, "<not available>");
	else
		sprintf(buf, "%llu MB", mem >> 20);
	return buf;
}

static void print_distances(int maxnode)
{
	int i,k;
	int fst = 0;

	for (i = 0; i <= maxnode; i++)
		if (numa_bitmask_isbitset(numa_nodes_ptr, i)) {
			fst = i;
			break;
		}
	if (numa_distance(maxnode,fst) == 0) {
		printf("No distance information available.\n");
		return;
	}
	printf("node distances:\n");
	printf("node ");
	for (i = 0; i <= maxnode; i++)
		if (numa_bitmask_isbitset(numa_nodes_ptr, i))
			printf("% 3d ", i);
	printf("\n");
	for (i = 0; i <= maxnode; i++) {
		if (!numa_bitmask_isbitset(numa_nodes_ptr, i))
			continue;
		printf("% 3d: ", i);
		for (k = 0; k <= maxnode; k++)
			if (numa_bitmask_isbitset(numa_nodes_ptr, i) &&
			    numa_bitmask_isbitset(numa_nodes_ptr, k))
				printf("% 3d ", numa_distance(i,k));
		printf("\n");
	}
}

void print_node_cpus(int node)
{
	int i, err;
	struct bitmask *cpus;

	cpus = numa_allocate_cpumask();
	err = numa_node_to_cpus(node, cpus);
	if (err >= 0) {
		for (i = 0; i < cpus->size; i++)
			if (numa_bitmask_isbitset(cpus, i))
				printf(" %d", i);
	}
	putchar('\n');
}

void hardware(void)
{
	int i;
	int numnodes=0;
	int prevnode=-1;
	int skip=0;
	int maxnode = numa_max_node();

	if (numa_available() < 0) {
                printf("No NUMA available on this system\n");
                exit(1);
        }

	for (i=0; i<=maxnode; i++)
		if (numa_bitmask_isbitset(numa_nodes_ptr, i))
			numnodes++;
	printf("available: %d nodes (", numnodes);
	for (i=0; i<=maxnode; i++) {
		if (numa_bitmask_isbitset(numa_nodes_ptr, i)) {
			if (prevnode == -1) {
				printf("%d", i);
				prevnode=i;
				continue;
			}

			if (i > prevnode + 1) {
				if (skip) {
					printf("%d", prevnode);
					skip=0;
				}
				printf(",%d", i);
				prevnode=i;
				continue;
			}

			if (i == prevnode + 1) {
				if (!skip) {
					printf("-");
					skip=1;
				}
				prevnode=i;
			}

			if ((i == maxnode) && skip)
				printf("%d", prevnode);
		}
	}
	printf(")\n");

	for (i = 0; i <= maxnode; i++) {
		char buf[64];
		long long fr;
		unsigned long long sz = numa_node_size64(i, &fr);
		if (!numa_bitmask_isbitset(numa_nodes_ptr, i))
			continue;

		printf("node %d cpus:", i);
		print_node_cpus(i);
		printf("node %d size: %s\n", i, fmt_mem(sz, buf));
		printf("node %d free: %s\n", i, fmt_mem(fr, buf));
	}
	print_distances(maxnode);
}

void checkerror(char *s)
{
	if (errno) {
		perror(s);
		exit(1);
	}
}

void checknuma(void)
{
	static int numa = -1;
	if (numa < 0) {
		if (numa_available() < 0)
			complain("This system does not support NUMA policy");
	}
	numa = 0;
}

int set_policy = -1;

void setpolicy(int pol)
{
	if (set_policy != -1)
		usage_msg("Conflicting policies");
	set_policy = pol;
}

void nopolicy(void)
{
	if (set_policy >= 0)
		usage_msg("specify policy after --shm/--file");
}

int did_cpubind = 0;
int did_strict = 0;
int do_shm = 0;
int do_dump = 0;
int shmattached = 0;
int did_node_cpu_parse = 0;
int parse_all = 0;
char *shmoption;

void check_cpubind(int flag)
{
	if (flag)
		usage_msg("cannot do --cpubind on shared memory\n");
}

void noshm(char *opt)
{
	if (shmattached)
		usage_msg("%s must be before shared memory specification", opt);
	shmoption = opt;
}

void dontshm(char *opt)
{
	if (shmoption)
		usage_msg("%s shm option is not allowed before %s", shmoption, opt);
}

void needshm(char *opt)
{
	if (!shmattached)
		usage_msg("%s must be after shared memory specification", opt);
}

void check_all_parse(int flag)
{
	if (did_node_cpu_parse)
		usage_msg("--all/-a option must be before all cpu/node specifications");
}

void get_short_opts(struct option *o, char *s)
{
	*s++ = '+';
	while (o->name) {
		if (isprint(o->val)) {
			*s++ = o->val;
			if (o->has_arg)
				*s++ = ':';
		}
		o++;
	}
	*s = '\0';
}

void check_shmbeyond(char *msg)
{
	if (shmoffset >= shmlen) {
		fprintf(stderr,
		"numactl: region offset %#llx beyond its length %#llx at %s\n",
				shmoffset, shmlen, msg);
		exit(1);
	}
}

static struct bitmask *numactl_parse_nodestring(char *s, int flag)
{
	static char *last;

	if (s[0] == 's' && !strcmp(s, "same")) {
		if (!last)
			usage_msg("same needs previous node specification");
		s = last;
	} else {
		last = s;
	}

	if (flag == ALL)
		return numa_parse_nodestring_all(s);
	else
		return numa_parse_nodestring(s);
}

int main(int ac, char **av)
{
	int c, i, nnodes=0;
	long node=-1;
	char *end;
	char shortopts[array_len(opts)*2 + 1];
	struct bitmask *mask = NULL;

	get_short_opts(opts,shortopts);
	while ((c = getopt_long(ac, av, shortopts, opts, NULL)) != -1) {
		switch (c) {
		case 's': /* --show */
			show();
			exit(0);
		case 'H': /* --hardware */
			nopolicy();
			hardware();
			exit(0);
		case 'i': /* --interleave */
			checknuma();
			if (parse_all)
				mask = numactl_parse_nodestring(optarg, ALL);
			else
				mask = numactl_parse_nodestring(optarg, CPUSET);
			if (!mask) {
				printf ("<%s> is invalid\n", optarg);
				usage();
			}

			errno = 0;
			did_node_cpu_parse = 1;
			setpolicy(MPOL_INTERLEAVE);
			if (shmfd >= 0)
				numa_interleave_memory(shmptr, shmlen, mask);
			else
				numa_set_interleave_mask(mask);
			checkerror("setting interleave mask");
			break;
		case 'N': /* --cpunodebind */
		case 'c': /* --cpubind */
			dontshm("-c/--cpubind/--cpunodebind");
			checknuma();
			if (parse_all)
				mask = numactl_parse_nodestring(optarg, ALL);
			else
				mask = numactl_parse_nodestring(optarg, CPUSET);
			if (!mask) {
				printf ("<%s> is invalid\n", optarg);
				usage();
			}
			errno = 0;
			check_cpubind(do_shm);
			did_cpubind = 1;
			did_node_cpu_parse = 1;
			numa_run_on_node_mask_all(mask);
			checkerror("sched_setaffinity");
			break;
		case 'C': /* --physcpubind */
		{
			struct bitmask *cpubuf;
			dontshm("-C/--physcpubind");
			if (parse_all)
				cpubuf = numa_parse_cpustring_all(optarg);
			else
				cpubuf = numa_parse_cpustring(optarg);
			if (!cpubuf) {
				printf ("<%s> is invalid\n", optarg);
				usage();
			}
			errno = 0;
			check_cpubind(do_shm);
			did_cpubind = 1;
			did_node_cpu_parse = 1;
			numa_sched_setaffinity(0, cpubuf);
			checkerror("sched_setaffinity");
			free(cpubuf);
			break;
		}
		case 'm': /* --membind */
			checknuma();
			setpolicy(MPOL_BIND);
			if (parse_all)
				mask = numactl_parse_nodestring(optarg, ALL);
			else
				mask = numactl_parse_nodestring(optarg, CPUSET);
			if (!mask) {
				printf ("<%s> is invalid\n", optarg);
				usage();
			}
			errno = 0;
			did_node_cpu_parse = 1;
			numa_set_bind_policy(1);
			if (shmfd >= 0) {
				numa_tonodemask_memory(shmptr, shmlen, mask);
			} else {
				numa_set_membind(mask);
			}
			numa_set_bind_policy(0);
			checkerror("setting membind");
			break;
		case 'p': /* --preferred */
			checknuma();
			setpolicy(MPOL_PREFERRED);
			if (parse_all)
				mask = numactl_parse_nodestring(optarg, ALL);
			else
				mask = numactl_parse_nodestring(optarg, CPUSET);
			if (!mask) {
				printf ("<%s> is invalid\n", optarg);
				usage();
			}
			for (i=0; i<mask->size; i++) {
				if (numa_bitmask_isbitset(mask, i)) {
					node = i;
					nnodes++;
				}
			}
			if (nnodes != 1)
				usage();
			numa_bitmask_free(mask);
			errno = 0;
			did_node_cpu_parse = 1;
			numa_set_bind_policy(0);
			if (shmfd >= 0)
				numa_tonode_memory(shmptr, shmlen, node);
			else
				numa_set_preferred(node);
			checkerror("setting preferred node");
			break;
		case 'l': /* --local */
			checknuma();
			setpolicy(MPOL_DEFAULT);
			errno = 0;
			if (shmfd >= 0)
				numa_setlocal_memory(shmptr, shmlen);
			else
				numa_set_localalloc();
			checkerror("local allocation");
			break;
		case 'S': /* --shm */
			check_cpubind(did_cpubind);
			nopolicy();
			attach_sysvshm(optarg, "--shm");
			shmattached = 1;
			break;
		case 'f': /* --file */
			check_cpubind(did_cpubind);
			nopolicy();
			attach_shared(optarg, "--file");
			shmattached = 1;
			break;
		case 'L': /* --length */
			noshm("--length");
			shmlen = memsize(optarg);
			break;
		case 'M': /* --shmmode */
			noshm("--shmmode");
			shmmode = strtoul(optarg, &end, 8);
			if (end == optarg || *end)
				usage();
			break;
		case 'd': /* --dump */
			if (shmfd < 0)
				complain(
				"Cannot do --dump without shared memory.\n");
			dump_shm();
			do_dump = 1;
			break;
		case 'D': /* --dump-nodes */
			if (shmfd < 0)
				complain(
			    "Cannot do --dump-nodes without shared memory.\n");
			dump_shm_nodes();
			do_dump = 1;
			break;
		case 't': /* --strict */
			did_strict = 1;
			numa_set_strict(1);
			break;
		case 'I': /* --shmid */
			shmid = strtoul(optarg, &end, 0);
			if (end == optarg || *end)
				usage();
			break;

		case 'u': /* --huge */
			noshm("--huge");
			shmflags |= SHM_HUGETLB;
			break;

		case 'o':  /* --offset */
			noshm("--offset");
			shmoffset = memsize(optarg);
			break;

		case 'T': /* --touch */
			needshm("--touch");
			check_shmbeyond("--touch");
			numa_police_memory(shmptr, shmlen);
			break;

		case 'V': /* --verify */
			needshm("--verify");
			if (set_policy < 0)
				complain("Need a policy first to verify");
			check_shmbeyond("--verify");
			numa_police_memory(shmptr, shmlen);
			if (!mask)
				complain("Need a mask to verify");
			else
				verify_shm(set_policy, mask);
			break;

		case 'a': /* --all */
			check_all_parse(did_node_cpu_parse);
			parse_all = 1;
			break;
		default:
			usage();
		}
	}

	av += optind;
	ac -= optind;

	if (shmfd >= 0) {
		if (*av)
			usage();
		exit(exitcode);
	}

	if (did_strict)
		fprintf(stderr,
			"numactl: warning. Strict flag for process ignored.\n");

	if (do_dump)
		usage_msg("cannot do --dump|--dump-shm for process");

	if (shmoption)
		usage_msg("shm related option %s for process", shmoption);

	if (*av == NULL)
		usage();
	execvp(*av, av);
	complain("execution of `%s': %s\n", av[0], strerror(errno));
	return 0; /* not reached */
}
