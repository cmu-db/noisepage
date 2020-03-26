/* Copyright (C) 2003,2004 Andi Kleen, SuSE Labs.
   Allocate memory with policy for testing.

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

#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/fcntl.h>
#include <string.h>
#include <stdbool.h>
#include "numa.h"
#include "numaif.h"
#include "util.h"

#define terr(x) perror(x)

enum {
	UNIT = 10*1024*1024,
};

#ifndef MADV_NOHUGEPAGE
#define MADV_NOHUGEPAGE 15
#endif

int repeat = 1;

void usage(void)
{
	printf("memhog [-fFILE] [-rNUM] size[kmg] [policy [nodeset]]\n");
	printf("-f mmap is backed by FILE\n");
	printf("-rNUM repeat memset NUM times\n");
	printf("-H disable transparent hugepages\n");
	print_policies();
	exit(1);
}

long length;

void hog(void *map)
{
	long i;
	for (i = 0;  i < length; i += UNIT) {
		long left = length - i;
		if (left > UNIT)
			left = UNIT;
		putchar('.');
		fflush(stdout);
		memset(map + i, 0xff, left);
	}
	putchar('\n');
}

int main(int ac, char **av)
{
	char *map;
	struct bitmask *nodes, *gnodes;
	int policy, gpolicy;
	int ret = 0;
	int loose = 0;
	int i;
	int fd = -1;
	bool disable_hugepage = false;

	nodes = numa_allocate_nodemask();
	gnodes = numa_allocate_nodemask();

	while (av[1] && av[1][0] == '-') {
		switch (av[1][1]) {
		case 'f':
			fd = open(av[1]+2, O_RDWR);
			if (fd < 0)
				perror(av[1]+2);
			break;
		case 'r':
			repeat = atoi(av[1] + 2);
			break;
		case 'H':
			disable_hugepage = true;
			break;
		default:
			usage();
		}
		av++;
	}

	if (!av[1]) usage();

	length = memsize(av[1]);
	if (av[2] && numa_available() < 0) {
		printf("Kernel doesn't support NUMA policy\n");
	} else
		loose = 1;
	policy = parse_policy(av[2], av[3]);
	if (policy != MPOL_DEFAULT)
		nodes = numa_parse_nodestring(av[3]);
        if (!nodes) {
		printf ("<%s> is invalid\n", av[3]);
		exit(1);
	}

	if (fd >= 0)
		map = mmap(NULL,length,PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	else
		map = mmap(NULL, length, PROT_READ|PROT_WRITE,
				   MAP_PRIVATE|MAP_ANONYMOUS,
				   0, 0);
	if (map == (char*)-1)
		err("mmap");

	if (mbind(map, length, policy, nodes->maskp, nodes->size, 0) < 0)
		terr("mbind");

	if (disable_hugepage)
		madvise(map, length, MADV_NOHUGEPAGE);

	gpolicy = -1;
	if (get_mempolicy(&gpolicy, gnodes->maskp, gnodes->size, map, MPOL_F_ADDR) < 0)
		terr("get_mempolicy");
	if (!loose && policy != gpolicy) {
		ret = 1;
		printf("policy %d gpolicy %d\n", policy, gpolicy);
	}
	if (!loose && !numa_bitmask_equal(gnodes, nodes)) {
		printf("nodes differ %lx, %lx!\n",
			gnodes->maskp[0], nodes->maskp[0]);
		ret = 1;
	}

	for (i = 0; i < repeat; i++)
		hog(map);
	exit(ret);
}
