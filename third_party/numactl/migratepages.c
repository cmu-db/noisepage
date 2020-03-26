/*
 * Copyright (C) 2005 Christoph Lameter, Silicon Graphics, Incorporated.
 * based on Andi Kleen's numactl.c.
 *
 * Manual process migration
 *
 * migratepages is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation; version 2.
 *
 * migratepages is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should find a copy of v2 of the GNU General Public License somewhere
 * on your Linux system; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

#define _GNU_SOURCE
#include <getopt.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include "numa.h"
#include "numaif.h"
#include "numaint.h"
#include "util.h"

struct option opts[] = {
	{"help", 0, 0, 'h' },
	{ 0 }
};

void usage(void)
{
	fprintf(stderr,
		"usage: migratepages pid from-nodes to-nodes\n"
		"\n"
		"nodes is a comma delimited list of node numbers or A-B ranges or all.\n"
);
	exit(1);
}

void checknuma(void)
{
	static int numa = -1;
	if (numa < 0) {
		if (numa_available() < 0)
			complain("This system does not support NUMA functionality");
	}
	numa = 0;
}

int main(int argc, char *argv[])
{
	int c;
	char *end;
	int rc;
	int pid;
	struct bitmask *fromnodes;
	struct bitmask *tonodes;

	while ((c = getopt_long(argc,argv,"h", opts, NULL)) != -1) {
		switch (c) {
		default:
			usage();
		}
	}

	argv += optind;
	argc -= optind;

	if (argc != 3)
		usage();

	checknuma();

	pid = strtoul(argv[0], &end, 0);
	if (*end || end == argv[0])
		usage();

	fromnodes = numa_parse_nodestring(argv[1]);
	if (!fromnodes) {
		printf ("<%s> is invalid\n", argv[1]);
		exit(1);
	}
	tonodes = numa_parse_nodestring(argv[2]);
	if (!tonodes) {
		printf ("<%s> is invalid\n", argv[2]);
		exit(1);
	}

	rc = numa_migrate_pages(pid, fromnodes, tonodes);

	if (rc < 0) {
		perror("migrate_pages");
		return 1;
	}
	return 0;
}
