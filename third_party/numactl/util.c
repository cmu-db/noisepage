/* Copyright (C) 2003,2004 Andi Kleen, SuSE Labs.

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
#include "numa.h"
#include "numaif.h"
#include "util.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>

void printmask(char *name, struct bitmask *mask)
{
	int i;
	printf("%s: ", name);
	for (i = 0; i < mask->size; i++)
		if (numa_bitmask_isbitset(mask, i))
			printf("%d ", i);
	putchar('\n');
}

int find_first(struct bitmask *mask)
{
	int i;
	for (i = 0; i < mask->size; i++)
		if (numa_bitmask_isbitset(mask, i))
			return i;
	return -1;
}

void complain(char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "numactl: ");
	vfprintf(stderr,fmt,ap);
	putchar('\n');
	va_end(ap);
	exit(1);
}

void nerror(char *fmt, ...)
{
	int err = errno;
	va_list ap;
	va_start(ap,fmt);
	fprintf(stderr, "numactl: ");
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	if (err)
		fprintf(stderr,": %s\n", strerror(err));
	else
		fputc('\n', stderr);
	exit(1);
}

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

static struct policy {
	char *name;
	int policy;
	int noarg;
} policies[] = {
	{ "interleave", MPOL_INTERLEAVE, },
	{ "membind",    MPOL_BIND, },
	{ "preferred",   MPOL_PREFERRED, },
	{ "default",    MPOL_DEFAULT, 1 },
	{ NULL },
};

static char *policy_names[] = { "default", "preferred", "bind", "interleave" };

char *policy_name(int policy)
{
	static char buf[32];
	if (policy >= array_len(policy_names)) {
		sprintf(buf, "[%d]", policy);
		return buf;
	}
	return policy_names[policy];
}

int parse_policy(char *name, char *arg)
{
	int k;
	struct policy *p = NULL;
	if (!name)
		return MPOL_DEFAULT;
	for (k = 0; policies[k].name; k++) {
		p = &policies[k];
		if (!strcmp(p->name, name))
			break;
	}
	if (!p || !p->name || (!arg && !p->noarg))
		usage();
    return p->policy;
}

void print_policies(void)
{
	int i;
	printf("Policies:");
	for (i = 0; policies[i].name; i++)
		printf(" %s", policies[i].name);
	printf("\n");
}
