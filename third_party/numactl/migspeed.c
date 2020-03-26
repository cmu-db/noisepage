/*
 * Migration test program
 *
 * (C) 2007 Silicon Graphics, Inc. Christoph Lameter <clameter@sgi.com>
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include "numa.h"
#include "numaif.h"
#include <time.h>
#include <errno.h>
#include <malloc.h>
#include <unistd.h>
#include "util.h"

char *memory;

unsigned long pages = 1000;

unsigned long pagesize;

const char *optstr = "hvp:";

char *cmd;

int verbose;
struct timespec start,end;

void usage(void)
{
	printf("usage %s [-p pages] [-h] [-v] from-nodes to-nodes\n", cmd);
	printf("      from and to nodes may specified in form N or N-N\n");
	printf("      -p pages  number of pages to try (defaults to %ld)\n",
			pages);
	printf("      -v        verbose\n");
	printf("      -h        usage\n");
	exit(1);
}

void displaymap(void)
{
	FILE *f = fopen("/proc/self/numa_maps","r");

	if (!f) {
		printf("/proc/self/numa_maps not accessible.\n");
		exit(1);
	}

	while (!feof(f))
	{
		char buffer[2000];

		if (!fgets(buffer, sizeof(buffer), f))
			break;
		if (!strstr(buffer, "bind"))
			continue ;
		printf("%s", buffer);

	}
	fclose(f);
}

int main(int argc, char *argv[])
{
	char *p;
	int option;
	struct timespec result;
	unsigned long bytes;
	double duration, mbytes;
	struct bitmask *from;
	struct bitmask *to;

	pagesize = getpagesize();

	/* Command line processing */
	opterr = 1;
	cmd = argv[0];

	while ((option = getopt(argc, argv, optstr)) != EOF)
	switch (option) {
	case 'h' :
	case '?' :
		usage();
	case 'v' :
		verbose++;
		break;
	case 'p' :
		pages = strtoul(optarg, &p, 0);
		if (p == optarg || *p)
			usage();
		break;
	}

	if (!argv[optind])
		usage();

	if (verbose > 1)
		printf("numa_max_node = %d\n", numa_max_node());

	numa_exit_on_error = 1;

	from = numa_parse_nodestring(argv[optind]);
	if (!from) {
                printf ("<%s> is invalid\n", argv[optind]);
		exit(1);
	}
	if (errno) {
		perror("from mask");
		exit(1);
	}

	if (verbose)
		printmask("From", from);

	if (!argv[optind+1])
		usage();

	to = numa_parse_nodestring(argv[optind+1]);
	if (!to) {
                printf ("<%s> is invalid\n", argv[optind+1]);
		exit(1);
	}
	if (errno) {
		perror("to mask");
		exit(1);
	}

	if (verbose)
		printmask("To", to);

	bytes = pages * pagesize;

	if (verbose)
		printf("Allocating %lu pages of %lu bytes of memory\n",
				pages, pagesize);

	memory = memalign(pagesize, bytes);

	if (!memory) {
		printf("Out of Memory\n");
		exit(2);
	}

	if (mbind(memory, bytes, MPOL_BIND, from->maskp, from->size, 0) < 0)
		numa_error("mbind");

	if (verbose)
		printf("Dirtying memory....\n");

	for (p = memory; p <= memory + bytes; p += pagesize)
		*p = 1;

	if (verbose)
		printf("Starting test\n");

	displaymap();
	clock_gettime(CLOCK_REALTIME, &start);

	if (mbind(memory, bytes, MPOL_BIND, to->maskp, to->size, MPOL_MF_MOVE) <0)
		numa_error("memory move");

	clock_gettime(CLOCK_REALTIME, &end);
	displaymap();

	result.tv_sec = end.tv_sec - start.tv_sec;
	result.tv_nsec = end.tv_nsec - start.tv_nsec;

	if (result.tv_nsec < 0) {
		result.tv_sec--;
		result.tv_nsec += 1000000000;
	}

	if (result.tv_nsec >= 1000000000) {
		result.tv_sec++;
		result.tv_nsec -= 1000000000;
	}

	duration = result.tv_sec + result.tv_nsec / 1000000000.0;
	mbytes = bytes / (1024*1024.0);

	printf("%1.1f Mbyte migrated in %1.2f secs. %3.1f Mbytes/second\n",
			mbytes,
			duration,
			mbytes / duration);
	return 0;
}
