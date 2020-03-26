/* Copyright (C) 2003,2004 Andi Kleen, SuSE Labs.

   numamon is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public
   License as published by the Free Software Foundation; version
   2.

   numamon is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should find a copy of v2 of the GNU General Public License somewhere
   on your Linux system; if not, write to the Free Software Foundation,
   Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

   Display some numa statistics collected by the CPU.
   Opteron specific. Also not reliable because the counters
   are not quite correct in hardware.  */

#define _LARGE_FILE_SOURCE 1
#define _GNU_SOURCE 1
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>
#include <getopt.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/fcntl.h>

enum { LOCALLOCAL = 0, LOCALREMOTE = 1, REMOTELOCAL = 2 };
static int mem[] = { [LOCALLOCAL] = 0xa8, [LOCALREMOTE] = 0x98, [REMOTELOCAL] = 0x68 };
static int io[] = {  [LOCALLOCAL] = 0xa4, [LOCALREMOTE] = 0x94, [REMOTELOCAL] = 0x64 };
static int *masks = mem;

#define err(x) perror(x),exit(1)

#define PERFEVTSEL0 0xc0010000
#define PERFEVTSEL1 0xc0010001
#define PERFEVTSEL2 0xc0010002
#define PERFEVTSEL3 0xc0010003

#define PERFCTR0 0xc0010004
#define PERFCTR1 0xc0010005
#define PERFCTR2 0xc0010006
#define PERFCTR3 0xc0010007

#define EVENT 0xe9
#define PERFEVTSEL_EN (1 << 22)
#define PERFEVTSEL_OS (1 << 17)
#define PERFEVTSEL_USR (1 << 16)

#define BASE (EVENT | PERFEVTSEL_EN | PERFEVTSEL_OS | PERFEVTSEL_USR)

#define MAXCPU 8

int force = 0;
int msrfd[MAXCPU];
int delay;
int absolute;
char *cfilter;
int verbose;

void usage(void);

void Vprintf(char *fmt, ...)
{
	va_list ap;
	va_start(ap,fmt);
	if (verbose)
		vfprintf(stderr,fmt,ap);
	va_end(ap);
}

unsigned long long rdmsr(int cpu, unsigned long msr)
{
	unsigned long long val;
	if (pread(msrfd[cpu], &val, 8, msr) != 8) {
		fprintf(stderr, "rdmsr of %lx failed: %s\n", msr, strerror(errno));
		exit(1);
	}
	return val;
}

void wrmsr(int cpu, unsigned long msr, unsigned long long value)
{
	if (pwrite(msrfd[cpu], &value, 8, msr) != 8) {
		fprintf(stderr, "wdmsr of %lx failed: %s\n", msr, strerror(errno));
		exit(1);
	}
}

int cpufilter(int cpu)
{
	long num;
	char *end;
	char *s;

	if (!cfilter)
		return 1;
	for (s = cfilter;;) {
		num = strtoul(s, &end, 0);
		if (end == s)
			usage();
		if (cpu == num)
			return 1;
		if (*end == ',')
			s = end+1;
		else if (*end == 0)
			break;
		else
			usage();
	}
	return 0;
}

void checkcounter(int cpu, int clear)
{
	int i;
	for (i = 1; i < 4; i++) {
		int clear_this = clear;
		unsigned long long evtsel = rdmsr(cpu, PERFEVTSEL0 + i);
		Vprintf("%d: %x %Lx\n", cpu, PERFEVTSEL0 + i, evtsel);
		if (!(evtsel & PERFEVTSEL_EN)) {
			Vprintf("reinit %d\n", cpu);
			wrmsr(cpu, PERFEVTSEL0 + i, BASE | masks[i - 1]);
			clear_this = 1;
		} else if (evtsel == (BASE | (masks[i-1] << 8))) {
			/* everything fine */
		} else if (force) {
			Vprintf("reinit force %d\n", cpu);
			wrmsr(cpu, PERFEVTSEL0 + i, BASE | (masks[i - 1] << 8));
			clear_this = 1;
		} else {
			fprintf(stderr, "perfctr %d cpu %d already used with %Lx\n",
				i, cpu, evtsel);
			fprintf(stderr, "Consider using -f if you know what you're doing.\n");
			exit(1);
		}
		if (clear_this) {
			Vprintf("clearing %d\n", cpu);
			wrmsr(cpu, PERFCTR0 + i, 0);
		}
	}
}

void setup(int clear)
{
	DIR *dir;
	struct dirent *d;
	int numcpus = 0;

	memset(msrfd, -1, sizeof(msrfd));
	dir = opendir("/dev/cpu");
	if (!dir)
		err("cannot open /dev/cpu");
	while ((d = readdir(dir)) != NULL) {
		char buf[64];
		char *end;
		long cpunum = strtoul(d->d_name, &end, 0);
		if (*end != 0)
			continue;
		if (cpunum > MAXCPU) {
			fprintf(stderr, "too many cpus %ld %s\n", cpunum, d->d_name);
			continue;
		}
		if (!cpufilter(cpunum))
			continue;
		snprintf(buf, 63, "/dev/cpu/%ld/msr", cpunum);
		msrfd[cpunum] = open64(buf, O_RDWR);
		if (msrfd[cpunum] < 0)
			continue;
		numcpus++;
		checkcounter(cpunum, clear);
	}
	closedir(dir);
	if (numcpus == 0) {
		fprintf(stderr, "No CPU found using MSR driver.\n");
		exit(1);
	}
}

void printf_padded(int pad, char *fmt, ...)
{
	char buf[pad + 1];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, pad, fmt, ap);
	printf("%-*s", pad, buf);
	va_end(ap);
}

void print_header(void)
{
	printf_padded(4, "CPU ");
	printf_padded(16, "LOCAL");
	printf_padded(16, "LOCAL->REMOTE");
	printf_padded(16, "REMOTE->LOCAL");
	putchar('\n');
}

void print_cpu(int cpu)
{
	int i;
	static unsigned long long lastval[4];
	printf_padded(4, "%d", cpu);
	for (i = 1; i < 4; i++) {
		unsigned long long val = rdmsr(cpu, PERFCTR0 + i);
		if (absolute)
			printf_padded(16, "%Lu", val);
		else
			printf_padded(16, "%Lu", val - lastval[i]);
		lastval[i] = val;
	}
	putchar('\n');
}

void dumpall(void)
{
	int cnt = 0;
	int cpu;
	print_header();
	for (;;) {
		for (cpu = 0; cpu < MAXCPU; ++cpu) {
			if (msrfd[cpu] < 0)
				continue;
			print_cpu(cpu);
		}
		if (!delay)
			break;
		sleep(delay);
		if (++cnt > 40) {
			cnt = 0;
			print_header();
		}
	}
}

void checkk8(void)
{
	char *line = NULL;
	size_t size = 0;
	int bad = 0;
	FILE *f = fopen("/proc/cpuinfo", "r");
	if (!f)
		return;
	while (getline(&line, &size, f) > 0) {
		if (!strncmp("vendor_id", line, 9)) {
			if (!strstr(line, "AMD"))
				bad++;
		}
		if (!strncmp("cpu family", line, 10)) {
			char *s = line + strcspn(line,":");
			int family;
			if (*s == ':') ++s;
			family = strtoul(s, NULL, 0);
			if (family != 15)
				bad++;
		}
	}
	if (bad) {
		printf("not a opteron cpu\n");
		exit(1);
	}
	free(line);
	fclose(f);
}

void usage(void)
{
	fprintf(stderr, "usage: numamon [args] [delay]\n");
	fprintf(stderr, "       -f forcibly overwrite counters\n");
	fprintf(stderr, "       -i count IO (default memory)\n");
	fprintf(stderr, "       -a print absolute counter values (with delay)\n");
	fprintf(stderr, "       -s setup counters and exit\n");
	fprintf(stderr, "       -c clear counters and exit\n");
	fprintf(stderr, "       -m Print memory traffic (default)\n");
	fprintf(stderr, "       -C cpu{,cpu} only print for cpus\n");
	fprintf(stderr, "       -v Be verbose\n");
	exit(1);
}

int main(int ac, char **av)
{
	int opt;
	checkk8();
	while ((opt = getopt(ac,av,"ifscmaC:v")) != -1) {
		switch (opt) {
		case 'f':
			force = 1;
			break;
		case 'c':
			setup(1);
			exit(0);
		case 's':
			setup(0);
			exit(0);
		case 'm':
			masks = mem;
			break;
		case 'i':
			masks = io;
			break;
		case 'a':
			absolute = 1;
			break;
		case 'C':
			cfilter = optarg;
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			usage();
		}
	}
	if (av[optind]) {
		char *end;
		delay = strtoul(av[optind], &end, 10);
		if (*end)
			usage();
		if (av[optind+1])
			usage();
	}

	setup(0);
	dumpall();
	return 0;
}
