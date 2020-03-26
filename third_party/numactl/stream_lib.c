#include <stdio.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <sys/time.h>
#include <stdlib.h>
#include "stream_lib.h"

static inline double mysecond(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec + tv.tv_usec * 1.e-6;
}

/*
 * Program: Stream
 * Programmer: Joe R. Zagar
 * Revision: 4.0-BETA, October 24, 1995
 * Original code developed by John D. McCalpin
 *
 * This program measures memory transfer rates in MB/s for simple
 * computational kernels coded in C.  These numbers reveal the quality
 * of code generation for simple uncacheable kernels as well as showing
 * the cost of floating-point operations relative to memory accesses.
 *
 * INSTRUCTIONS:
 *
 *	1) Stream requires a good bit of memory to run.  Adjust the
 *          value of 'N' (below) to give a 'timing calibration' of
 *          at least 20 clock-ticks.  This will provide rate estimates
 *          that should be good to about 5% precision.
 *
 * Hacked by AK to be a library
 */

long N = 8000000;
#define NTIMES	10
#define OFFSET	0

/*
 *	3) Compile the code with full optimization.  Many compilers
 *	   generate unreasonably bad code before the optimizer tightens
 *	   things up.  If the results are unreasonably good, on the
 *	   other hand, the optimizer might be too smart for me!
 *
 *         Try compiling with:
 *               cc -O stream_d.c second_wall.c -o stream_d -lm
 *
 *         This is known to work on Cray, SGI, IBM, and Sun machines.
 *
 *
 *	4) Mail the results to mccalpin@cs.virginia.edu
 *	   Be sure to include:
 *		a) computer hardware model number and software revision
 *		b) the compiler flags
 *		c) all of the output from the test case.
 * Thanks!
 *
 */

int checktick(void);

# define HLINE "-------------------------------------------------------------\n"

# ifndef MIN
# define MIN(x,y) ((x)<(y)?(x):(y))
# endif
# ifndef MAX
# define MAX(x,y) ((x)>(y)?(x):(y))
# endif

static double *a, *b, *c;

static double rmstime[4] = { 0 }, maxtime[4] = {
0}, mintime[4] = {
FLT_MAX, FLT_MAX, FLT_MAX, FLT_MAX};

static char *label[4] = { "Copy:      ", "Scale:     ",
	"Add:       ", "Triad:     "
};
char *stream_names[] = { "Copy","Scale","Add","Triad" };

static double bytes[4];

int stream_verbose = 1;

#define Vprintf(x...) do { if (stream_verbose) printf(x); } while(0)

void stream_check(void)
{
	int quantum;
	int BytesPerWord;
	register int j;
	double t;

	/* --- SETUP --- determine precision and check timing --- */

	Vprintf(HLINE);
	BytesPerWord = sizeof(double);
	Vprintf("This system uses %d bytes per DOUBLE PRECISION word.\n",
	       BytesPerWord);

	Vprintf(HLINE);
	Vprintf("Array size = %lu, Offset = %d\n", N, OFFSET);
	Vprintf("Total memory required = %.1f MB.\n",
	       (3 * N * BytesPerWord) / 1048576.0);
	Vprintf("Each test is run %d times, but only\n", NTIMES);
	Vprintf("the *best* time for each is used.\n");

	/* Get initial value for system clock. */

	for (j = 0; j < N; j++) {
		a[j] = 1.0;
		b[j] = 2.0;
		c[j] = 0.0;
	}

	Vprintf(HLINE);

	if ((quantum = checktick()) >= 1)
		Vprintf("Your clock granularity/precision appears to be "
		       "%d microseconds.\n", quantum);
	else
		Vprintf("Your clock granularity appears to be "
		       "less than one microsecond.\n");

	t = mysecond();
	for (j = 0; j < N; j++)
		a[j] = 2.0E0 * a[j];
	t = 1.0E6 * (mysecond() - t);

	Vprintf("Each test below will take on the order"
	       " of %d microseconds.\n", (int) t);
	Vprintf("   (= %d clock ticks)\n", (int) (t / quantum));
	Vprintf("Increase the size of the arrays if this shows that\n");
	Vprintf("you are not getting at least 20 clock ticks per test.\n");

	Vprintf(HLINE);

	Vprintf("WARNING -- The above is only a rough guideline.\n");
	Vprintf("For best results, please be sure you know the\n");
	Vprintf("precision of your system timer.\n");
	Vprintf(HLINE);
}

void stream_test(double *res)
{
	register int j, k;
	double scalar, times[4][NTIMES];

	/*  --- MAIN LOOP --- repeat test cases NTIMES times --- */

	scalar = 3.0;
	for (k = 0; k < NTIMES; k++) {
		times[0][k] = mysecond();
		for (j = 0; j < N; j++)
			c[j] = a[j];
		times[0][k] = mysecond() - times[0][k];

		times[1][k] = mysecond();
		for (j = 0; j < N; j++)
			b[j] = scalar * c[j];
		times[1][k] = mysecond() - times[1][k];

		times[2][k] = mysecond();
		for (j = 0; j < N; j++)
			c[j] = a[j] + b[j];
		times[2][k] = mysecond() - times[2][k];

		times[3][k] = mysecond();
		for (j = 0; j < N; j++)
			a[j] = b[j] + scalar * c[j];
		times[3][k] = mysecond() - times[3][k];
	}

	/*  --- SUMMARY --- */

	for (k = 0; k < NTIMES; k++) {
		for (j = 0; j < 4; j++) {
			rmstime[j] =
			    rmstime[j] + (times[j][k] * times[j][k]);
			mintime[j] = MIN(mintime[j], times[j][k]);
			maxtime[j] = MAX(maxtime[j], times[j][k]);
		}
	}

	Vprintf
	    ("Function      Rate (MB/s)   RMS time     Min time     Max time\n");
	for (j = 0; j < 4; j++) {
		double speed = 1.0E-06 * bytes[j] / mintime[j];

		rmstime[j] = sqrt(rmstime[j] / (double) NTIMES);

		Vprintf("%s%11.4f  %11.4f  %11.4f  %11.4f\n", label[j],
			speed,
		       rmstime[j], mintime[j], maxtime[j]);

		if (res)
			res[j] = speed;

	}
}

# define	M	20

int checktick(void)
{
	int i, minDelta, Delta;
	double t1, t2, timesfound[M];

/*  Collect a sequence of M unique time values from the system. */

	for (i = 0; i < M; i++) {
		t1 = mysecond();
		while (((t2 = mysecond()) - t1) < 1.0E-6);
		timesfound[i] = t1 = t2;
	}

/*
 * Determine the minimum difference between these M values.
 * This result will be our estimate (in microseconds) for the
 * clock granularity.
 */

	minDelta = 1000000;
	for (i = 1; i < M; i++) {
		Delta =
		    (int) (1.0E6 * (timesfound[i] - timesfound[i - 1]));
		minDelta = MIN(minDelta, MAX(Delta, 0));
	}

	return (minDelta);
}

void stream_setmem(unsigned long size)
{
	N = (size - OFFSET) / (3*sizeof(double));
}

long stream_memsize(void)
{
	return 3*(sizeof(double) * (N+OFFSET)) ;
}

long stream_init(void *mem)
{
	int i;

	for (i = 0; i < 4; i++) {
		rmstime[i] = 0;
		maxtime[i] = 0;
		mintime[i] = FLT_MAX;
	}

	bytes[0] = 2 * sizeof(double) * N;
	bytes[1] = 2 * sizeof(double) * N;
	bytes[2] = 3 * sizeof(double) * N;
	bytes[3] = 3 * sizeof(double) * N;

	a = mem;
	b = (double *)mem +   (N+OFFSET);
	c = (double *)mem + 2*(N+OFFSET);
	stream_check();
	return 0;
}
