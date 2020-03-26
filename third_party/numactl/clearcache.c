/* Clear the CPU cache for benchmark purposes. Pretty simple minded.
 * Might not work in some complex cache topologies.
 * When you switch CPUs it's a good idea to clear the cache after testing
 * too.
 */
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "clearcache.h"

unsigned cache_size(void)
{
	unsigned cs = 0;
#ifdef _SC_LEVEL1_DCACHE_SIZE
	cs += sysconf(_SC_LEVEL1_DCACHE_SIZE);
#endif
#ifdef _SC_LEVEL2_CACHE_SIZE
	cs += sysconf(_SC_LEVEL2_CACHE_SIZE);
#endif
#ifdef _SC_LEVEL3_CACHE_SIZE
	cs += sysconf(_SC_LEVEL3_CACHE_SIZE);
#endif
#ifdef _SC_LEVEL4_CACHE_SIZE
	cs += sysconf(_SC_LEVEL4_CACHE_SIZE);
#endif
	if (cs == 0) {
		static int warned;
		if (!warned) {
			printf("Cannot determine CPU cache size\n");
			warned = 1;
		}
		cs = 64*1024*1024;
	}
	cs *= 2; /* safety factor */

	return cs;
}

void fallback_clearcache(void)
{
	static unsigned char *clearmem;
	unsigned cs = cache_size();
	unsigned i;

	if (!clearmem)
		clearmem = malloc(cs);
	if (!clearmem) {
		printf("Warning: cannot allocate %u bytes of clear cache buffer\n", cs);
		return;
	}
	for (i = 0; i < cs; i += 32)
		clearmem[i] = 1;
}

void clearcache(unsigned char *mem, unsigned size)
{
#if defined(__i386__) || defined(__x86_64__)
	unsigned i, cl, eax, feat;
	/* get clflush unit and feature */
	asm("cpuid" : "=a" (eax), "=b" (cl), "=d" (feat) : "0" (1) : "cx");
	if (!(feat & (1 << 19)))
		fallback_clearcache();
	cl = ((cl >> 8) & 0xff) * 8;
	for (i = 0; i < size; i += cl)
		asm("clflush %0" :: "m" (mem[i]));
#elif defined(__ia64__)
        unsigned long cl, endcl;
        // flush probable 128 byte cache lines (but possibly 64 bytes)
        cl = (unsigned long)mem;
        endcl = (unsigned long)(mem + (size-1));
        for (; cl <= endcl; cl += 64)
                asm ("fc %0" :: "r"(cl) : "memory" );
#else
#warning "Consider adding a clearcache implementation for your architecture"
	fallback_clearcache();
#endif
}
