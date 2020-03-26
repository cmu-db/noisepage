long stream_memsize(void);
long stream_init(void *mem);
#define STREAM_NRESULTS 4
void stream_test(double *res);
void stream_check(void);
void stream_setmem(unsigned long size);
extern int stream_verbose;
extern char *stream_names[];
