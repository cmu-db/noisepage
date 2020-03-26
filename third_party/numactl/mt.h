#define MT_LEN	     624

extern void mt_init(void);
extern void mt_refill(void);

extern int mt_index;
extern unsigned int mt_buffer[MT_LEN];

static inline unsigned int mt_random(void)
{
    unsigned int * b = mt_buffer;
    int idx = mt_index;

    if (idx == MT_LEN*sizeof(unsigned int)) {
	    mt_refill();
	    idx = 0;
    }
    mt_index += sizeof(unsigned int);
    return *(unsigned int *)((unsigned char *)b + idx);
}
