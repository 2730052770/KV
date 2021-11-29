#include "main.h"
#include <time.h>

struct share_mem{
	int signal;
	int ACK;
	int window;
	int n_split;
	volatile KV *kv[];
};

struct TEST_KV{// 3
	us len_value;
	uc len_key; 
	char content[TEST_KEY_LEN + TEST_VALUE_LEN];// key + value
}__attribute__((packed));
/*
struct STATISTICS{
	int fbs;
	int sbs;
	int 
}statistics;
*/
inline void volatile_cpy(void *e, volatile void *s, int len) {
	for(int i = 0; i < len; i++)
		((char*)e)[i] = ((volatile char*)s)[i];
}

inline void volatile_set(volatile void *e, char byte, int len) {
	for(int i = 0; i < len; i++)
		((volatile char*)e)[i] = byte;
}
ull get_time_ns()
{
	struct timespec tm;
	clock_gettime(CLOCK_MONOTONIC, &tm);
	return tm.tv_sec * 1000000000ull + tm.tv_nsec;
}
