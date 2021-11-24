#include "main.h"

struct share_mem{
	int signal;
	int ACK;
	int window;
	Query q[];
};

inline void volatile_cpy(void *e, volatile void *s, int len) {
	for(int i = 0; i < len; i++)
		((char*)e)[i] = ((volatile char*)s)[i];
}

inline void volatile_set(volatile void *e, char byte, int len) {
	for(int i = 0; i < len; i++)
		((volatile char*)e)[i] = byte;
}
