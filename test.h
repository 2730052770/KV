#ifndef TEST_H
#define TEST_H

#include <string>
using namespace std;

#define MAX_TEST_BLOCK_SIZE 256
#define MAX_TEST_KV_SIZE (MAX_TEST_BLOCK_SIZE-sizeof(Block)+sizeof(KV))

#define MAX_TEST2_V_SIZE (MAX_TEST_KV_SIZE-sizeof(KV)-sizeof(ull))

#define TEST1_KEY_LEN 16
#define TEST1_VALUE_LEN 45

#define TEST1_Q_SIZE (KV_SIZE(TEST1_KEY_LEN, TEST1_VALUE_LEN) + sizeof(TEST_Q) - sizeof(KV))

#define TEST2_Q_SIZE (MAX_TEST_KV_SIZE + sizeof(TEST_Q) - sizeof(KV))

struct TEST_Q{// 3
	uc req_type;// REQ_GET, REQ_PUT
	uc resp_type;
	KV kv;
};

struct share_mem{
	int START;
	int S_ACK;
	int END;
	int E_ACK;
	int window;
	int n_split;
	volatile TEST_Q *tq[];
};

/*
struct STATISTICS{
	int fbs;
	int sbs;
	int 
}statistics;
*/
inline void volatile_cpy(volatile void *e, volatile void *s, int len) {
	for(int i = 0; i < len; i++)
		((char*)e)[i] = ((volatile char*)s)[i];
}

inline void volatile_set(volatile void *e, char byte, int len) {
	for(int i = 0; i < len; i++)
		((volatile char*)e)[i] = byte;
}

ull get_time_ns();
uint STL_randuint();
ull STL_randull();
ull fastrand(ull*);
string gen_str(int, int);

#define DEBUG

#endif
