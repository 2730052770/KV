#ifndef GLOBAL_H
#define GLOBAL_H

#include <stdio.h> 
#include <algorithm>
#include <pthread.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <string.h>
#include <utility>
#include <assert.h>
#include <unistd.h>
#include <new>

#include <immintrin.h>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

typedef long long ll;
typedef unsigned long long ull;
typedef unsigned int uint;
typedef unsigned short us;
typedef unsigned char uc;

const uint PAGE_SIZE = 2 << 20;
const ll K = 1<<10, M = K*K, G = K*M;

inline ull get_time_ns()
{
	struct timespec tm;
	clock_gettime(CLOCK_MONOTONIC_RAW, &tm);
	return tm.tv_sec * 1000000000ull + tm.tv_nsec;// wrapped around every 585 years after starting up
}

inline void* mmap_hugepage(size_t size) {
	void *ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | 0x40000 /*HUGEPAGE*/, -1, 0);
	if(unlikely(ret == MAP_FAILED)) {
		puts("mmap failed");
		exit(0);
	}
	return ret;
}
inline void* mmap_page(size_t size) {
	void *ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if(unlikely(ret == MAP_FAILED)) {
		puts("mmap failed");
		exit(0);
	}
	return ret;
}

constexpr size_t remaining(size_t x) {
	return ((x + 63) & ~63) - x;
}

#endif
