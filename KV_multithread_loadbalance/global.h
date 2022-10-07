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


#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

typedef long long ll;
typedef unsigned long long ull;
typedef unsigned int uint;
typedef unsigned short us;
typedef unsigned char uc;

const int PAGE_SIZE = 2 << 20;

#endif
