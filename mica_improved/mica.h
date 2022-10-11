#define _GNU_SOURCE
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory.h>
#include <stdlib.h>
#include <time.h> 
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <assert.h>


#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define K ((size_t)1024)

#define M (K*K)

#define G (M*K)

/*
#define ENTRY_SIZE ((size_t)8)
#define NENTRY_PER_BUCKET 8
#define NBUCKET_PER_THREAD M_4

#define THREAD_INDEX_SIZE (ENTRY_SIZE * NENTRY_PER_BUCKET * NBUCKET_PER_THREAD)
#define THREAD_LOG_SIZE M_128
#define THREAD_INDEX_AND_LOG_SIZE (THREAD_INDEX_SIZE + THREAD_LOG_SIZE)

#define TOTAL_INDEX_SIZE (THREAD * THREAD_INDEX_SIZE)
#define TOTAL_LOG_SIZE (THREAD * THREAD_LOG_SIZE)
#define TOTAL_INDEX_AND_LOG_SIZE (THREAD * THREAD_INDEX_AND_LOG_SIZE)

#define NPIPELINE_SLOT 4

#define KEY_TO_BID(key) ((key>>16) & (NBUCKET_PER_THREAD-1))
#define KEY_TO_VID(key) (key & 0xffff)

#define POS_TO_OFF(tail) (tail & (THREAD_LOG_SIZE-1))

#define MAKE_ENTRY(tail, key) (tail<<16 | KEY_TO_VID(key))
#define ENTRY_TO_POS(entry) ((ull)entry >> 16) 
#define ENTRY_TO_OFF(entry) POS_TO_OFF(ENTRY_TO_POS(entry))
#define ENTRY_TO_VID(entry) (entry & 0xffff) 
#define ENTRY_EMPTY(entry) (entry == 0ull)
#define ENTRY_OUT_OF_DATE(entry, tail) (tail - ENTRY_TO_POS(entry) > THREAD_LOG_SIZE)
#define ENTRY_AVAILABLE(entry, tail) (ENTRY_EMPTY(entry) || ENTRY_OUT_OF_DATE(entry, tail))
*/

#define NENTRY 8 
#define NBUCKET (4 * M)
#define MAX_VAL_SIZE ((ll)(64 - (sizeof(struct Key) + sizeof(char)*2)))
#define LOG_BITS 29
#define LOGSIZE (1ll << LOG_BITS)
#define MAX_BATCH_SIZE 32

#define OP_GET 1
#define OP_PUT 2

#define RESP_GET_SUCCESS 3
#define RESP_PUT 4
#define RESP_GET_FAILED 5

typedef long long ll;
typedef unsigned long long ull;
typedef unsigned int uint;

struct Key{
  ull __unused : 64;
  uint bkt : 32;
  uint server : 16;
  uint tag : 16;
};

struct Entry{
  ull offset : 40;
  ull in_use : 8;
  ull tag : 16;
};

struct Bucket{
  struct Entry entry[NENTRY];
};

struct Table{
  struct Bucket *index;
  char *log;
  int id;
  int nbkt;
  int bkt_mask;
  int nentry;
  ll logsize;
  ll logmask;
  ll loghead;
  
  int nget_success;
  int nget_failed;
  int nput;
  int nevict;
};

struct Resp{
  char type;
  char val_len;
  short unused[3];
  char *val_ptr;
};

struct Op{
  struct Key key;
  char opcode;
  char val_len;
  char value[MAX_VAL_SIZE];
};

void run_test(struct Table *tb);


