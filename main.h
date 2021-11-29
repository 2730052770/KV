#ifndef MAIN_H
#define MAIN_H

#include <stdio.h> 
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
#define treetag(x) ((uint)((x)>>32 & 0x7fffffff))
#define buckettag(x) ((us)((x)>>16 & (BUCKET_NUM-1)))
#define entrytag(x) ((us)((x) & 0xffff))

#define TREE_TAG_INF ((uint)-1) 

#define REQ_GET 1
#define REQ_PUT 2
#define RESP_GET_SUCCESS 3
#define RESP_PUT_SUCCESS 4
#define RESP_GET_KEY_NOT_EXISTS 5
#define RESP_DELETE_KEY_NOT_EXISTS 6
#define NOT_COMPLETE(type) ((type) <= REQ_PUT)


typedef long long ll;
typedef unsigned long long ull;
typedef unsigned int uint;
typedef unsigned short us;
typedef unsigned char uc;
//on my computer
//L1 data cache = 32KB
//L2 cache = 256KB
//L3 cache = 12MB
const ll K = 1<<10, M = K*K, G = K*M;
const ll L1 = 32*K, L2 = 256*K, L3 = 12*M, MEM = G;
const uint PAGE_SIZE = 2*M;

const uint ALIGN = 64;
const uint MAX_BATCH = 8; // maybe 5
// can not be too big, otherwise the g++ will not flatten loops in -O3

const ull SPACE = 0x001000000000ull;//64GB
void* const BASE = (void*)0x700000000000ull;
#define BASE_ADDR(i) ((char*)BASE+SPACE*(i))

void* const NODE_BASE = BASE_ADDR(-2);//64 = 1
void* const INDEX_BASE = BASE_ADDR(-1);//8*(4096+512) = 36K ~= 600
void* const LOG_BASE/*unused*/ = BASE;//8*(4096+512)/8*64 ~= 300K ~= 5K

const uint GROUP_SIZE[] = {
	32, 40, 48, 64, 80, 96, 128, 160, 192, 256, 
	320, 384, 512, 640, 768, 1024, 1280, 1536, 2048, 2560, 
	3072, 4096, 5120, 6144, 8192, 10240, 12288, 16384, 20480, 24576, 
	32768, 40960//, 49152, 65536 // only contains 32 numbers
	/*
	// alternative
	16, 20, 24, 28, 32, 36, 40, 48, 56, 64, 
	72, 80, 96, 112, 128, 144, 160, 192, 224, 256, 
	288, 320, 384, 448, 512, 576, 640, 768, 896, 1024, 
	1152, 1280, 1536, 1792, 2048, 2304, 2560, 3072, 3584, 4096, 
	4608, 5120, 6144, 7168, 8192, 9216, 10240, 12288, 14336, 16384, 
	18432, 20480, 24576, 28672, 32768, 36864, 40960, 49152, 57344, 65536
	*/
};
#define NO_GROUP ((uc)-1)

const uint GROUP_NUM = sizeof(GROUP_SIZE)/sizeof(GROUP_SIZE[0]);// 32
const uint JMP_START = GROUP_NUM/2;



struct Node_entry{
	uint tag;// 0~0x7fffffff
	uint offset;
};

const uint FORK_NUM = 8;

union Node {//64 easy to calculate id
	uint father;
	Node_entry entry[FORK_NUM];
};// do not use sizeof(Node)

const uint NODE_SIZE = sizeof(Node);//64

struct Bucket_entry{//12
	uint offset;
	uc type;
	uc group;
	us tag;
	uint tree_tag;
};

#define ENTRY_TYPE_META 3
#define ENTRY_TYPE_INUSE 1
//#define ENTRY_TYPE_TOMB 2
#define ENTRY_TYPE_EMPTY 0

const uint BUCKET_LEN = 10;     

struct Bucket{// 128
	Bucket_entry entry[BUCKET_LEN];// BUCKET_LEN are suggested to be at least MAX_BATCH*2
	uint unused[2];
};

const uint BUCKET_SIZE = sizeof(Bucket);

const uint BUCKET_NUM = 32;
// 32 buckets of size 16 will have an average load-rate of 60%

union Index {//4096
	struct {
		uint father;
		uc first_entry_type;
		uc unused[3];
	}meta;
	Bucket bucket[BUCKET_NUM];//bucket[0].entry[0] is meta data
};

const uint INDEX_SIZE = sizeof(Index);

struct KV{// 3
	us len_value;
	uc len_key; 
	char content[];// key + value
}__attribute__((packed));
#define KV_SIZE(key_len,value_len) (sizeof(KV)+(key_len)+(value_len))
#define TEST_KEY_LEN 16
#define TEST_VALUE_LEN 45
#define TEST_KV_SIZE KV_SIZE(TEST_KEY_LEN, TEST_VALUE_LEN)



struct Block{
	uint father;
	uint offset;
	KV kv;
}__attribute__((packed));

#define BLOCK_SIZE(key_len,value_len) (KV_SIZE(key_len,value_len)+sizeof(Block)-sizeof(KV));

#define CAL_ID(ptr, base, delta) (((char*)(ptr)-(char*)(base))/(delta))
#define NODE_ID(ptr) CAL_ID(ptr, NODE_BASE, NODE_SIZE)
#define INDEX_ID(ptr) CAL_ID(ptr, INDEX_BASE, INDEX_SIZE)
#define BLOCK_ID(ptr) (((Block*)(ptr))->offset)

#define CAL_ADDR(id, base, size) ((void*)((char*)(base) + (ull)(size)*(id)))
#define ID2NODE(id) ((Node*)CAL_ADDR(id, NODE_BASE, NODE_SIZE))
#define ID2INDEX(id) ((Index*)CAL_ADDR(id, INDEX_BASE, INDEX_SIZE))
#define ID2BLOCK(id, group) ((Block*)CAL_ADDR(id, BASE_ADDR(group), GROUP_SIZE[group]))

#define OBJ_NEXT(ptr) (((void**)(ptr))[1])

#define FATHER(ptr) (*(uint*)(ptr))


#define FIND_EMPTY 1
#define FIND_MATCH 2
#define FIND_FULL 3


struct Query{
	uc type;// 1 REQ_GET, 2 REQ_PUT, 3 GET_SUC, 4 PUT_SUC, 
			// 5 GET_KEY_NOT_EXISTS, 6 DELETE_KEY_NOT_EXISTS
	uc group;// only for PUT
	uc entry_type;
	uc unused;
	uint entry_id;
	Index *index;
	union {
		Node_entry *node_entry;
		Bucket_entry *bucket_entry;
	}entry;
	uint tree_tag;
	us bucket_tag;
	us entry_tag;
	KV *q_kv;// outer kv of query
	Block *old_block;
	Block *new_block;// inner block
};

struct Allocator {
	char* const base;
	char* head;
	char* obj_tail;
	char* page_tail;
	const size_t obj_size;
	int page_num;
	int obj_num;// the number of in use and free ones
	int free_obj_num;
	const size_t page_size;
	//const int page_capacity;
	const int reserve_num;// cache
	const int batch_free_num;
	
	Allocator(size_t size, void* base, size_t _pgsz);
	Allocator(const Allocator & a);
	char* allocate_page();
	inline void* allocate() {
		if(!head) head = allocate_page();
		void* ret = head;
		head = (char*)OBJ_NEXT(head);
		free_obj_num --;
		return ret;
	}
	inline void free(void *ptr) {
		OBJ_NEXT(ptr) = head;
		head = (char*)ptr;
		free_obj_num ++;
	}
	void shrink();
};

struct Cacheline{
	ull mem[8];
}__attribute__((aligned(64)));

uint randuint();
ull randull();
inline ull myrand(ull *seed);

const int SHMKEY = 123456;
void* const SHMVA = (void*)0x600000000000ull;


#endif
