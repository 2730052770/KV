#ifndef MAIN_H
#define MAIN_H

#include <stdio.h> 
#include <pthread.h>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define treetag(x) (((uint*)&(x))[1]&0x7fffffff)
#define buckettag(x) (((us*)&(x))[1]&(BUCKET_NUM-1))
#define entrytag(x) (((us*)&(x))[0])
#define REQ_GET 1
#define REQ_PUT 2
#define RESP_GET_SUCCESS 3
#define RESP_PUT_SUCCESS 4
#define RESP_GET_KEY_NOT_EXISTS 5
#define RESP_DELETE_KEY_NOT_EXISTS 6


typedef long long ll;
typedef unsigned long long ull;
typedef unsigned int uint;
typedef unsigned short us;
typedef unsigned char uc;
//on my computer
//L1 data cache = 32KB
//L2 cache = 256KB
//L3 cache = 12MB
#define K (1024ll)
#define M (K*K)
#define G (K*M)
#define L1 (32*K) 
#define L2 (256*K)
#define L3 (12*M)
#define MEM G;
#define PAGE_SIZE (2*M)

#define ALIGN 64
#define MAX_BATCH 8 // maybe 5
// can not be too big, otherwise the g++ will not flatten loops in -O3

#define SPACE 0x001000000000ull//64GB
#define BASE ((void*)0x700000000000ull)
#define BASE_ADDR(i) ((void*)((char*)BASE+SPACE*(i)))

#define NODE_BASE BASE_ADDR(-2)//64 = 1
#define INDEX_BASE BASE_ADDR(-1)//8*(4096+512) = 36K ~= 600

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

#define GROUP_NUM (sizeof(GROUP_SIZE)/sizeof(GROUP_SIZE[0]))// 32
#define JMP_START (GROUP_NUM/2)



struct Node_entry{
	uint tag;// 0~0x7fffffff
	uint offset;
};

#define FORK_NUM 8

union Node {//64 easy to calculate id
	uint father;
	Node_entry entry[FORK_NUM];
};// do not use sizeof(Node)

#define NODE_SIZE sizeof(Node)

struct Bucket_entry{//12
	uint offset;
	uc type;// 0 empty, 1 in use, 2 tomb, 3 index meta data
	uc group;
	us tag;
	uint tree_tag;
};

#define BUCKET_LEN 10

struct Bucket{// 128
	Bucket_entry entry[BUCKET_LEN];// BUCKET_LEN are suggested to be at least MAX_BATCH*2
	uint unused[2];
};

#define BUCKET_SIZE sizeof(Bucket)

#define BUCKET_NUM 32
// 32 buckets of size 16 will have an average load-rate of 60%

union Index {//4096
	struct {
		uint father;
		uc first_entry_type;
		uc unused[3];
	}meta;
	Bucket bucket[BUCKET_NUM];//bucket[0].entry[0] is meta data
};

#define INDEX_SIZE sizeof(Index)

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
	const uint offset;
	KV kv;
}__attribute__((packed));

#define BLOCK_SIZE(key_len,value_len) (KV_SIZE(key_len,value_len)+sizeof(Block)-sizeof(KV));

#define CAL_ID(ptr, base, delta) (((void*)(ptr)-(void*)(base))/(delta))
#define NODE_ID(ptr) CAL_ID(ptr, NODE_BASE, NODE_SIZE)
#define INDEX_ID(ptr) CAL_ID(ptr, INDEX_BASE, INDEX_SIZE)
#define BLOCK_ID(ptr) (((Block*)(ptr))->offset)

#define CAL_ADDR(id, base, size) ((void*)(base) + (ull)(size)*(id))
#define ID2NODE(id) ((Node*)CAL_ADDR(id, NODE_BASE, NODE_SIZE))
#define ID2INDEX(id) ((Index*)CAL_ADDR(id, INDEX_BASE, INDEX_SIZE))
#define ID2BLOCK(id, group) ((Block*)CAL_ADDR(id, BASE_ADDR(group), GROUP_SIZE[group]))

#define OBJ_NEXT(ptr) ((void**)(ptr)[1])

#define FATHER(ptr) (*(uint*)(ptr))

struct Query{
	uc type;// 1 GET, 2 PUT, 3 GET_SUC, 4 PUT_SUC, 
			// 5 GET_KEY_NOT_EXISTS, 6 DELETE_KEY_NOT_EXISTS
	uc group;// only for PUT
	uc unused[6];
	KV *q_kv;// outer kv of query
	Block *block;// inner block
};

struct Allocator {
	void* const base;
	void* head;
	void* obj_tail;
	void* page_tail;
	const size_t obj_size;
	int page_num;
	int obj_num;// the number of in use and free ones
	int free_obj_num;
	const size_t page_size;
	//const int page_capacity;
	const int reserve_num;// cache
	const int batch_free_num;
	
	Allocator(size_t size, void* base);
	void* allocate_page();
	void* allocate();
	void free(void *ptr);
	void shrink();
};


#define SHMKEY 123456
#define SHMVA 0x600000000000ull


#endif

