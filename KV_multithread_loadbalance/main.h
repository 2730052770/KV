#ifndef MAIN_H
#define MAIN_H

#include "global.h"

typedef unsigned int key_type;
const uint test_len_val = 4;

const ll K = 1<<10, M = K*K, G = K*M;
const ll L1 = 32*K, L2 = 256*K, L3 = 12*M, MEM = G;



/* structure */

const uint ALIGN = 64;
const uint MAX_BATCH = 16; // maybe 5
// can not be too big, otherwise the g++ will not flatten loops in -O3

struct Node;

struct Node_entry{//12
	Node* ptr;
	key_type tag;// 0~0x7fffffff
	//uint unused;
}__attribute__((packed));

static_assert(sizeof(Node_entry)==12); 

const uint FORK_NUM = 20; 
const uint NODE_SECOND_STEP = (1<<(31-__builtin_clz(FORK_NUM))) / 2;
const uint NODE_FIRST_STEP = FORK_NUM - 2*NODE_SECOND_STEP;

#define VAL_MASK (0xfffffffeull << 32)
#define VAL_CHANGING_MASK (2ull << 32)
#define KEY_MASK (0xfffffffeull)
#define KEY_CHANGING_MASK (2ull)
#define TYPE_MASK (1ull)

inline bool key_range_changed(ull version_diff) {
	return (version_diff & KEY_MASK) != 0;
}

inline bool key_range_changed_or_changing(ull version, ull version_diff) {
	return (version & KEY_CHANGING_MASK) != 0 || key_range_changed(version_diff);
}

inline bool structure_changing(ull version) {
	return (version & VAL_CHANGING_MASK) != 0;
}

inline bool structure_changed(ull version_diff) {
	return (version_diff & VAL_MASK) != 0;
}

inline bool structure_changed_or_changing(ull version, ull version_diff) {
	return structure_changing(version) || structure_changed(version_diff);
}

struct Node {//256
	uint type : 1;// 0 Node, 1 KV_Node
	uint key_range_version : 31;// indicate if the key range of this node has changed 
	uint locked : 1;
	uint structure_version : 31;// indicate if the structure/son of this node has changed 
	Node_entry entry[FORK_NUM];
	ull unused;
	friend inline ull get_version(volatile Node *node) {
		return *(volatile ull*)node & 0xfffffffeffffffff;
	}
	friend inline void lock(volatile Node *node) {
		volatile uint *l = (volatile uint*)node + 1;
		while(1) {
			uint read_val = *l;
			if((read_val & 1) == 0) {
				if(__sync_bool_compare_and_swap(l, read_val, read_val | 1))
					break;
			}
		}
	}
	friend inline void unlock(volatile Node *node) {
		volatile uint *l = (volatile uint*)node + 1;
		*l = *l & ~(uint)1;
	}
	friend inline bool full(volatile Node *node) {
		for(uint i = 0; i < FORK_NUM; i++)
			if(node->entry[i].ptr == NULL)
				return false;
		return true;
	}
}__attribute__((packed));// do not use sizeof(Node)

struct KV{
	key_type key;
	uint len_val;
	char val[];
}__attribute__((packed));

struct KV_Node{
	uint type : 1;// 0 Node, 1 KV_Node
	uint modification_version : 31;// indicate if the value of this node has changed 
	KV kv;
	void init() {
		type = 1;
		if((modification_version & 1) == 0) modification_version++;// make it "modifying"
	}
	friend inline uint get_version(volatile KV_Node *node) {
		return *(volatile uint*)node;
	}
}__attribute__((packed));


#define MODIFY_MASK 0xfffffffe
#define MODIFYING_MASK 2
inline bool modified(uint version_diff) {
	return (version_diff & MODIFY_MASK) != 0;
}

inline bool modifying(uint version) {
	return (version & MODIFYING_MASK) != 0;
}


#define KV_SIZE(len_val) (sizeof(KV) + (len_val))
#define KV_NODE_SIZE(len_val) (sizeof(KV_Node) + (len_val))

/* query */

// query state
#define QUERY_COMPLETE 0
#define QUERY_FILLING 1
#define QUERY_NEW 2
#define QUERY_PROCESSING 3

// request type
#define REQ_GET 1
#define REQ_PUT 2
#define REQ_DELETE 3

// response type
#define RESP_HAS_KEY 1 // solved
#define RESP_NO_KEY 2 // solved


#define INT_STATE_INIT 1
#define INT_STATE_NODE 2
#define INT_STATE_FIN 3

struct Query{
	volatile uc q_state;
	volatile uc server_id;
	volatile uc req_type;// 1 REQ_GET, 2 REQ_PUT, 3 REQ_DELETE
	volatile uc resp_type;
	uc inter_state;
	
	key_type key;
	volatile KV *resp_kv;// query的kv和response copy出来的kv都在这里
	KV_Node *put_node;

	volatile Node *node, *father;
	ull fa_version;
	ull version;

	volatile Node *ptr_stk[20];
	ull version_stk[20];
	uint stk_size;

	/*
	int fail_cnt;
	inline bool lock(volatile ull *version) {
		retry:
		bool ret = (1ull & __sync_fetch_and_or(version, 1ull)) == 0ull;
		if(fail_cnt == 15 && !ret) goto retry;
		fail_cnt = ret ? 0: fail_cnt + 1;
		return ret;
	}
	*/
	inline bool lock(uc id) {
		return __sync_bool_compare_and_swap(&server_id, (uc)0, id);
	}
};

/* shared memory */

const int SHMKEY = 123456;
void* const SHMVA = (void*)0x600000000000ull;


inline void memfence()
{
#ifdef __x86_64__
	asm volatile ("" ::: "memory"); // this only limits the compiler
#else 
	static_assert(false);// you need add load-fence instruction on other platform
	//__sync_synchronize();
#endif
}

#endif