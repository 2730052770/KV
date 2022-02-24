#ifndef KV_H
#define KV_H

#include <utility>
#include "global.h"
#include "allocator.h"
using namespace std;
//#include "test.h"

#define treetag(x) ((uint)((x)>>32 & 0x7fffffff))
#define buckettag(x) ((us)((x)>>16 & (BUCKET_NUM-1)))
#define entrytag(x) ((us)((x) & 0xffff))

#define TREE_TAG_INF ((uint)-1) 

#define REQ_EMPTY 0
#define REQ_GET 1
#define REQ_PUT 2
#define REQ_DELETE 3

#define RESP_INIT 3 // only happen once
#define RESP_READ 4 // read but not solved
#define RESP_EMPTY 0 // new KV that hasn't been read
#define RESP_HAS_KEY 1 // solved
#define RESP_NO_KEY 2 // solved

#define BUCKET_ENTRY_TYPE_INUSE 1
#define BUCKET_ENTRY_TYPE_EMPTY 0

#define FIND_NULL 1
#define FIND_MATCH 2
#define FIND_FULL_FOR_PUT 3

#define KV_SIZE(key_len,value_len) (sizeof(KV)+(key_len)+(value_len))
#define BLOCK_SIZE(key_len,value_len) (KV_SIZE(key_len,value_len)+sizeof(Block)-sizeof(KV))

const uint MAX_BATCH = 12;// can not be too big, otherwise the g++ will not flatten loops in -O3
const ull aligned_addr_mask = (1ull << 48) - 4;// has 46 bit of 1
const ull full_addr_mask = (1ull << 48) - 1;

const uint NODE_FORK_NUM = 12; 
const uint NODE_SECOND_STEP = (1<<(31-__builtin_clz(NODE_FORK_NUM))) / 2;
const uint NODE_FIRST_STEP = NODE_FORK_NUM - 2*NODE_SECOND_STEP;

const uint BUCKET_LEN = 20; 

const uint BUCKET_NUM = 32;
// 32 buckets of size 16 will have an average load-rate of 60%

struct Ptr{
	char addr[6];
};

struct Meta_data{// 8
	uc allocator_meta_data;
	uc unused;
	Ptr father;
};

struct Virtual_node{
	Meta_data meta;
};

inline void * readptr(void * ptr) {
	return (void*)(*(ull*)ptr & aligned_addr_mask);
}

inline void setptr(void *ptr, void *tar) {
	ull *pu = (ull*)ptr;
	*pu = (*pu & ~aligned_addr_mask) | (ull)tar;
}

inline void setpureptr(Ptr *ptr, void *tar) {
	ull *pu = (ull*)ptr;
	*pu = (*pu & ~full_addr_mask) | (ull)tar;
}

inline void setfather(void *node, void *tar) {
	setpureptr(&((Virtual_node*)node)->meta.father, tar);
}

inline void * readfather(void *node) {
	return readptr(&((Virtual_node*)node)->meta.father);
}

struct Node_entry{//10
	Ptr ptr;
	uint tag;// 0~0x7fffffff
}__attribute__((packed));

struct Node {// 128
	Meta_data meta;
	Node_entry entry[NODE_FORK_NUM];
};

struct Bucket_entry{//12        
	ull type : 2;// 1 bit is enough		
	ull addr : 46;// address is 4 byte aligned
	ull tag : 16;
	uint tree_tag;
}__attribute__((packed));

struct Bucket{// 256
	char unused[16];
	Bucket_entry entry[BUCKET_LEN];
};

union Table {//16k (note that this is a union)
	Meta_data meta;
	Bucket bucket[BUCKET_NUM];
};

struct KV{// 3
	us len_value;
	uc len_key; 
	char content[];// key + value
}__attribute__((packed));

struct Block{
	Meta_data meta;
	KV kv;
}__attribute__((packed));


// CACHE
/*
const int CACHE_GROUP_LEN = 4;
const int CACHE_GROUP_NUM = 16;

struct Cache_entry{
	ull nxt : 2;
	ull block : 46;
	ull tag : 16;
};

struct Cache_group{
	Cache_entry[CACHE_GROUP_LEN];
};

struct Cache{
	Cache_group[CACHE_GROUP_NUM];
};
*/


struct TEST_Q{// 3
	uc req_type;// REQ_GET, REQ_PUT
	uc resp_type;
	KV kv;
};

struct Query{
	uc req_type;// 1 REQ_GET, 2 REQ_PUT, 3 REQ_DELETE
	uc resp_type;
		// 4 RESP_HAS_KEY (GET: success, PUT: success (update), DELETE: success)
		// 5 RESP_NO_KEY (GET: failed (return null), PUT: success (insert a new key), DELETE: failed)
	uc first_entry_type;
	uint first_entry_id;
	uint tree_tag;
	us bucket_tag;
	us entry_tag;
	volatile TEST_Q *tq;
	Table *table;
	union {
		Node_entry *node_entry;
		Bucket_entry *bucket_entry;
	}entry;
	KV *q_kv;// outer kv of query
	Block *old_block;
	Block *new_block;
};

struct KVS{
	Allocator_pair block_allocator;
	Small_allocator node_allocator, table_allocator;
	Node *rt;
	uint global_level;
	ull (*hash) (char *st, char *ed);
	//Cache cache;
	
	static ull default_hash_function(char *, char *);
	
	KVS(ull (*_hash) (char *, char *) = default_hash_function);
	KVS(const KVS &_);
	
	void init_allocator();
	
	void init_structure();
	
	template<uint batch>
	void tree_search(Query *q) {
		Node_entry *entry[batch];
		uint tag[batch];
		
		for(uint id = 0; id < batch; id++) {
			Query *qy = q+id;
			entry[id] = qy->entry.node_entry;
			tag[id] = qy->tree_tag;
		}
		
		for(uint id = 0; id < batch; id++) {
			Node_entry * nxt = entry[id] + NODE_FIRST_STEP;
			if(tag[id] >= nxt->tag)
				entry[id] = nxt;
		}
		
		for(uint jmp = NODE_SECOND_STEP; jmp; jmp>>=1) {
			for(uint id = 0; id < batch; id++) {
				Node_entry * nxt = entry[id] + jmp;
				if(tag[id] >= nxt->tag)
					entry[id] = nxt;
			}
		}
		
		for(uint lv = 1; lv < global_level; lv++) {
			for(uint id = 0; id < batch; id++) {
				Node *son = (Node*)readptr(&entry[id]->ptr);
				entry[id] = son->entry;
				__builtin_prefetch(son, 0, 0);
				__builtin_prefetch((char*)son+64, 0, 0);
				//__builtin_prefetch((char*)son+128, 0, 0);
				//__builtin_prefetch((char*)son+192, 0, 0);
				/*
				__builtin_prefetch((char*)son+256, 0, 0);
				__builtin_prefetch((char*)son+320, 0, 0);
				__builtin_prefetch((char*)son+384, 0, 0);
				__builtin_prefetch((char*)son+448, 0, 0);
				*/
			}
			
			for(uint id = 0; id < batch; id++) {
				Node_entry * nxt = entry[id] + NODE_FIRST_STEP;
				if(tag[id] >= nxt->tag)
					entry[id] = nxt;
			}
			
			for(uint jmp = NODE_SECOND_STEP; jmp; jmp>>=1) {
				for(uint id = 0; id < batch; id++) { 
					Node_entry * nxt = entry[id] + jmp;
					if(tag[id] >= nxt->tag)
						entry[id] = nxt;
				}
			}
		}
		
		for(uint id = 0; id < batch; id++) {
			Query *qy = q+id;
			Table * tb = (Table*)readptr(&entry[id]->ptr);
			uint bid = qy->bucket_tag;
			qy->table = tb;
			Bucket_entry * first = tb->bucket[bid].entry;
			qy->entry.bucket_entry = first;
			qy->first_entry_id = 0;
			__builtin_prefetch(first, 1, 0);
			__builtin_prefetch((char*)first+64, 1, 0);
			__builtin_prefetch((char*)first+128, 1, 0);
			__builtin_prefetch((char*)first+192, 1, 0);
		}
	}
	
	void first_bucket_search(uint batch, Query *q);
	
	void second_bucket_search(uint batch, Query *q);
	
	void tree_insert(Node *node, uint tree_tag, void *old_son, void *new_son);
	
	pair<Table*, uint> table_split(Query *qy);
	
	void put_one(Query *qy);
	
	template<uint batch>
	uint solve(uint unsolved, Query *q){
		//printf("solve\n");
		
		for(uint id = 0; id < unsolved; id++) 
			q[id].entry.node_entry = rt->entry;
		
		for(uint id = unsolved; id < batch; id++) {
			Query *qy = q + id;
			KV *kv = qy->q_kv;
			
			
			//log.record(qy->req_type, kv, time);
			
			
			qy->entry.node_entry = rt->entry;
			ull tag = hash(kv->content, kv->content+kv->len_key);
			
			qy->tree_tag = treetag(tag);
			//printf("%d %llx %d %llx\n", qy->tree_tag, tag, kv->len_key, *(ull*)kv->content);
			qy->bucket_tag = buckettag(tag);
			qy->entry_tag = entrytag(tag);
			
			// allocate a block for PUT
			if(qy->req_type == REQ_PUT) {
				uint kv_size = KV_SIZE(kv->len_key, kv->len_value);
				uint block_size = BLOCK_SIZE(kv->len_key, kv->len_value);
				qy->new_block = (Block*)block_allocator.allocate(block_size);// why this part takes a long time
				
				memcpy(&qy->new_block->kv, kv, kv_size);
			}
		}
		/*
		for(uint id = 0; id < unsolved; id++) {
			
		}
		*/
		// find table/bucket
		tree_search<batch>(q);
		
		// delay those who have conflicts (keep time order)
		unsolved = 0;
		
		for(uint id = 0; id<batch; id++) {
			Query *qy = q + id;
			//printf("repeat: %llu\n", *(ull*)qy->q_kv->content);
			for(uint pre = unsolved; pre < id; pre++) {
				Query *qp = q + pre;
				if(qp->entry.bucket_entry != qy->entry.bucket_entry) continue;
				//only check the first that match
				//if(qp->req_type == REQ_GET && qy->req_type == REQ_GET) break;// THIS WILL BREAK THE ORDER
				swap(q[unsolved++], *qy);
				break;
			}
		}

		//printf("unsolved %d\n", unsolved);
		// complete all quests that can be solved without extension
		first_bucket_search(batch-unsolved, q+unsolved);
		second_bucket_search(batch-unsolved, q+unsolved);
		
		for(uint id = unsolved; id < batch; id ++) {
			Query *qy = q + id, *qn;
			if(qy->resp_type != RESP_EMPTY) continue;
			pair<Table*, uint>p = table_split(qy);
			if(p.first != NULL) {
				Table* old_table = qy->table;
				for(uint nxt = id; nxt < batch; nxt++) {
					qn = q + nxt;
					if(qn->resp_type != RESP_EMPTY || qn->table != old_table || qn->tree_tag < p.second) 
						continue;
					qn->table = p.first;
				}
			}
			put_one(qy);
		}
		
		return unsolved;
	}
};

#endif
