#ifndef KV_H
#define KV_H

#include <utility>
#include "kvp.h"


#include "global.h"

#include "allocator.h"
//#include "log.h"

using namespace std;
//#include "test.h"

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
	uint tree_tag;
	volatile TEST_Q *tq;
	KV *q_kv;// outer kv of query
	Block *old_block;
	Block *new_block;
};

struct KVS{
	Allocator_pair block_allocator;
	Small_allocator node_allocator;
	Node *rt;
	uint global_level;
	ull (*hash) (char *st, char *ed);
	//Log_pair logp;
	int tid;
	//Cache cache;
	
	static ull default_hash_function(char *, char *);
	
	KVS(int _tid = 0, ull (*_hash) (char *, char *) = default_hash_function);
	KVS(const KVS &_);
	
	void init_allocator();
	
	void init_structure();
	
	template<uint batch>
	void tree_search(Query *q) {
		Node_entry *entry[batch] __attribute__((aligned(64)));
		uint tag[batch] __attribute__((aligned(64)));
		
		for(uint id = 0; id < batch; id++) {
			Query *qy = q+id;
			entry[id] = (Node_entry*)rt;
			tag[id] = qy->tree_tag;
		}
		
		for(uint lv = 0; lv < global_level; lv++) {
			for(uint id = 0; id < batch; id++) {
				Node *son = (Node*)entry[id];
				__builtin_prefetch(son, 0, 0);
				__builtin_prefetch((char*)son+64, 0, 0);//上面的prefetch远没有下面的重要
			}
			
			for(uint id = 0; id < batch; id++) { 
				
				//static Node nd __attribute__((aligned(64)));
				
				//char *nda = (char*)entry[id];
				
				//*(__m256i*) ((char*)&nd) = _mm256_load_si256((__m256i*) ((char*)nda));
				//*(__m256i*) ((char*)&nd + 32) = _mm256_load_si256((__m256i*) ((char*)nda + 32));
				//*(__m256i*) ((char*)&nd + 64) = _mm256_load_si256((__m256i*) ((char*)nda + 64));
				//*(__m256i*) ((char*)&nd + 96) = _mm256_load_si256((__m256i*) ((char*)nda + 96));
				
				//entry[id] = nd.entry;
				
				entry[id] = ((Node*)entry[id])->entry;
				
				Node_entry * nxt = entry[id] + NODE_FIRST_STEP;
				if(tag[id] >= nxt->tag)
					entry[id] = nxt;
				for(uint jmp = NODE_SECOND_STEP; jmp; jmp>>=1) { 
					nxt = entry[id] + jmp;
					if(tag[id] >= nxt->tag)
						entry[id] = nxt;
				}
				entry[id] = (Node_entry*)readptr(&entry[id]->ptr);
			}
		}
		
		// _mm512_store
		for(uint id = 0; id < batch; id++) {
			Query *qy = q+id;
			qy->old_block = (Block *)entry[id];
			__builtin_prefetch(qy->old_block, 0, 0);
		}
	}
	
	void tree_insert(Node *node, uint tree_tag, void *old_son, void *new_son);

	void replace(Node *node, void *old_son, void *new_son);
	
	template<uint batch>
	uint solve(uint unsolved, Query *q){
		//printf("solve\n");
		
		for(uint id = 0; id < batch; id++) {
			Query *qy = q + id;
			KV *kv = qy->q_kv;
			
			//if(qy->req_type != REQ_GET)
			//	logp.record(qy->req_type, kv, tm);
			
			qy->tree_tag = *(uint*)kv->content;
			assert((qy->tree_tag & (1<<31)) == 0);
			//printf("%d %llx %d %llx\n", qy->tree_tag, tag, kv->len_key, *(ull*)kv->content);
			
			// allocate a block for PUT
			if(qy->req_type == REQ_PUT) {
				uint kv_size = KV_SIZE(kv->len_key, kv->len_value);
				uint block_size = MY_BLOCK_SIZE(kv->len_key, kv->len_value);
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

		for(uint id = 0; id < batch; id ++) {
			Query *qy = q + id;
			if(qy->req_type == REQ_GET) {
				uint old_key = *(uint*)qy->old_block->kv.content;
				if(old_key == qy->tree_tag) {
					memcpy(qy->q_kv, &qy->old_block->kv, KV_SIZE(qy->old_block->kv.len_key, qy->old_block->kv.len_value));
					qy->resp_type = RESP_HAS_KEY;
				}
				else {
					qy->resp_type = RESP_NO_KEY;
				}
			}
		}
		for(uint id = batch-1; id < batch; id --) {// note id is unsigned
			Query *qy = q + id;
			if(qy->req_type == REQ_PUT && qy->resp_type == RESP_EMPTY) {
				uint old_key = *(uint*)qy->old_block->kv.content;
				Node *father = (Node *)readfather(qy->old_block);

				if(father == NULL) {
					int a = 0;
				}


				// 在replace和tree_ins前可能需要设置newblock的父亲
				if(old_key == qy->tree_tag) {
					replace(father, qy->old_block, qy->new_block);
					setfather(qy->new_block, father);
					block_allocator.free(qy->old_block);

					qy->resp_type = RESP_HAS_KEY;

					for(uint pre_id = 0; pre_id < id; pre_id ++) {
						if(q[pre_id].tree_tag == qy->tree_tag) 
							q[pre_id].resp_type = RESP_HAS_KEY;
					}
				}
				else {
					if(qy->tree_tag < old_key) qy->old_block = NULL;
					tree_insert(father, qy->tree_tag, qy->old_block, qy->new_block);
					
					bool first = 1;
					for(uint pre_id = 0; pre_id < id; pre_id ++) {
						if(q[pre_id].tree_tag == qy->tree_tag) {
							if(first) {
								q[pre_id].resp_type = RESP_NO_KEY;
								first = 0;
							}
							else {
								q[pre_id].resp_type = RESP_HAS_KEY;
							}
						}
					}
					if(first) qy->resp_type = RESP_NO_KEY;
					else qy->resp_type = RESP_HAS_KEY;
				}
				
			}
		}
		
		// delay those who have conflicts (keep time order)
		
		return 0;
	}
};

#endif
