#include "global.h"
#include "kvp.h"
#include "kv.h"
#include "allocator.h"
//#include "log.h"
#include "test.h"

using namespace std;

KVS::KVS(int _tid, ull (*_hash) (char *, char *)) {

	tid = _tid;
	
	//logp.init(_tid);

	hash = _hash;
	init_allocator();
	init_structure();
}

KVS::KVS(const KVS &_) {
	assert(false && (&_ == &_));// make g++ happy
}

ull KVS::default_hash_function(char *st, char *ed)
{
	ull ret = 0;
	while(st + sizeof(ull) < ed) {
		ret ^= *(ull*)st;
		st += sizeof(ull);
	}
	for(int step = 0; st < ed; step+=8, st++) {
		ret ^= (ull)*st << step;
	}
	return ret;
}

void KVS::init_allocator() {
	new(&block_allocator) Allocator_pair();
	new(&node_allocator) Small_allocator(sizeof(Node), PAGE_SIZE, sizeof(Node), PAGE_SIZE);
}

void KVS::init_structure() {
	rt = (Node*)node_allocator.allocate();
	//setfather(rt, 0);//
	for(uint i = 1; i < NODE_FORK_NUM; i++)
		rt->entry[i].tag = TREE_TAG_INF;
		
	uint block_size = MY_BLOCK_SIZE(4, 4);
	Block *b = (Block*)block_allocator.allocate(block_size);
	//setfather(b, rt);

	rt->entry[0].tag = 0x80000000;
	setpureptr(&rt->entry[0].ptr, b);
	global_level = 1;
}

void KVS::replace(Node *node, void *old_son, void *new_son)
{
	for(uint i = 0; i < NODE_FORK_NUM; i++) {
		if(readptr(&node->entry[i].ptr) == old_son) {
			setptr(&node->entry[i].ptr, new_son);
		}
	}
}

void KVS::tree_insert(Node *node, uint tree_tag, void *old_son, void *new_son, uint node_level, uint id, uint batch) 
{
	uint pos, cnt;
	for(pos = cnt = 1; cnt < NODE_FORK_NUM; cnt++) {
		if(node->entry[cnt].tag == TREE_TAG_INF) // these 2 "if" can not change order
			break; 
		if(readptr(&node->entry[cnt].ptr) == old_son) 
			pos = cnt + 1;			// if pos has not been assigned here, then pos = 1;
	}
	if(old_son == NULL) pos = 0;
	
	//setfather(new_son, node);////////////////////////////////////////////////////////////////////////////////////////////
	
	if(likely(cnt < NODE_FORK_NUM)) {
		//puts("normal");
		for(uint i = cnt; i > pos; i--)
			node->entry[i] = node->entry[i-1];
		node->entry[pos].tag = tree_tag;
		setptr(&node->entry[pos].ptr, new_son);
	}
	else {
		Node *new_node = (Node*)node_allocator.allocate();
		static const uint l_cnt = NODE_FORK_NUM/2, r_cnt = NODE_FORK_NUM-l_cnt;
		//void* fp[r_cnt];
		// pos will never be 0
		if(pos <= l_cnt) {
			for(uint i = 0; i < r_cnt; i++) {
				new_node->entry[i] = node->entry[i + l_cnt];
				//fp[i] = readptr(&new_node->entry[i].ptr);
				//__builtin_prefetch(fp[i], 1, 0);
				for(uint q_id = 0; q_id < batch; q_id ++) {
					if(route[node_level + 1][q_id] == readptr(&new_node->entry[i].ptr))
						route[node_level][q_id] = new_node;
				}

			}
			for(uint i = l_cnt; i > pos; i--)
				node->entry[i] = node->entry[i-1];
			node->entry[pos].tag = tree_tag;
			setptr(&node->entry[pos].ptr, new_son);
		}
		else{
			// 0 ~ half, half+1 ~ NODE_FORK_NUM-1
			for(uint i = 0; i < pos-(l_cnt+1); i++) {
				new_node->entry[i] = node->entry[i + l_cnt + 1];
				//fp[i] = readptr(&new_node->entry[i].ptr);
				//__builtin_prefetch(fp[i], 1, 0);
				for(uint q_id = 0; q_id < batch; q_id ++) {
					if(route[node_level + 1][q_id] == readptr(&new_node->entry[i].ptr))
						route[node_level][q_id] = new_node;
				}
			}
			new_node->entry[pos-(l_cnt+1)].tag = tree_tag;
			setptr(&new_node->entry[pos-(l_cnt+1)].ptr, new_son);

			for(uint q_id = 0; q_id < batch; q_id ++) {
				if(route[node_level + 1][q_id] == new_son)
					route[node_level][q_id] = new_node;
			}

			//fp[pos-(l_cnt+1)] = new_son;
			// no prefetch
			for(uint i = pos-l_cnt; i < r_cnt; i++) {
				new_node->entry[i] = node->entry[i + l_cnt];
				//fp[i] = readptr(&new_node->entry[i].ptr);
				//__builtin_prefetch(fp[i], 1, 0);
				for(uint q_id = 0; q_id < batch; q_id ++) {
					if(route[node_level + 1][q_id] == readptr(&new_node->entry[i].ptr))
						route[node_level][q_id] = new_node;
				}
			}
		}
		for(uint i = l_cnt+1; i < NODE_FORK_NUM; i++)
			node->entry[i].tag = TREE_TAG_INF;// can change -1 to ...
		for(uint i = r_cnt; i < NODE_FORK_NUM; i++)
			new_node->entry[i].tag = TREE_TAG_INF;
		
		// son -> new father
		//for(uint i = 0; i < r_cnt; i++) 
		//	setfather(fp[i], new_node);/////////////////////////////////////////////////////////////////////////////
		// new father -> grandfather
		uint mid_tag = new_node->entry[0].tag;
		//readfather(node);
		if(node_level == 0) {
			global_level ++;
			rt = (Node*)node_allocator.allocate();
			//setfather(node, rt);
			//setfather(new_node, rt);
			
			//setfather(rt, NULL);
			for(uint l = global_level; l > 0; l--) 
				for(uint q_id = 0; q_id < batch; q_id++) 
					route[l][q_id] = route[l-1][q_id];
			for(uint q_id = 0; q_id < batch; q_id++) 
				route[0][q_id] = rt;


			rt->entry[0].tag = 0;
			setptr(&rt->entry[0].ptr, node);
			rt->entry[1].tag = mid_tag;
			setptr(&rt->entry[1].ptr, new_node);
			for(uint i = 2; i < NODE_FORK_NUM; i++)
				rt->entry[i].tag = TREE_TAG_INF;
		}
		else{
			void *father = route[node_level - 1][id];
			//setfather(new_node, father);
			// grandfather -> new father
			tree_insert((Node*)father, mid_tag, node, new_node, node_level - 1, id, batch);	
		}
	}
}


