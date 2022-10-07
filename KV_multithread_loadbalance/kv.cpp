/*
	单线程肯定没问题，多线程需要给每个thread分配一个内存分配器










*/

#include "main.h"
#include "allocator.h"
using namespace std;

#define TREE_TAG_INF ((key_type)0xffffffff) 

volatile Node * fa_rt;// the address of fa_rt is constant

struct thread_metadata{
	Small_allocator node_allocator;
	Allocator kv_node_allocator;
	thread_metadata() 
	: node_allocator(sizeof(Node), PAGE_SIZE, sizeof(Node))
	{ }
};

void init_tree(thread_metadata &tm)
{
	tm.node_allocator.allocate();// this node(id = 0) is useless
	fa_rt = (Node*)tm.node_allocator.allocate();
	Node *rt = (Node*)tm.node_allocator.allocate();
	*(ull*)fa_rt = 0;
	*(ull*)rt = 0;
	for(uint i = 0; i < FORK_NUM; i++) {
		fa_rt->entry[i].ptr = NULL;
		fa_rt->entry[i].tag = TREE_TAG_INF;

		rt->entry[i].ptr = NULL;
		rt->entry[i].tag = TREE_TAG_INF;// maximum and invalid
	}
	fa_rt->entry[0].ptr = rt;
	fa_rt->entry[0].tag = 0;

	KV_Node *dead_node = (KV_Node *)tm.kv_node_allocator.allocate(KV_NODE_SIZE(test_len_val));
	*(ull*)dead_node = 1;
	dead_node->kv.key = 0x80000000;
	dead_node->kv.len_val = test_len_val;
	*(key_type*)dead_node->kv.val = 0x80000000;

	rt->entry[0].ptr = (Node*)dead_node;
	rt->entry[0].tag = dead_node->kv.key;// this is accurate
}

// when entering this function, the state of q.node may NOT be static.
void iterate_node_repeat(Query &q, thread_metadata &tm) {
	// repeat when previous state is not static
loop:
	ull version = get_version(q.node);
	ull version_diff = version ^ q.version;
	if(key_range_changed_or_changing(version, version_diff)) {// key range change，只能重试
		q.inter_state = INT_STATE_INIT;
		return;
	}
	else {
		q.version = version;
		if(structure_changing(q.version)) {// structure changing
			goto loop;
		}
		// if val only changed (not changing), we can go done
	}

	// Now, the recorded state is a static state

	volatile Node_entry *entry = q.node->entry, *nxt;
	nxt = entry + NODE_FIRST_STEP;
	if(q.key >= nxt->tag) entry = nxt;
	for(uint step = NODE_SECOND_STEP; step; step >>= 1) {
		nxt = entry + step;
		if(q.key >= nxt->tag) entry = nxt;
	}
	assert(entry->ptr != NULL);

	q.fa_version = q.version;
	q.father = q.node;
	q.node = entry->ptr;
	q.version = 0;// to be assigned
	__builtin_prefetch((uc*)q.node, 0, 0);
	__builtin_prefetch((uc*)q.node + 64, 0, 0);
	__builtin_prefetch((uc*)q.node + 128, 0, 0);
}

void insert_or_split(Node *father, Node *&son, Node *&pre_son, key_type &ins_key, thread_metadata &tm) {
	// set "ins_pos" to "(uint)-1", in order to cause SEGMENTATION FAULT when mismatch.
	uint ins_pos = (uint)-1, cnt = 0;
	for(uint i = 0; i < FORK_NUM; i++) {
		if(father->entry[i].ptr == pre_son) ins_pos = i + 1;
		if(father->entry[i].ptr != NULL) cnt++;
	}
	if(pre_son == NULL) ins_pos = 0;
	
	if(cnt < FORK_NUM) {
		for(uint i = cnt; i > ins_pos; i--) 
			father->entry[i] = father->entry[i-1];
			
		father->entry[ins_pos] = {son, ins_key};

		pre_son = NULL;
		son = NULL;
		ins_key = TREE_TAG_INF;
		return;
	}

	Node *new_father = (Node *)tm.node_allocator.allocate();
	new_father->type = 0;
	new_father->locked = 0;
	if(new_father->key_range_version & 1) new_father->key_range_version++;
	if(new_father->structure_version & 1) new_father->structure_version++;

	const int l_num = (FORK_NUM + 1) / 2;
	const int r_num = FORK_NUM + 1 - l_num;
	if(ins_pos < l_num) {
		for(uint i = 0; i < r_num; i++)
			new_father->entry[i] = father->entry[l_num - 1 + i];
		for(uint i = r_num; i < FORK_NUM; i++)
			new_father->entry[i] = {NULL, TREE_TAG_INF};

		for(uint i = l_num - 1; i > ins_pos; i--)
			father->entry[i] = father->entry[i - 1];
		father->entry[ins_pos] = {son, ins_key};
		for(uint i = l_num; i < FORK_NUM; i++)
			father->entry[i] = {NULL, TREE_TAG_INF};
	}
	else {
		for(uint i = 0; i < ins_pos - l_num; i++)
			new_father->entry[i] = father->entry[l_num + i];
		new_father->entry[ins_pos - l_num] = {son, ins_key};
		for(uint i = ins_pos - l_num + 1; i < r_num; i ++)
			new_father->entry[i] = father->entry[l_num + i - 1];
		for(uint i = r_num; i < FORK_NUM; i++)
			new_father->entry[i] = {NULL, TREE_TAG_INF};

		for(uint i = l_num; i < FORK_NUM; i++)
			father->entry[i] = {NULL, TREE_TAG_INF};
	}
	pre_son = father;
	son = new_father;
	ins_key = new_father->entry[0].tag;
}

void iterate_kv_node_repeat(Query &q, thread_metadata &tm) {
	// DELETE: don't change key_version
	// REALLOCATE: key_version++
	// AFTER MODIFICATION: key_version++ (now it can be used again)
	assert(!modifying(q.version));
	volatile KV_Node *kv_node = (volatile KV_Node *)q.node;
	if(q.req_type == REQ_GET) {

		for(uint i = 0; i < KV_SIZE(test_len_val)/sizeof(uint); i++)
			((uint*)q.resp_kv)[i] = ((uint*)&kv_node->kv)[i];

		uint version = get_version(kv_node);
		uint version_diff = version ^ q.version;

		if(modified(version_diff)) {
			q.resp_type = RESP_NO_KEY;
			q.inter_state = INT_STATE_FIN;
			return;
		}
		q.resp_type = (q.resp_kv->key == q.key) ? RESP_HAS_KEY : RESP_NO_KEY;
		q.inter_state = INT_STATE_FIN;
		return;
	}
	else if(q.req_type == REQ_PUT) {
		lock(q.father);
		ull version = get_version(q.father);
		ull version_diff = version ^ q.version_stk[q.stk_size];
		if(key_range_changed_or_changing(version, version_diff)) {// (changing is impossible)
			unlock(q.father);
			q.inter_state = INT_STATE_INIT;
			return;
		}
		
		KV_Node **ptr_of_ptr;
		{
			volatile Node_entry *entry = q.father->entry, *nxt;
			nxt = entry + NODE_FIRST_STEP;
			if(q.key >= nxt->tag) entry = nxt;
			for(uint step = NODE_SECOND_STEP; step; step >>= 1) {
				nxt = entry + step;
				if(q.key >= nxt->tag) entry = nxt;
			}
			if(q.key < q.father->entry[0].tag) ptr_of_ptr = NULL;
			else ptr_of_ptr = (KV_Node**)&entry->ptr;
		}

		if(ptr_of_ptr != NULL && (*ptr_of_ptr) -> kv.key == q.key) {
			kv_node = *ptr_of_ptr;

			tm.kv_node_allocator.free((void*)kv_node);

//#define ADD_VERSION_ON_REPLACE
#ifdef ADD_VERSION_ON_REPLACE
			uint sv = q.father->structure_version;
			q.father->structure_version = sv + 1;
#endif
			*ptr_of_ptr = q.put_node;
#ifdef ADD_VERSION_ON_REPLACE
			q.father->structure_version = sv + 2;
#endif
			/*
				可能需要修改father的version
			*/

			q.put_node = NULL;// 避免出错

			unlock(q.father);

			q.resp_type = RESP_HAS_KEY;
			q.inter_state = INT_STATE_FIN;
			return;
		}
		else {
			uint top = q.stk_size;
			{
				volatile Node *ptr = q.ptr_stk[q.stk_size];
				if(full(ptr)) {
					for(uint i = q.stk_size - 1; i; i--) {
						ptr = q.ptr_stk[i];
						lock(ptr);
						version = get_version(ptr);
						version_diff = version ^ q.version_stk[i];
						if(key_range_changed_or_changing(version, version_diff)) {
							for(uint j = i; j <= q.stk_size; j++) {
								unlock(q.ptr_stk[j]);
							}
							q.inter_state = INT_STATE_INIT;
							return;
						}
						else {
							if(full(ptr)) {
								continue;
							}
							else {
								top = i;
								break;
							}
						}
					}
				}
			}
				

			// Now, we can insert

			// allocate kv_node
			key_type ins_key = q.put_node->kv.key;//
			Node *ins_node = (Node*) q.put_node;

			kv_node = q.put_node = NULL;// 避免出错

			Node *pre_node = (ptr_of_ptr == NULL) ? NULL : (Node*)*ptr_of_ptr;
			
			q.father->structure_version ++;

			if(q.stk_size > top)
				q.father->key_range_version ++;

			for(uint i = q.stk_size; i >= top; i--) {
				// this function will change pre_node & ins_node & ins_key
				if(i == 1) {// stk[1] == fa_rt
					Node *rt = (Node*)tm.node_allocator.allocate();
					rt->type = 0;
					rt->locked = 0;
					if(rt->key_range_version & 1) rt->key_range_version++;
					if(rt->structure_version & 1) rt->structure_version++;
					rt->entry[0] = {pre_node, 0};
					rt->entry[1] = {ins_node, ins_key};
					for(uint j = 2; j < FORK_NUM; j++)
						rt->entry[j] = {NULL, TREE_TAG_INF};
					fa_rt->entry[0].ptr = rt;
				}
				else {
					insert_or_split((Node*)q.ptr_stk[i], ins_node, pre_node, ins_key, tm);
				}
				// static -> dynamic
				if(i - 1 >= top) 
					q.ptr_stk[i - 1]->structure_version ++;
				if(i - 1 > top) 
					q.ptr_stk[i - 1]->key_range_version ++;
				
				// dynamic -> static
				if(i > top) 
					q.ptr_stk[i]->key_range_version ++;

				q.ptr_stk[i]->structure_version ++;

				unlock(q.ptr_stk[i]);
			}
			q.resp_type = RESP_NO_KEY;
			q.inter_state = INT_STATE_FIN;
			return;
		}
	}
}

// when entering this function, the recorded father's state (q.fa_version) must be static
void iterate_node(Query &q, thread_metadata &tm) {
	ull fa_version, fa_version_diff, version = -1;// assign useless "-1" to make gcc happy
	fa_version = get_version(q.father);// in case that we get a invalid pointer "q.node"
	fa_version_diff = fa_version ^ q.fa_version;
	if(!structure_changed(fa_version_diff)) {
		// if key_changed is true, then val_changed must be true
		// if val_changed is false, then "q.node" is a valid pointer. (not partial modified)
		version = get_version(q.node);
	}	
	fa_version = get_version(q.father);
	fa_version_diff = fa_version ^ q.fa_version;
	if(key_range_changed(fa_version_diff)) {// restart from root
		q.inter_state = INT_STATE_INIT;
		return;
	} 
	else if(structure_changed(fa_version_diff)){// restart from father
		q.node = q.father;
		q.version = fa_version;
		iterate_node_repeat(q, tm);
		return;
	}
	else {// go forward, assign "version"
		q.stk_size++;
		q.version_stk[q.stk_size] = q.fa_version;
		q.ptr_stk[q.stk_size] = q.father;

		if((version & TYPE_MASK) == 0) {
			q.version = version;
			iterate_node_repeat(q, tm);
		}
		else {
			q.version = (uint)version;// truncate
			iterate_kv_node_repeat(q, tm);
		}
	}
}

void iterate_init(Query &q, thread_metadata &tm) {
loop:
	q.fa_version = get_version(fa_rt);
	if(q.fa_version & 1) goto loop;// try next time

	q.father = fa_rt;
	q.node = fa_rt->entry[0].ptr;

	q.stk_size = 0;
	q.inter_state = INT_STATE_NODE;

	iterate_node(q, tm);
}



