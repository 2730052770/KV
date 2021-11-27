#include "main.h"
#include "test.h"
using namespace std;

ull allocator_buffer[sizeof(Allocator)*(2+GROUP_NUM)/8];
Allocator *node_allocator = (Allocator *)allocator_buffer;// -2
Allocator *index_allocator = node_allocator + 1;// -1
Allocator *ray_allocator = node_allocator + 2;// 0
Node *rt;
uint global_level;

void init_allocators()
{
	new(node_allocator) Allocator(NODE_SIZE, NODE_BASE, PAGE_SIZE);
	new(index_allocator) Allocator(INDEX_SIZE, INDEX_BASE, PAGE_SIZE);
	for(uint i = 0; i < GROUP_NUM; i++) 
		new(ray_allocator+i) Allocator(GROUP_SIZE[i], BASE_ADDR(i), PAGE_SIZE);
}

void init_tree()
{
	node_allocator->allocate();// this node(id = 0) is useless
	rt = (Node*)node_allocator->allocate();
	rt->father = 0;// type of father is unsigned int
	for(uint i = 1; i < FORK_NUM; i++)
		rt->entry[i].tag = TREE_TAG_MAX;// maximum and invalid
		
	Index *t = (Index*)index_allocator->allocate();
	memset(t, 0, index_allocator->obj_size);
	t->meta.first_entry_type = ENTRY_TYPE_META;
	t->meta.father = 1;
	rt->entry[0].offset = 0;
	global_level = 1;
}

template<uint batch>
void tree_search(Query *q)
{
	printf("step2\n");
	
	Node_entry *entry[batch];
	uint tag[batch];
	
	for(uint id = 0; id < batch; id++) {
		Query *qy = q+id;
		entry[id] = qy->entry.node_entry;
		tag[id] = qy->tree_tag;
	}
	
	for(uint jmp = FORK_NUM>>1; jmp; jmp>>=1) {
		for(uint id = 0; id < batch; id++) {
			Node_entry * nxt = entry[id] + jmp;
			if(tag[id] >= nxt->tag)
				entry[id] = nxt;
		}
	}
	
	printf("step3\n");
	
	for(uint lv = 1; lv < global_level; lv++) {// the nodes have $level different kind of hight
		for(uint id = 0; id < batch; id++) {
			Node *son = (Node*)NODE_BASE + entry[id]->offset;
			entry[id] = son->entry;// to be jump to level $lv
			__builtin_prefetch(entry[id], 0, 0);
		}
		
		for(uint jmp = FORK_NUM>>1; jmp; jmp>>=1) {//  now in level $lv, choose a son
			for(uint id = 0; id < batch; id++) { 
				Node_entry * nxt = entry[id] + jmp;
				if(tag[id] >= nxt->tag)
					entry[id] = nxt;
			}
		}
	}
	
	printf("step4\n");
	
	for(uint id = 0; id < batch; id++) {
		Query *qy = q+id;
		Index * idx = (Index*)INDEX_BASE + entry[id]->offset;
		uint bid = qy->bucket_tag;
		qy->index = idx;
		Bucket_entry * first = idx->bucket[bid].entry;
		qy->entry.bucket_entry = first;//this is Bucket_entry* in fact
		qy->entry_id = 0;
		__builtin_prefetch(first, 1, 0);
	}
}
void (*tree_search_table[]) (Query *) = {
	tree_search<0>, tree_search<1>, tree_search<2>, tree_search<3>, tree_search<4>,
	tree_search<5>, tree_search<6>, tree_search<7>, tree_search<8>
};


template<uint batch>
void first_bucket_search(uint n_put, Query *q)
{
	//这里还可以尝试换换两个循环的顺序，需要一些技巧跳过已经查完的
	//这里我可以手动展开+跳转实现对变长的支持,先摆烂
	// switch 就行 
	//这里可以让两个tag分别从两端向中间查 
	if(n_put > batch) return;

	Query *qy;
	Block *old_block;
	KV *old_kv, *new_kv;
	Bucket_entry *entry;
	
	for(uint id = 0; id < batch; id++) { // PUT
		qy = q+id;
		entry = qy->entry.bucket_entry;
		uint tree_tag = qy->tree_tag;
		us entry_tag = qy->entry_tag;
		uint col;
		for(col = 0; col < BUCKET_LEN; col++, entry++) {
			if(entry->type == ENTRY_TYPE_EMPTY || 
			   (entry->type == ENTRY_TYPE_INUSE && entry->tag == entry_tag && entry->tree_tag == tree_tag)) 
			   break;
		}
		if(col==BUCKET_LEN) qy->entry_type = FIND_FULL;// do we need pre-extend?
		else if(entry->type == ENTRY_TYPE_INUSE) {
			qy->entry_type = FIND_MATCH;
			old_block = ID2BLOCK(entry->offset, entry->group);
			qy->old_block = old_block;
			__builtin_prefetch(old_block, 0, 0);
		}
		else qy->entry_type = FIND_EMPTY;
		qy->entry.bucket_entry = entry;
		qy->entry_id = col;
	}
	for(uint id = 0; id < n_put; id++) {
		qy = q + id;
		entry = qy->entry.bucket_entry;
		switch(qy->entry_type){
			case FIND_EMPTY:// PUT not match / DELETE_LEY_NOT_EXISTS
				if(qy->group == NO_GROUP) {
					qy->type = RESP_DELETE_KEY_NOT_EXISTS;
				}
				else {
					qy->type = RESP_PUT_SUCCESS;
					entry->type = 1;
					entry->group = qy->group;
					entry->tag = qy->entry_tag;
					entry->tree_tag = qy->tree_tag;
					entry->offset = qy->new_block->offset;
					qy->new_block->father = INDEX_ID(qy->index);
				}
				break;
			case FIND_MATCH:// PUT/DELETE match
				old_block = qy->old_block;
				old_kv = &old_block->kv;
				new_kv = qy->q_kv;
				if(old_kv->len_key == new_kv->len_key && 
				   memcmp(old_kv->content, new_kv->content, old_kv->len_key) == 0) {
					// if really match
				   	qy->type = RESP_PUT_SUCCESS;
				   	
				   	ray_allocator[entry->group].free(old_block);
				   	
				   	if(qy->group == NO_GROUP) {// delete
				   		entry->type = ENTRY_TYPE_EMPTY;
				   		uint col = qy->entry_id + 1;
				   		Bucket_entry *last = qy->entry.bucket_entry + 1;
				   		for(; col < BUCKET_LEN; col++, last++)
				   			if(last->type == ENTRY_TYPE_EMPTY)
				   				break;
				   		last--;
				   		swap(*last, *entry);
					}
					else {
						entry->group = qy->group;
					   	entry->offset = qy->new_block->offset;
					   	qy->new_block->father = INDEX_ID(qy->index);
					}
				}
				break;
		}
	}
	for(uint id = n_put; id < batch; id++) {
		qy = q + id;
		switch(qy->entry_type){
			case FIND_EMPTY:// GET KEY NOT EXISTS
			case FIND_FULL:
				qy->type = RESP_GET_KEY_NOT_EXISTS;
				break;
			case FIND_MATCH:// GET match
				old_block = qy->old_block;
				old_kv = &old_block->kv;
				new_kv = qy->q_kv;
				if(old_kv->len_key == new_kv->len_key && 
				   memcmp(old_kv->content, new_kv->content, old_kv->len_key) == 0) {
					// if really match
				   	qy->type = RESP_GET_SUCCESS;
				}
				break;
		}
	}
}
void (*first_bucket_search_table[]) (uint, Query*) = {
	first_bucket_search<0>, first_bucket_search<1>, first_bucket_search<2>, first_bucket_search<3>,first_bucket_search<4>,
	first_bucket_search<5>, first_bucket_search<6>, first_bucket_search<7>, first_bucket_search<8>,
};

void second_bucket_search(uint n_op, Query *q)
{
	Query *qy;
	Block *old_block;
	KV *old_kv, *new_kv;
	Bucket_entry *entry;
	
	for(uint id = 0; id < n_op; id++) if(NOT_COMPLETE(q[id].type) && q[id].entry_type == FIND_MATCH){ // PUT
		qy = q+id;
		entry = qy->entry.bucket_entry + 1;
		uint tree_tag = qy->tree_tag;
		us entry_tag = qy->entry_tag;
		uint col = qy->entry_id + 1;
		for(; col < BUCKET_LEN; col++, entry++) {
			if(entry->type == ENTRY_TYPE_EMPTY) {
				if(qy->type == REQ_PUT) {
					if(qy->group == NO_GROUP) {
						qy->type = RESP_DELETE_KEY_NOT_EXISTS;
					}
					else {
						qy->type = RESP_PUT_SUCCESS;
						entry->type = 1;
						entry->group = qy->group;
						entry->tag = qy->entry_tag;
						entry->tree_tag = qy->tree_tag;
						entry->offset = qy->new_block->offset;
						qy->new_block->father = INDEX_ID(qy->index);
					}
				}
				else {
					qy->type = RESP_GET_KEY_NOT_EXISTS;
				}
				break;
			}
			else if(entry->type == ENTRY_TYPE_INUSE && entry->tag == entry_tag && entry->tree_tag == tree_tag) { 
				old_block = ID2BLOCK(entry->offset, entry->group);
				old_kv = &old_block->kv;
				new_kv = qy->q_kv;
				if(old_kv->len_key != new_kv->len_key || 
				   memcmp(old_kv->content, new_kv->content, old_kv->len_key) != 0)
					continue;
				if(qy->type == REQ_PUT) {
					qy->type = RESP_PUT_SUCCESS;
				   	
				   	ray_allocator[entry->group].free(old_block);
				   	
				   	if(qy->group == NO_GROUP) {// delete
				   		entry->type = ENTRY_TYPE_EMPTY;
				   		uint ncol = col + 1;
				   		Bucket_entry *last = entry + 1;
				   		for(; ncol < BUCKET_LEN; ncol++, last++)
				   			if(last->type == ENTRY_TYPE_EMPTY)
				   				break;
				   		last--;
				   		swap(*last, *entry);
					}
					else {
						entry->group = qy->group;
					   	entry->offset = qy->new_block->offset;
					   	qy->new_block->father = INDEX_ID(qy->index);
					}
				}
				else {
				   	qy->type = RESP_GET_SUCCESS;
					qy->old_block = old_block;
				}
				break;
			}
		}
		if(col != BUCKET_LEN) continue;
		if(qy->type == REQ_GET) 
			qy->type = RESP_GET_KEY_NOT_EXISTS;
		else if(qy->group == NO_GROUP) 
			qy->type = RESP_DELETE_KEY_NOT_EXISTS;
		else
			qy->entry_type = FIND_FULL;
	}
}

void bucket_search(uint n_put, uint n_op, Query *q)
{
	printf("step5\n");
	//你需要考虑：GET after PUT, PUT after same PUT, PUT after different PUT, PUT after DEL, DEL after PUT...
	//不如直接BUCKET互斥
	first_bucket_search_table[n_op](n_put, q);
	second_bucket_search(n_op, q);
}

void tree_insert(Node *ptr, uint tree_tag, uint old_offset, uint offset, uint leaf)
{
	uint pos, cnt;
	for(cnt = 0; cnt < FORK_NUM; cnt++) {
		if(ptr->entry[cnt].offset == old_offset) {
			pos = cnt + 1;
		}
		if(ptr->entry[cnt].tag == TREE_TAG_MAX)
			break; 
	}
	if(cnt < FORK_NUM) {
		for(uint i = cnt; i > pos; i--)
			ptr->entry[i] = ptr->entry[i-1];
		ptr->entry[pos].tag = tree_tag;
		ptr->entry[pos].offset = offset;
	}
	else {
		Node *new_ptr = (Node*)node_allocator->allocate();
		static const uint half = FORK_NUM/2;
		void* fp[half];
		// pos will never be 0
		if(pos <= half) {
			for(uint i = 0; i < half; i++) {
				new_ptr->entry[i] = ptr->entry[i + half];
				uint son_offset = new_ptr->entry[i].offset;
				fp[i] = leaf ? (void*)ID2INDEX(son_offset) : (void*)ID2NODE(son_offset);
				__builtin_prefetch(fp[i], 1, 0);
			}
			for(uint i = half; i > pos; i--)
				ptr->entry[i] = ptr->entry[i-1];
			ptr->entry[pos].tag = tree_tag;
			ptr->entry[pos].offset = offset;
		}
		else{
			// 0 ~ half, half+1 ~ FORK_NUM-1
			for(uint i = 0; i < pos-(half+1); i++) {
				new_ptr->entry[i] = ptr->entry[i + half + 1];
				uint son_offset = new_ptr->entry[i].offset;
				fp[i] = leaf ? (void*)ID2INDEX(son_offset) : (void*)ID2NODE(son_offset);
				__builtin_prefetch(fp[i], 1, 0);
			}
			new_ptr->entry[pos-(half+1)].tag = tree_tag;
			new_ptr->entry[pos-(half+1)].offset = offset;
			fp[pos-(half+1)] = leaf ? (void*)ID2INDEX(offset) : (void*)ID2NODE(offset);
			// no prefetch
			for(uint i = pos-half; i < half; i++) {
				new_ptr->entry[i] = ptr->entry[i + half];
				uint son_offset = new_ptr->entry[i].offset;
				fp[i] = leaf ? (void*)ID2INDEX(son_offset) : (void*)ID2NODE(son_offset);
				__builtin_prefetch(fp[i], 1, 0);
			}
		}
		for(uint i = half+1; i < FORK_NUM; i++)
			ptr->entry[i].tag = TREE_TAG_MAX;
		for(uint i = half; i < FORK_NUM; i++)
			new_ptr->entry[i].tag = TREE_TAG_MAX;
		
		// son -> new father
		uint new_id = NODE_ID(new_ptr);
		uint old_id = NODE_ID(ptr);
		for(uint i = 0; i < half; i++) 
			FATHER(fp[i]) = new_id;
		// new father -> grandfather
		uint mid_tag = new_ptr->entry[0].tag;// these two share the same memory
		if(ptr->father == 0) {
			global_level ++;
			rt = (Node*)node_allocator->allocate();
			ptr->father = new_ptr->father = NODE_ID(rt);
			
			rt->father = 0;
			rt->entry[0].offset = old_id;
			rt->entry[1].tag = mid_tag;
			rt->entry[1].offset = new_id;
			for(uint i = 2; i < FORK_NUM; i++)
				rt->entry[i].tag = TREE_TAG_MAX;
		}
		else{
			new_ptr->father = ptr->father;
			// grandfather -> new father
			tree_insert(ID2NODE(ptr->father), mid_tag, old_id, new_id, 0);	
		}
	}
}

void index_split(Index *index, ull bucket_tag) {// tag is in the line which is full 

	// get mid_tag
	uint line = bucket_tag;
	Bucket_entry *entrys = index->bucket[line].entry;
	/*
	Block * blocks[BUCKET_LEN];
	for(uint col = 0; col < BUCKET_LEN; col++) {
		//既然反正都要改block,那就都从block里读需要的信息吧
		blocks[col] = ID2BLOCK(entrys[col]->offset, entrys[col]->group);
		//__builtin_prefetch(blocks[col], 1, 0);
	}
	*/
	const uint size = BUCKET_LEN/2;
	uint tree_tags[size];
	for(uint col = 0; col < size; col++) {// insertion sort
		uint element = entrys[col].tree_tag;
		uint pos = 0;
		for(; pos < col; pos++)
			if(element > tree_tags[pos])// big -> small
				break;
		for(uint i = col; i > pos; i--)
			tree_tags[i] = tree_tags[i-1];
		tree_tags[pos] = element;
	}
	for(uint col = size; col < BUCKET_LEN; col++) {
		uint element = entrys[col].tree_tag;
		uint pos = 0;
		for(; pos < size; pos++)
			if(element > tree_tags[pos])
				break;
		if(pos == size) continue;
		for(uint i = col; i > pos; i--)
			tree_tags[i] = tree_tags[i-1];
		tree_tags[pos] = element;
	}
	uint mid_tree_tag = tree_tags[size-1];// < and >=
	
	// split index 
	Index *new_index = (Index*)index_allocator->allocate();
	new_index->meta = index->meta;
	
	for(uint raw = 0; raw < BUCKET_NUM; raw++) {
		uint add = raw == 0;
		Bucket_entry *ptr = index->bucket[raw].entry + add;
		Bucket_entry *pl = ptr, *pr = new_index->bucket[raw].entry + add;
		Bucket_entry *l_end = index->bucket[raw].entry + BUCKET_LEN;
		Bucket_entry *r_end = new_index->bucket[raw].entry + BUCKET_LEN;
		
		for(; ptr < l_end && ptr->type; ptr++) if(ptr->type==ENTRY_TYPE_INUSE){
	  		if(ptr->tree_tag < mid_tree_tag) 
			  	*pl++ = *ptr;
	  		else {
	  			__builtin_prefetch(ID2BLOCK(ptr->offset, ptr->group), 1, 0);
	  			*pr++ = *ptr;
			}
		}
		while(pl < l_end) (pl++)->type = ENTRY_TYPE_EMPTY;
		while(pr < r_end) (pr++)->type = ENTRY_TYPE_EMPTY;
	}
	
	// update block
	uint new_id = INDEX_ID(new_index);
	for(uint raw = 0; raw < BUCKET_NUM; raw++) {
		for(Bucket_entry * pr = new_index->bucket[raw].entry + (raw == 0); 
			pr < new_index->bucket[raw].entry + BUCKET_LEN && pr->type == ENTRY_TYPE_INUSE; pr++) {
			ID2BLOCK(pr->offset, pr->group) -> father = new_id;
		}
	}
	
	// insert to tree
	Node *father = ID2NODE(index->meta.father);
	tree_insert(father, mid_tree_tag, INDEX_ID(index), INDEX_ID(new_index), 1);
	//return make_pair(new_index, mid_tree_tag);
}
/*
int put_one(Index *index, Query *qy, ull full_tag) // -1 iff error
{
	uint raw = buckettag(full_tag);
	Bucket *bucket = index->bucket + raw;
	for(Bucket_entry *entry = bucket->entry; entry < bucket->entry + BUCKET_LEN; entry++) {
		if(entry->type == 0) {// after split, there is no tomb
			entry->offset = qy->block->offset;
			entry->type = 1;
			entry->group = qy->group;
			entry->tag = entrytag(full_tag);
			entry->tree_tag = treetag(full_tag);
			return 0;
		}
	}
	puts("put_one error");
	exit(-1);
	return -1;
}
*/
template<uint batch>
uint solve(uint unsolved, Query *q)
{
	static int sum = 0;
	static int cnt = 0;
	if((++cnt & ((1<<20)-1)) == 0) printf("sum = %d\n",sum);
	
	printf("step1\n");
	
	for(uint id = 0; id < unsolved; id++) 
		q[id].entry.node_entry = rt->entry;
	
	for(uint id = unsolved; id < batch; id++) {
		Query *qy = q + id;
		KV *kv = qy->q_kv;
		qy->entry.node_entry = rt->entry;
		ull tag = *(ull*)kv->content;
		qy->tree_tag = treetag(tag);
		qy->bucket_tag = buckettag(tag);
		qy->entry_tag = entrytag(tag);
		
		// allocate a block for PUT
		if(qy->type == REQ_PUT) {
			if(kv->len_value == (us)-1) {// DELETE
				qy->group = NO_GROUP;
				continue;
			}
			uint group = 0;
			uint kv_size = KV_SIZE(kv->len_key, kv->len_value);
			uint block_size = BLOCK_SIZE(kv->len_key, kv->len_value);
			for(uint jmp = JMP_START; jmp; jmp >>= 1) {
				uint nxt = group + jmp;
				if(GROUP_SIZE[nxt] < block_size)
					group = nxt;
			}
			assert(group < GROUP_NUM-1);
			group++;
			qy->group = group;// for PUT
			qy->new_block = (Block*)ray_allocator[group].allocate();
			// block->father = NULL
			memcpy(&qy->new_block->kv, kv, kv_size);
		}
	}
	// find index/bucket
	tree_search_table[batch](q);
	
	// delay those who have conflicts (keep time order)
	unsolved = 0;
	
	for(uint id = batch-1; id; id--) {
		Query *qy = q + id;
		for(uint pre = 0; pre < id; pre++) {
			Query *qp = q + pre;
			if(qp->entry.bucket_entry != qy->entry.bucket_entry) continue;
			//only check the first that match
			if(qp->type == REQ_GET && qy->type == REQ_GET) break;
			unsolved ++;
			Query *qs = q+batch-unsolved;
			swap(*qs, *qy);
			break;
		}
	}
	// divide PUT and GET
	uint n_put = 0;
	for(uint id = 0; id < batch-unsolved; id++) {
		uint put_id = n_put;
		if(q[id].type != REQ_PUT || id == put_id)
			continue;
		Query *qy = q + id, *qs = q + put_id;
		swap(*qs, *qy);
		n_put ++;
	}
	
	// complete all quests that can be solve in at most 1 match (of tag)
	bucket_search(n_put, batch-unsolved, q);
	
	// third try
	// solve all requests requiring spliting index
	uint n_extend = 0;
	
	for(uint id = 0; id < batch-unsolved; id ++) {
		Query *qy = q + id, *qs;
		if(NOT_COMPLETE(qy->type)) {
			//pair<Index*, uint>pr = 
			index_split(qy->index, qy->bucket_tag);// to be worked
			qs = q + n_extend++;
			swap(*qs, *qy);
			// a index may be splited more than once
		}
		else {
			sum ^= qy->type;
		}
	}
	for(uint id = 0; id < unsolved; id++) {
		Query *qy = q+batch-unsolved+id, *qs = q+n_extend+id;
		swap(*qy, *qs);
	}
	unsolved += n_extend;
	
	return unsolved;
}
uint (*solve_table[]) (uint, Query*) = {
	NULL, 	solve<1>, solve<2>, solve<3>, solve<4>, 
		solve<5>, solve<6>, solve<7>, solve<8>
};

int main()
{
	char global_kv_buf[MAX_BATCH*TEST_KV_SIZE];
	Query q[MAX_BATCH];


	init_allocators();
	
	init_tree();
	
	int shmid = shmget(SHMKEY, 4*K, 0666 | IPC_CREAT);
	if(shmid == -1) {
		puts("shm error");
		return 0;
	}
	volatile share_mem *sm = (share_mem *)shmat(shmid, SHMVA, 0);
	if((long long)sm == (long long)-1) {
		puts("shm error");
		return 0;
	}
	int signal = sm->signal;
	while(signal == sm->signal) usleep(1000);
	sm->ACK ++;
	
	int batch;
	printf("input value of \"batch\"\n");
	scanf("%d", &batch);
	int num = 0, old = 0;
	int window = sm->window;
	
	for(int id = 0; ; id = id+1==window ? 0 : id+1) {
		//如果要避免多个PUT在同一个BUCKET导致的各种情况，则可以要求每个batch中每个BUCKET至多一个PUT
		//进一步地，如果要防止同一个batch中的GET/PUT乱序，最好加强要求每个BUCKET中PUT和GET不共存
		//注意！treetag不同也可能会分到同一个BUCKET
		
		volatile KV *kv = sm->kv[id];
		if(kv->len_key == 0) continue;
	
		//fence
		Query *qy = q + num;
		qy->type = kv->len_value==0 ? REQ_GET : REQ_PUT;
		qy->q_kv = (KV*)(global_kv_buf + TEST_KV_SIZE*num);
		volatile_cpy(qy->q_kv, kv, TEST_KV_SIZE);
		
		//fence
		
		kv->len_key = 0;//
		num++;
		
		if(num == batch) {
			old = num = solve_table[num](old, q);// assert num of put <= 5
		}
	}
	return 0;
 } 
