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
		rt->entry[i].tag = TREE_TAG_INF;// maximum and invalid
		
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
	
	
	for(uint lv = 1; lv < global_level; lv++) {// the nodes have $level different kind of hight
		for(uint id = 0; id < batch; id++) {
			Node *son = (Node*)NODE_BASE + entry[id]->offset;
			entry[id] = son->entry;// to be jump to level $lv
			__builtin_prefetch(entry[id], 0, 0);
		}
		
		for(uint jmp = FORK_NUM>>1; jmp; jmp>>=1) {//  now in level $lv, choose a son	// WE CAN SWAP THESE TWO AND PUT PREFETCH IN IT !!!
			for(uint id = 0; id < batch; id++) { 
				Node_entry * nxt = entry[id] + jmp;
				if(tag[id] >= nxt->tag)
					entry[id] = nxt;
			}
		}
	}
	
	/*
	for(uint lv = 1; lv < global_level; lv++) {// the nodes have $level different kind of hight
		for(uint jmp = FORK_NUM>>1; jmp; jmp>>=1) {//  now in level $lv, choose a son	// WE CAN SWAP THESE TWO AND PUT PREFETCH IN IT !!!
			for(uint id = 0; id < batch; id++) { 
				Node_entry * nxt = entry[id] + jmp;
				if(tag[id] >= nxt->tag)
					entry[id] = nxt;
			}
		}
		for(uint id = 0; id < batch; id++) { 
			Node *son = (Node*)NODE_BASE + entry[id]->offset;
			entry[id] = son->entry;// to be jump to level $lv
			__builtin_prefetch(entry[id], 0, 0);
		}
	}
	for(uint jmp = FORK_NUM>>1; jmp; jmp>>=1) {//  now in level $lv, choose a son	// WE CAN SWAP THESE TWO AND PUT PREFETCH IN IT !!!
		for(uint id = 0; id < batch; id++) { 
			Node_entry * nxt = entry[id] + jmp;
			if(tag[id] >= nxt->tag)
				entry[id] = nxt;
		}
	}
	*/
	for(uint id = 0; id < batch; id++) {
		Query *qy = q+id;
		Index * idx = (Index*)INDEX_BASE + entry[id]->offset;
		uint bid = qy->bucket_tag;
		qy->index = idx;
		Bucket_entry * first = idx->bucket[bid].entry;
		qy->entry.bucket_entry = first;//this is Bucket_entry* in fact
		qy->first_entry_id = 0;
		__builtin_prefetch(first, 1, 0);
	}
}
void (*tree_search_table[]) (Query *) = {
	tree_search<0>, tree_search<1>, tree_search<2>, tree_search<3>, tree_search<4>,
	tree_search<5>, tree_search<6>, tree_search<7>, tree_search<8>
};

template<uint batch>
void first_bucket_search(Query *q)//							THE TEMPLATE MAY NOT BE USEFUL
{
	//这里还可以尝试换换两个循环的顺序，需要一些技巧跳过已经查完的
	//这里我可以手动展开+跳转实现对变长的支持,先摆烂
	// switch 就行 
	//这里可以让两个tag分别从两端向中间查 

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
		if(col==BUCKET_LEN && qy->req_type == REQ_PUT) {
			qy->first_entry_type = FIND_FULL_FOR_PUT;// do we need pre-extend?
		}
		else if(col < BUCKET_LEN && entry->type == ENTRY_TYPE_INUSE) {
			qy->first_entry_type = FIND_MATCH;
			old_block = ID2BLOCK(entry->offset, entry->group);
			qy->old_block = old_block;
			__builtin_prefetch(old_block, 0, 0);
		}
		//else 
		else qy->first_entry_type = FIND_NULL;
		qy->first_entry_id = col;
		qy->entry.bucket_entry = entry;
	}
	for(uint id = 0; id < batch; id++) {
		qy = q + id;
		entry = qy->entry.bucket_entry;
		switch(qy->first_entry_type){
			case FIND_NULL:// PUT not match / DELETE_LEY_NOT_EXISTS
				qy->resp_type = RESP_NO_KEY;
				
				if(qy->req_type == REQ_PUT) {
					entry->type = ENTRY_TYPE_INUSE;
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
				   
				   	qy->resp_type = RESP_HAS_KEY;
				   	
				   	if(qy->req_type != REQ_GET) {
				   	
					   	ray_allocator[entry->group].free(old_block);
					   	
					   	if(qy->req_type == REQ_DELETE) {// delete
					   		entry->type = ENTRY_TYPE_EMPTY;
					   		uint col = qy->first_entry_id + 1;
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
				}
				break;
		}
	}
}
void (*first_bucket_search_table[]) (Query*) = {
	first_bucket_search<0>, first_bucket_search<1>, first_bucket_search<2>, first_bucket_search<3>,first_bucket_search<4>,
	first_bucket_search<5>, first_bucket_search<6>, first_bucket_search<7>, first_bucket_search<8>,
};

void second_bucket_search(uint n_op, Query *q)
{
	Query *qy;
	Block *old_block;
	KV *old_kv, *new_kv;
	Bucket_entry *entry;
	
	for(uint id = 0; id < n_op; id++) if(q[id].resp_type == RESP_EMPTY && q[id].first_entry_type == FIND_MATCH){ // PUT
	
		//puts("an KV enter second_search");
	
		qy = q+id;
		entry = qy->entry.bucket_entry + 1;
		uint tree_tag = qy->tree_tag;
		us entry_tag = qy->entry_tag;
		/*
		if(tree_tag == 1936946035) {
			printf("find!!!, %d\n", Cnt);
		}
		*/
		uint col = qy->first_entry_id + 1;
		for(; col < BUCKET_LEN; col++, entry++) {
			if(entry->type == ENTRY_TYPE_EMPTY) {

				qy->resp_type = RESP_NO_KEY;

				if(qy->req_type == REQ_PUT){
					entry->type = ENTRY_TYPE_INUSE;
					entry->group = qy->group;
					entry->tag = qy->entry_tag;
					entry->tree_tag = qy->tree_tag;
					entry->offset = qy->new_block->offset;
					qy->new_block->father = INDEX_ID(qy->index);
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
					
				qy->resp_type = RESP_HAS_KEY;	
				qy->old_block = old_block;
				
				if(qy->req_type != REQ_GET) {
				   	
				   	ray_allocator[entry->group].free(old_block);
				   	
				   	if(qy->req_type == REQ_DELETE) {// delete
				   		entry->type = ENTRY_TYPE_EMPTY;
				   		uint ncol = col + 1;
				   		Bucket_entry *last = entry + 1;
				   		for(; ncol < BUCKET_LEN; ncol++, last++)
				   			if(last->type == ENTRY_TYPE_EMPTY)
				   				break;
				   		last--;
				   		swap(*last, *entry);
					}
					else {// put
						entry->group = qy->group;
					   	entry->offset = qy->new_block->offset;
					   	qy->new_block->father = INDEX_ID(qy->index);
					}
				}
				break;
			}
		}
		if(col == BUCKET_LEN && qy->req_type != REQ_PUT) 
			qy->resp_type = RESP_NO_KEY;
	}
}

void bucket_search(uint n_op, Query *q)
{
	//你需要考虑：GET after PUT, PUT after same PUT, PUT after different PUT, PUT after DEL, DEL after PUT...
	//不如直接BUCKET互斥
	first_bucket_search_table[n_op](q);
	second_bucket_search(n_op, q);
}

void tree_insert(Node *ptr, uint tree_tag, uint old_offset, uint offset, uint leaf)
{
	uint pos, cnt;
	for(pos = cnt = 1; cnt < FORK_NUM; cnt++) {
		if(ptr->entry[cnt].tag == TREE_TAG_INF) // these 2 "if" can not change order
			break; 
		if(ptr->entry[cnt].offset == old_offset) 
			pos = cnt + 1;			// if pos has not been assigned here, then pos = 1;
	}
	if(cnt < FORK_NUM) {
		//puts("normal");
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
			ptr->entry[i].tag = TREE_TAG_INF;
		for(uint i = half; i < FORK_NUM; i++)
			new_ptr->entry[i].tag = TREE_TAG_INF;
		
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
				rt->entry[i].tag = TREE_TAG_INF;
		}
		else{
			new_ptr->father = ptr->father;
			// grandfather -> new father
			tree_insert(ID2NODE(ptr->father), mid_tag, old_id, new_id, 0);	
		}
	}
}


volatile share_mem *sm;



pair<Index*, uint> index_split(const Query *qy) {// tag is in the line which is full 
#ifdef DEBUG
	sm->n_split ++;
#endif
	
	//puts("split");
	// get mid_tag
	Index *index = qy->index;
	uint line = qy->bucket_tag;
	Bucket_entry *entrys = index->bucket[line].entry;

	if(entrys[BUCKET_LEN-1].type == ENTRY_TYPE_EMPTY) return make_pair((Index*)NULL, (uint)0); // not full
	
	const uint size = BUCKET_LEN/2;
	uint min_tag = TREE_TAG_INF, tree_tags[size];
	for(uint col = 0; col < size; col++) {// insertion sort
		uint element = entrys[col].tree_tag;
		if(element < min_tag) min_tag = element;
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
		if(element < min_tag) min_tag = element;
		uint pos = 0;
		for(; pos < size; pos++)
			if(element > tree_tags[pos])
				break;
		if(pos == size) continue;
		for(uint i = size-1; i > pos; i--)
			tree_tags[i] = tree_tags[i-1];
		tree_tags[pos] = element;
	}
	assert(min_tag != tree_tags[0]/* == max_tag */);
	// assert the number of elements that share the same tree_tag and bucket_tag is never greater than BUCKET_LEN
	
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
		
		for(; ptr < l_end && ptr->type && ptr->type==ENTRY_TYPE_INUSE; ptr++){
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
	return make_pair(new_index, mid_tree_tag);
}

void put_one(Query *qy)
{
	//put the new KV in bucket
	Bucket_entry *be = qy->index->bucket[qy->bucket_tag].entry, *end = be + BUCKET_LEN;
	for( ; be < end; be++) if(be->type == ENTRY_TYPE_EMPTY) break;
	assert(be != end);
	
	qy->resp_type = RESP_NO_KEY;
	be->offset = qy->new_block->offset;
	be->type = ENTRY_TYPE_INUSE;
	be->group = qy->group;
	be->tag = qy->entry_tag;
	be->tree_tag = qy->tree_tag;
	qy->new_block->father = INDEX_ID(qy->index);
}

template<uint batch>
uint solve(uint unsolved, Query *q)
{
#ifdef DEBUG
	static int cnt = 0;
	if((++cnt & ((1<<20)-1)) != 0) goto end;
	printf("cnt = %d, level = %d\n", cnt, global_level);
	printf("node: %d %d\n", node_allocator->num_inuse(), node_allocator->page_num);
	printf("index: %d %d\n", index_allocator->num_inuse(), index_allocator->page_num);
	printf("ray: ");
	for(uint i = 0; i < GROUP_NUM; i++) 
	  printf("(OBJSZ=%d, OBJNUM=%d, PAGENUM=%d)%c", GROUP_SIZE[i], ray_allocator[i].num_inuse(), ray_allocator[i].page_num, " \n"[i==GROUP_NUM-1]);
	end:
#endif
	
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
		if(qy->req_type == REQ_PUT) {
			int group = -1;// this should be int
			uint kv_size = KV_SIZE(kv->len_key, kv->len_value);
			uint block_size = BLOCK_SIZE(kv->len_key, kv->len_value);
			for(int jmp = JMP_START; jmp; jmp >>= 1) {
				int nxt = group + jmp;
				if(GROUP_SIZE[nxt] < block_size)
					group = nxt;
			}
			group++;
			qy->group = group;// for PUT
			qy->new_block = (Block*)ray_allocator[group].allocate();
			
			if(qy->new_block != ID2BLOCK(qy->new_block->offset, qy->group)) {
				puts("addr not match");
			}
			
			memcpy(&qy->new_block->kv, kv, kv_size);
		}
	}
	// find index/bucket
	tree_search<batch>(q);
	
	// delay those who have conflicts (keep time order)
	unsolved = 0;
	
	for(uint id = 0; id<batch; id++) {
		Query *qy = q + id;
		//printf("repeat: %llu\n", *(ull*)qy->q_kv->content);
		for(uint pre = unsolved; pre < id; pre++) {//				HERE CAN ADD A HASH, IF FIND SAME, THEN LOOP, OTHERWISE NOT LOOP
			Query *qp = q + pre;
			if(qp->entry.bucket_entry != qy->entry.bucket_entry) continue;
			//only check the first that match
			//if(qp->req_type == REQ_GET && qy->req_type == REQ_GET) break;// THIS WILL BREAK THE ORDER
			swap(q[unsolved++], *qy);
			break;
		}
	}

	// complete all quests that can be solved without extension
	bucket_search(batch-unsolved, q+unsolved);
	
	for(uint id = unsolved; id < batch; id ++) {
		Query *qy = q + id, *qn;
		if(qy->resp_type != RESP_EMPTY) continue;
		pair<Index*, uint>p = index_split(qy);// to be worked
		if(p.first != NULL) {
			Index* old_index = qy->index;
			for(uint nxt = id; nxt < batch; nxt++) {
				qn = q + nxt;
				if(qn->resp_type != RESP_EMPTY || qn->index != old_index || qn->tree_tag < p.second) 
					continue;
				qn->index = p.first;
			}
		}
		put_one(qy);
	}
	
	return unsolved;
}
uint (*solve_table[]) (uint, Query*) = {
	NULL, 	solve<1>, solve<2>, solve<3>, solve<4>, 
		solve<5>, solve<6>, solve<7>, solve<8>
};

int main(int argc, char **argv)
{
	int batch;
	if(argc != 2) {
		printf("usage: \"./main BATCH_SIZE\"\n");
		return 0;
	}
	batch = atoi(argv[1]);

	static char global_kv_buf[MAX_BATCH*MAX_TEST_KV_SIZE];
	Query q[MAX_BATCH];
	memset(q, 0, sizeof(q));

	init_allocators();
	
	init_tree();
	
	int shmid = shmget(SHMKEY, M, 0666 | IPC_CREAT);
	if(shmid == -1) {
		puts("shm error");
		return 0;
	}
	sm = (share_mem *)shmat(shmid, SHMVA, 0);
	if((long long)sm == (long long)-1) {
		puts("shm error");
		return 0;
	}
	int signal = sm->START;
	while(signal == sm->START) usleep(1000);
	sm->S_ACK ++;
	signal = sm->END;
	
	int num = 0, old = 0;
	int window = sm->window;
	
	int emptyloop = 0, validloop = 0;
	for(int id = 0; ; id = id+1==window ? 0 : id+1) {
		//如果要避免多个PUT在同一个BUCKET导致的各种情况，则可以要求每个batch中每个BUCKET至多一个PUT
		//进一步地，如果要防止同一个batch中的GET/PUT乱序，最好加强要求每个BUCKET中PUT和GET不共存
		//注意！treetag不同也可能会分到同一个BUCKET
		
		
		
		volatile TEST_Q *tq = sm->tq[id];
		if(tq->resp_type != RESP_EMPTY && signal == sm->END) {
			emptyloop++;
			continue;
		}
		if(signal != sm->END) break;
		
		validloop++;
		if((validloop & 0xfffff) == 0) {
			printf("%d %d\n", emptyloop, validloop);
		}
		
		//__sync_synchronize();
		//fence
		
		
		Query *qy = q + num;
		qy->tq = tq;
		qy->req_type = tq->req_type;
		qy->resp_type = RESP_EMPTY;
		if(!qy->q_kv) qy->q_kv = (KV*)(global_kv_buf + MAX_TEST_KV_SIZE*num);
		
		volatile KV *kv = &tq->kv;
		
		volatile_cpy(qy->q_kv, kv, KV_SIZE(kv->len_key, kv->len_value));
		
		tq->resp_type = RESP_READ;
		
		//printf("%s %llu\n", qy->req_type == REQ_GET ? "GET" : "PUT", *(ull*)qy->q_kv->content);
		
		//__sync_synchronize();
		//fence
		num++;
		
		if(num == batch) {
			old = num = solve_table[batch](old, q);// assert num of put <= 5
			for(int i = old; i < batch; i++) {
				if(q[i].req_type == REQ_GET && q[i].resp_type == RESP_HAS_KEY) {
					volatile_cpy(&q[i].tq->kv, &q[i].old_block->kv, KV_SIZE(q[i].old_block->kv.len_key, q[i].old_block->kv.len_value));
				}
				q[i].tq->resp_type = q[i].resp_type;
				//usleep(1);
			}
		}
	}
	sm->E_ACK++;
	return 0;
 } 
