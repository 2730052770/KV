#include "main.h"
#include "test.h"
using namespace std;

char global_kv_buf[MAX_BATCH*TEST_KV_SIZE];
Query q[MAX_BATCH];

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
		rt->entry[i].tag = -1;// maximum and invalid
		
	Index *t = (Index*)index_allocator->allocate();
	memset(t, 0, index_allocator->obj_size);
	t->meta.first_entry_type = BUCKET_TYPE_META;
	t->meta.father = 1;
	rt->entry[0].offset = 0;
	global_level = 1;
}

template<uint batch>
void tree_search
(const ull *tag, Node_entry **entry, Index **index)
{
	printf("step2\n");
	
	for(uint jmp = FORK_NUM>>1; jmp; jmp>>=1) {
		for(uint id = 0; id < batch; id++) {
			Node_entry * nxt = entry[id] + jmp;
			if(treetag(tag[id]) >= nxt->tag)
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
				if(treetag(tag[id]) >= nxt->tag)
					entry[id] = nxt;
			}
		}
	}
	
	printf("step4\n");
	
	for(uint id = 0; id < batch; id++) {
		Index * idx = (Index*)INDEX_BASE + entry[id]->offset;
		uint bid = buckettag(tag[id]);
		index[id] = idx;
		Bucket_entry * first = idx->bucket[bid].entry;
		entry[id] = (Node_entry*)(first - 1);//this is Bucket_entry* in fact
		__builtin_prefetch(first, 0, 0);
	}
}

template<uint batch>
uint first_bucket_search
(uint mid, uint status, const ull *tag, Bucket_entry **entry, Bucket_entry **tomb, Index **index)
{
	printf("step5\n");
	//你需要考虑：GET after PUT, PUT after same PUT, PUT after different PUT, PUT after DEL, DEL after PUT...
	//不如直接BUCKET互斥
	
	for(uint id = 0; id < mid; id++) { // PUT
		for(uint col = 0; col < BUCKET_LEN; col++) {
			Bucket_entry* be = ++entry[id];
			if(!tomb[id] && !(be->type&1)) {// TOMB OR EMPTY
				tomb[id] = be;
				be->type |= 1; // 0->1, 2->3 
			}
			if(be->type == ENTRY_TYPE_EMPTY) break;
			if(be->type == ENTRY_TYPE_INUSE && be->tag == entrytag(tag[id]) && be->tree_tag == treetag(tag[id])) {
				__builtin_prefetch(ID2BLOCK(be->offset, be->group), 0, 0);
				status |= 1<<(id*4);
				break;
			}
		}
	}
	for(uint id = mid; id < batch; id++) {
		//这里还可以尝试换换两个循环的顺序，需要一些技巧跳过已经查完的
		//这里我可以手动展开+跳转实现对变长的支持,先摆烂
		// switch 就行 
		for(uint col = 0; col < BUCKET_LEN; col++) {
			//这里可以让两个tag分别从两端向中间查 
			// we have decreased by 1 at tree_search 
			Bucket_entry* be = ++entry[id];
			if(be->type == ENTRY_TYPE_EMPTY) break; 
			if(be->type == ENTRY_TYPE_INUSE && be->tag == entrytag(tag[id]) && be->tree_tag == treetag(tag[id])) {
				__builtin_prefetch(ID2BLOCK(be->offset, be->group), 0, 0);
				status |= 1<<(id*4);
				break;
			}
		}
	}
	for(uint id = 0; id < mid; id++) {
		uint stts = status>>(id*4)&15;
		Query *qy = q + id;
		Block *block;
		KV *in_kv, *out_kv;
		switch(stts){
			case 0:// PUT not match / DELETE_LEY_NOT_EXISTS
				if(qy->group == -1) {
					qy->type = RESP_DELETE_KEY_NOT_EXISTS;
					status |= 4 << (id*4);
				}
				else {
					if(!tomb[id]) break;// resize outside
					entry[id] = tomb[id];
					
					qy->type = RESP_PUT_SUCCESS;
					entry[id]->type = 1;
					entry[id]->group = qy->group;
					entry[id]->tag = entrytag(tag[id]);
					entry[id]->tree_tag = treetag(tag[id]);
					entry[id]->offset = qy->block->offset;
					qy->block->father = INDEX_ID(index[id]);
					status |= 4 << (id*4);
				}
				break;
			case 1:// PUT/DELETE match
				block = ID2BLOCK(entry[id]->offset, entry[id]->group);
				in_kv = &block->kv;
				out_kv = qy->q_kv;
				if(in_kv->len_key == out_kv->len_key && 
				   memcmp(in_kv->content, out_kv->content, in_kv->len_key) == 0) {
					// if really match
				   	qy->type = RESP_PUT_SUCCESS;
				   	
				   	ray_allocator[entry[id]->group].free(block);
				   	
				   	if(qy->group == -1) {
				   		entry[id]->type = ENTRY_TYPE_TOMB;
					}
					else {
						entry[id]->group = qy->group;
					   	entry[id]->offset = qy->block->offset;
					   	qy->block->father = INDEX_ID(index[id]);
					}
					status |= 4 << (id*4);// complete
				}
				break;
		}
	}
	for(uint id = mid; id < batch; id++) {
		uint stts = (status>>(id*4)&7) & (entry[id]->type == ENTRY_TYPE_INUSE);// notice that "GET after DEL" may happen
		Query *qy = q + id;
		Block *block;
		KV *in_kv, *out_kv;
		switch(stts){
			case 0:// GET KEY NOT EXISTS
				qy->type = RESP_GET_KEY_NOT_EXISTS;
				status |= 4 << (id*4);// complete
				break;
			case 1:// GET match
				block = ID2BLOCK(entry[id]->offset, entry[id]->group);
				in_kv = &block->kv;
				out_kv = qy->q_kv;
				if(in_kv->len_key == out_kv->len_key && 
				   memcmp(in_kv->content, out_kv->content, in_kv->len_key) == 0) {
					// if really match
				   	qy->type = RESP_GET_SUCCESS;
					qy->block = block;
					status |= 4 << (id*4);// complete
				}
				break;
		}
	}
	return status;
}

template<uint batch>
uint second_bucket_search
(uint status, const ull *tag, Bucket_entry **entry, Index **index)
{
	printf("step5\n");
		
	Bucket_entry *tomb[batch] = {};
	for(uint id = 0; id < mid; id++) if((status>>id & 0b101) == 0b001){
										// not complete in last match
		Query *qy = q + id;
		for(uint col = 0; col < BUCKET_LEN; col++) { // only this line is different
			Bucket_entry* be = entry[id];
			if(!tomb[id] && !(be->type&1)) tomb[id] = be;
			if(be->type == 2) tomb[id] = be;
			if(be->type == 0) break; 
			if(be->type == 1 && be->tag == entrytag(tag[id]) && be->tree_tag == treetag(tag[id])) {
				status |= 1<<(id*4);
				
				int group = be->group;
				char * base = BASE_ADDR(group);
				Block * block = (Block *)(base + GROUP_SIZE[group] * be->offset);
				KV *in_kv = &block->kv;
				KV *out_kv = qy->q_kv;
				if(in_kv->len_key == out_kv->len_key && 
				   memcmp(in_kv->content, out_kv->content, in_kv->len_key) == 0) {
				   	// really match
				   	if(status>>id & 2) {// PUT
				   		qy->type = RESP_PUT_SUCCESS;
					   	ray_allocator[group].free(block);
					   	if(qy->group == -1) 
					   		entry[id]->type = 2;
						else {
							entry[id]->group = qy->group;
						   	entry[id]->offset = qy->block->offset;
						   	qy->block->father = INDEX_ID(index[id]);
						}
					}
					else {// GET
						qy->type = RESP_GET_SUCCESS;
						qy->block = block;
					}
					status |= 4<<(id*4);
					goto nxt;
				}
			}
		}
		status &= ~(1<<(id*4));
		if(status>>id & 2) {// put/delete key not exists
			if(qy->group == -1) {
				qy->type = RESP_DELETE_KEY_NOT_EXISTS;
			}
			else {
				if(col == BUCKET_LEN && !tomb[id]) break;// resize outside
				
				if(tomb[id] != NULL) entry[id] = tomb[id];
				qy->type = RESP_PUT_SUCCESS;
				entry[id]->type = 1;
				entry[id]->group = qy->group;
				entry[id]->tag = entrytag(tag[id]);
				entry[id]->tree_tag = treetag(tag[id]);
				entry[id]->offset = qy->block->offset;
				qy->block->father = INDEX_ID(index[id]);
			}
		}
		else {// get key not exists
			qy->type = RESP_GET_KEY_NOT_EXISTS;
		}
		status |= 4 << (id*4);
		nxt:
		;
	}
	return status;
}

void tree_insert(Node *ptr, uint tree_tag, uint offset, uint leaf)
{
	uint finding_bigger = 1, pos = FORK_NUM, cnt;
	for(cnt = 0; cnt < FORK_NUM; cnt++) {
		if(finding_bigger && ptr->entry[cnt].tag > tree_tag) {
			finding_bigger = 0;
			pos = cnt;
		}
		if(ptr->entry[cnt].tag == (uint)-1)
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
		if(pos <= half) {
			// new father -> son
			for(uint i = 0; i < half; i++) {
				new_ptr->entry[i] = ptr->entry[i + half];
				uint son_offset = new_ptr->entry[i].offset;
				fp[i] = leaf ? (void*)ID2INDEX(son_offset) : (void*)ID2NODE(son_offset);
				__builtin_prefetch(fp[i]);
			}
			for(uint i = half; i < FORK_NUM; i++)
				new_ptr->entry[i].tag = -1;
			// update old father
			for(uint i = FORK_NUM-1; i > half; i--)
				ptr->entry[i].tag = -1;
			for(uint i = half; i > pos; i--)
				ptr->entry[i] = ptr->entry[i-1];
			ptr->entry[pos].tag = tree_tag;
			ptr->entry[pos].offset = offset;
		}
		else{
			// new father -> son
			// 0 ~ half, half+1 ~ FORK_NUM-1
			for(uint i = 0; i < pos-(half+1); i++) {
				new_ptr->entry[i] = ptr->entry[i + half + 1];
				uint son_offset = new_ptr->entry[i].offset;
				fp[i] = leaf ? (void*)ID2INDEX(son_offset) : (void*)ID2NODE(son_offset);
				__builtin_prefetch(fp[i]);
			}
			new_ptr->entry[pos-(half+1)].tag = tree_tag;
			new_ptr->entry[pos-(half+1)].offset = offset;
			fp[pos-(half+1)] = leaf ? (void*)ID2INDEX(offset) : (void*)ID2NODE(offset);
			// no prefetch
			for(uint i = pos-half; i < half; i++) {
				new_ptr->entry[i] = ptr->entry[i + half];
				uint son_offset = new_ptr->entry[i].offset;
				fp[i] = leaf ? (void*)ID2INDEX(son_offset) : (void*)ID2NODE(son_offset);
				__builtin_prefetch(fp[i]);
			}
			// update old father
			for(uint i = half+1; i < FORK_NUM; i++)
				ptr->entry[i].tag = -1;
		}
		// son -> new father
		uint new_id = NODE_ID(new_ptr);
		for(uint i = 0; i < half; i++) 
			FATHER(fp[i]) = new_id;
		// new father -> grandfather
		uint mid_tag = new_ptr->entry[0].tag;// these two share the same memory
		if(ptr->father == 0) {
			global_level ++;
			rt = (Node*)node_allocator->allocate();
			ptr->father = new_ptr->father = NODE_ID(rt);
			
			rt->father = 0;
			rt->entry[0].offset = NODE_ID(ptr);
			rt->entry[1].tag = mid_tag;
			rt->entry[1].offset = new_id;
			for(uint i = 2; i < FORK_NUM; i++)
				rt->entry[i].tag = -1;
		}
		else{
			new_ptr->father = ptr->father;
			// grandfather -> new father
			tree_insert(ID2NODE(ptr->father), mid_tag, new_id, 0);	
		}
	}
}

pair<Index*, uint> index_split(Index *index, ull tag) {// tag is in the line which is full 

	// get mid_tag
	uint line = buckettag(tag) & (BUCKET_NUM - 1);
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
		
		for(; ptr < l_end && ptr->type; ptr++) if(ptr->type==1){
	  		if(ptr->tree_tag < mid_tree_tag) 
			  	*pl++ = *ptr;
	  		else {
	  			__builtin_prefetch(ID2BLOCK(ptr->offset, ptr->group), 1, 0);
	  			*pr++ = *ptr;
			}
		}
		while(pl < l_end) (pl++)->type = 0;
		while(pr < r_end) (pr++)->type = 0;
	}
	
	// update block
	uint new_id = INDEX_ID(new_index);
	for(uint raw = 0; raw < BUCKET_NUM; raw++) {
		for(Bucket_entry * pr = new_index->bucket[raw].entry + (raw == 0); 
			pr < new_index->bucket[raw].entry + BUCKET_LEN && pr->type; pr++) {
			ID2BLOCK(pr->offset, pr->group) -> father = new_id;
		}
	}
	
	// insert to tree
	Node *father = ID2NODE(index->meta.father);
	tree_insert(father, mid_tree_tag, INDEX_ID(new_index), 1);
	return make_pair(new_index, mid_tree_tag);
}

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

template<uint batch>
uint solve(uint old)
{
	static ll sum = 0;
	static int cnt = 0;
	if((++cnt & (1<<20)-1) == 0) printf("%d\n",cnt);
	
	
	Index * index[batch]; 
	void * entry[batch];
	ull tag[batch];
	Bucket_entry *tomb[batch] = {};
	uint status = 0;
	
	printf("step1\n");
	
	for(uint id = 0; id < batch; id++) {
		Query *qy = q + id;
		KV *kv = qy->q_kv;
		entry[id] = rt->entry;//////////////////////////////////////////////////
		tag[id] = *(ull*)kv->content;// content[0] == key[0]
		
		if(qy->type == REQ_PUT) {
			status |= 2 << (id*4);
			if(kv->len_value == 0) {// DELETE
				qy->group = -1;
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
			//if(qy->type == REQ_PUT) {
			qy->block = (Block*)ray_allocator[group].allocate();
			// block->father = NULL
			memcpy(&qy->block->kv, kv, kv_size);
			//}
		}
	}
	// first try
	tree_search<batch>(tag, (Node_entry**)entry, index);
	
	
	
	// complete all quests that can be solve in at most 1 match (of tag)
	status = first_bucket_search<batch>(mid, status, tag, (Bucket_entry**)entry, tomb, index);
	
	int not_complete = 0;
	
	for(uint id = 0; id < batch; id ++) {
		uint stts = status >> (id*4) & 7;
		if(likely(stts & 4)) {
			sum ^= q[id].type;// just a form of collect information
			q[id].type = 0;// inital state 
			continue;
		}
		not_complete = 1;
	}
	
	// in almost all cases, the program returns here
	if(likely(!not_complete)) return sum;
	
	
	
	
	// second try
	// complete all requests that can be solved without spliting index
	status = second_bucket_search<batch>(mid, status, tag, (Bucket_entry**)entry, tomb, index);
	
	not_complete = 0;
	
	for(uint id = 0; id < batch; id ++) {
		uint stts = status >> (id*4) & 7;
		if(likely(stts & 4)) {
			sum ^= q[id].type;
			q[id].type = 0;
			continue;
		}
		not_complete = 1;
	}
	
	if(likely(!not_complete)) return sum;
	
	// third try
	// solve all requests requiring spliting index
	for(uint id = 0; id < batch; id ++) if(q[id].type != 0) {
		pair<Index*, uint>pr = index_split(index[id], tag[id]);// to be worked
		for(uint id2 = id; id2 < batch; id2++) if(q[id2].type != 0 && index[id2] == index[id]) {
			ull tag_put = *(ull*)q[id2].q_kv->content;
			uint tree_tag = treetag(tag_put);
			if(tree_tag < pr.second)
				put_one(index[id], &q[id2], tag_put);
			else
				put_one(pr.first, &q[id2], tag_put);
		}
	}
	return sum;
}
ll (*solve_table[MAX_BATCH+1]) () = {
	NULL, 	solve<1>, solve<2>, solve<3>, solve<4>, 
			solve<5>, solve<6>, solve<7>, solve<8>
};

int main()
{
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
	
	int sum = 0;
	int batch;
	printf("input value of \"batch\"\n");
	scanf("%d", &batch);
	int num = 0;
	int window = sm->window;
	
	for(int i = 1, id = 0; ; i++) {
		//如果要避免多个PUT在同一个BUCKET导致的各种情况，则可以要求每个batch中每个BUCKET至多一个PUT
		//进一步地，如果要防止同一个batch中的GET/PUT乱序，最好加强要求每个BUCKET中PUT和GET不共存
		//注意！treetag不同也可能会分到同一个BUCKET
		int old =  num;
		
		while(num < batch) {
		
			while(sm->q[id].read_complete);
		
			volatile_cpy(q+num, &sm->q[id], sizeof(Query));
			qy.q_kv = (KV*)(global_kv_buf + TEST_KV_SIZE*num);
			volatile_cpy(q[num].q_kv, sm->q[id].q_kv, TEST_KV_SIZE);
			
			sm->q[id].read_complete = 1;//
			num++;
			if(++id == window) id = 0;
		}
		num = solve_table[num](old);// assert num of put <= 5
	}
	return 0;
 } 
