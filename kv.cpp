#include "global.h"
#include "allocator.h"
#include "kv.h"
#include "test.h"
using namespace std;

KVS::KVS(ull (*_hash) (char *, char *)) {
	hash = _hash;
	init_allocator();
	init_structure();
}

KVS::KVS(const KVS &_kvs) {
	assert(false);
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
	new(&block_allocator) Allocator();
	new(&node_allocator) Small_allocator(sizeof(Node), PAGE_SIZE, sizeof(Node));
	new(&table_allocator) Small_allocator(sizeof(Table), PAGE_SIZE, sizeof(Table));
}

void KVS::init_structure() {
	rt = (Node*)node_allocator.allocate();
	setpureptr(&rt->father, 0);//
	for(uint i = 1; i < NODE_FORK_NUM; i++)
		rt->entry[i].tag = TREE_TAG_INF;
		
	Table *t = (Table*)table_allocator.allocate();
	memset(t, 0, sizeof(Table));
	setpureptr(&t->father, rt);
	rt->entry[0].tag = 0;
	setptr(&rt->entry[0].ptr, t);
	global_level = 1;
}

void KVS::first_bucket_search(uint batch, Query *q)
{
	Query *qy;
	Block *old_block;
	KV *old_kv, *new_kv;
	Bucket_entry *entry;
	
	for(uint id = 0; id < batch; id++) {
		qy = q+id;
		entry = qy->entry.bucket_entry;
		uint tree_tag = qy->tree_tag;
		us entry_tag = qy->entry_tag;
		uint col;
		for(col = 0; col < BUCKET_LEN; col++, entry++) {
			if(entry->type == BUCKET_ENTRY_TYPE_EMPTY || 
			   (entry->type == BUCKET_ENTRY_TYPE_INUSE && (us)entry->tag == entry_tag && entry->tree_tag == tree_tag)) 
			   break;
		}
		if(col==BUCKET_LEN && qy->req_type == REQ_PUT) {
			qy->first_entry_type = FIND_FULL_FOR_PUT;
		}
		else if(col < BUCKET_LEN && entry->type == BUCKET_ENTRY_TYPE_INUSE) {
			qy->first_entry_type = FIND_MATCH;
			old_block = (Block*)readptr(entry);
			qy->old_block = old_block;
			__builtin_prefetch(old_block, 0, 0);
		}
		else qy->first_entry_type = FIND_NULL;
		qy->first_entry_id = col;
		qy->entry.bucket_entry = entry;
	}
	for(uint id = 0; id < batch; id++) {
		qy = q + id;
		entry = qy->entry.bucket_entry;
		switch(qy->first_entry_type){
			case FIND_NULL:
				qy->resp_type = RESP_NO_KEY;
				
				if(qy->req_type == REQ_PUT) {
					*(ull*)entry = BUCKET_ENTRY_TYPE_INUSE | (ull)qy->new_block | ((ull)qy->entry_tag << 48);
					entry->tree_tag = qy->tree_tag;
					setpureptr(&qy->new_block->father, qy->table);
				}
				break;

			case FIND_MATCH:
				old_block = qy->old_block;
				old_kv = &old_block->kv;
				new_kv = qy->q_kv;
				if(old_kv->len_key == new_kv->len_key && 
				   memcmp(old_kv->content, new_kv->content, old_kv->len_key) == 0) {
				   
				   	qy->resp_type = RESP_HAS_KEY;
				   	
				   	if(qy->req_type != REQ_GET) {
				   	
					   	block_allocator.free(old_block);
					   	
					   	if(qy->req_type == REQ_DELETE) {
					   		entry->type = BUCKET_ENTRY_TYPE_EMPTY;
					   		uint col = qy->first_entry_id + 1;
					   		Bucket_entry *last = qy->entry.bucket_entry + 1;
					   		for(; col < BUCKET_LEN; col++, last++)
					   			if(last->type == BUCKET_ENTRY_TYPE_EMPTY)
					   				break;
					   		last--;
					   		swap(*last, *entry);
						}
						else {
						   	setptr(entry, qy->new_block);
						   	setpureptr(&qy->new_block->father, qy->table);
						}
					}
				}
				break;
		}
	}
}

void KVS::second_bucket_search(uint batch, Query *q)
{
	Query *qy;
	Block *old_block;
	KV *old_kv, *new_kv;
	Bucket_entry *entry;
	
	for(uint id = 0; id < batch; id++) if(q[id].resp_type == RESP_EMPTY && q[id].first_entry_type == FIND_MATCH){
	
		qy = q+id;
		entry = qy->entry.bucket_entry + 1;
		uint tree_tag = qy->tree_tag;
		us entry_tag = qy->entry_tag;
		
		uint col = qy->first_entry_id + 1;
		for(; col < BUCKET_LEN; col++, entry++) {
			if(entry->type == BUCKET_ENTRY_TYPE_EMPTY) {

				qy->resp_type = RESP_NO_KEY;

				if(qy->req_type == REQ_PUT){
					*(ull*)entry = BUCKET_ENTRY_TYPE_INUSE | (ull)qy->new_block | ((ull)qy->entry_tag << 48);
					entry->tree_tag = qy->tree_tag;
					setpureptr(&qy->new_block->father, qy->table);
				}
				break;
			}
			else if(entry->type == BUCKET_ENTRY_TYPE_INUSE && entry->tag == entry_tag && entry->tree_tag == tree_tag) { 
				old_block = (Block*)readptr(entry);
				old_kv = &old_block->kv;
				new_kv = qy->q_kv;
				if(old_kv->len_key != new_kv->len_key || 
				   memcmp(old_kv->content, new_kv->content, old_kv->len_key) != 0)
					continue;
					
				qy->resp_type = RESP_HAS_KEY;	
				qy->old_block = old_block;
				
				if(qy->req_type != REQ_GET) {
				   	
				   	block_allocator.free(old_block);
				   	
				   	if(qy->req_type == REQ_DELETE) {
				   		entry->type = BUCKET_ENTRY_TYPE_EMPTY;
				   		uint ncol = col + 1;
				   		Bucket_entry *last = entry + 1;
				   		for(; ncol < BUCKET_LEN; ncol++, last++)
				   			if(last->type == BUCKET_ENTRY_TYPE_EMPTY)
				   				break;
				   		last--;
				   		swap(*last, *entry);
					}
					else {
						setptr(entry, qy->new_block);
					   	setpureptr(&qy->new_block->father, qy->table);
					}
				}
				break;
			}
		}
		if(col == BUCKET_LEN && qy->req_type != REQ_PUT) 
			qy->resp_type = RESP_NO_KEY;
	}
}

void KVS::tree_insert(Node *node, uint tree_tag, void *old_son, void *new_son) 
{
	uint pos, cnt;
	for(pos = cnt = 1; cnt < NODE_FORK_NUM; cnt++) {
		if(node->entry[cnt].tag == TREE_TAG_INF) // these 2 "if" can not change order
			break; 
		if(readptr(&node->entry[cnt].ptr) == old_son) 
			pos = cnt + 1;			// if pos has not been assigned here, then pos = 1;
	}
	
	setpureptr(new_son, node);
	
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
		void* fp[r_cnt];
		// pos will never be 0
		if(pos <= l_cnt) {
			for(uint i = 0; i < r_cnt; i++) {
				new_node->entry[i] = node->entry[i + l_cnt];
				fp[i] = readptr(&new_node->entry[i].ptr);
				__builtin_prefetch(fp[i], 1, 0);
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
				fp[i] = readptr(&new_node->entry[i].ptr);
				__builtin_prefetch(fp[i], 1, 0);
			}
			new_node->entry[pos-(l_cnt+1)].tag = tree_tag;
			setptr(&new_node->entry[pos-(l_cnt+1)].ptr, new_son);
			fp[pos-(l_cnt+1)] = new_son;
			// no prefetch
			for(uint i = pos-l_cnt; i < r_cnt; i++) {
				new_node->entry[i] = node->entry[i + l_cnt];
				fp[i] = readptr(&new_node->entry[i].ptr);
				__builtin_prefetch(fp[i], 1, 0);
			}
		}
		for(uint i = l_cnt+1; i < NODE_FORK_NUM; i++)
			node->entry[i].tag = TREE_TAG_INF;// can change -1 to ...
		for(uint i = r_cnt; i < NODE_FORK_NUM; i++)
			new_node->entry[i].tag = TREE_TAG_INF;
		
		// son -> new father
		for(uint i = 0; i < r_cnt; i++) 
			setpureptr(fp[i], new_node);
		// new father -> grandfather
		uint mid_tag = new_node->entry[0].tag;
		if(readptr(&node->father) == NULL) {
			global_level ++;
			rt = (Node*)node_allocator.allocate();
			setpureptr(&node->father, rt);
			setpureptr(&new_node->father, rt);
			
			setpureptr(&rt->father, NULL);
			rt->entry[0].tag = 0;
			setptr(&rt->entry[0].ptr, node);
			rt->entry[1].tag = mid_tag;
			setptr(&rt->entry[1].ptr, new_node);
			for(uint i = 2; i < NODE_FORK_NUM; i++)
				rt->entry[i].tag = TREE_TAG_INF;
		}
		else{
			new_node->father = node->father;
			// grandfather -> new father
			tree_insert((Node*)readptr(&node->father), mid_tag, node, new_node);	
		}
	}
}

pair<Table*, uint> KVS::table_split(Query *qy) {// tag is in the line which is full 
#ifdef DEBUG
	sm->n_split ++;
#endif
	// get mid_tag
	Table *table = qy->table;
	Bucket_entry *entrys = table->bucket[qy->bucket_tag].entry;

	if(entrys[BUCKET_LEN-1].type == BUCKET_ENTRY_TYPE_EMPTY) return make_pair((Table*)NULL, (uint)0); // not full
	
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
	if(unlikely(min_tag == tree_tags[0])) {
		assert(("need construct a inner structure", false));
	}
	
	uint mid_tree_tag = tree_tags[size-1];// < and >=
	
	// split table 
	Table *new_table = (Table*)table_allocator.allocate();
	new_table->father = table->father;
	
	for(uint raw = 0; raw < BUCKET_NUM; raw++) {
		Bucket_entry *ptr = table->bucket[raw].entry;
		Bucket_entry *pl = ptr, *pr = new_table->bucket[raw].entry;
		Bucket_entry *l_end = pl + BUCKET_LEN, *r_end = pr + BUCKET_LEN;
		
		for(; ptr < l_end && ptr->type; ptr++){
	  		if(ptr->tree_tag < mid_tree_tag) 
			  	*pl++ = *ptr;
	  		else {
	  			__builtin_prefetch(readptr(ptr), 1, 0);
	  			*pr++ = *ptr;
			}
		}
		while(pl < l_end) (pl++)->type = BUCKET_ENTRY_TYPE_EMPTY;
		while(pr < r_end) (pr++)->type = BUCKET_ENTRY_TYPE_EMPTY;
	}
	
	// update block
	for(uint raw = 0; raw < BUCKET_NUM; raw++) {
		for(Bucket_entry * pr = new_table->bucket[raw].entry; 
			pr < new_table->bucket[raw].entry + BUCKET_LEN && pr->type == BUCKET_ENTRY_TYPE_INUSE; pr++) {
			setpureptr(readptr(pr), new_table);
		}
	}
	
	// insert to tree
	tree_insert((Node*)readptr(&table->father), mid_tree_tag, table, new_table);
	return make_pair(new_table, mid_tree_tag);
}

void KVS::put_one(Query *qy)
{
	//put the new KV in bucket
	Bucket_entry *be = qy->table->bucket[qy->bucket_tag].entry, *end = be + BUCKET_LEN;
	for( ; be < end; be++) if(be->type == BUCKET_ENTRY_TYPE_EMPTY) break;
	assert(be != end);
	
	qy->resp_type = RESP_NO_KEY;
	*(ull*)be = BUCKET_ENTRY_TYPE_INUSE | (ull)qy->new_block | ((ull)qy->entry_tag << 48);
	be->tree_tag = qy->tree_tag;
	setpureptr(&qy->new_block->father, qy->table);
}

