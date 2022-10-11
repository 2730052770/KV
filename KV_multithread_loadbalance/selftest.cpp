#include <pthread.h>
#include "kv.cpp"


key_type key_mask;

ull get_time_ns()
{
	struct timespec tm;
	clock_gettime(CLOCK_MONOTONIC_RAW, &tm);
	return tm.tv_sec * 1000000000ull + tm.tv_nsec;
}


void check_rand()
{
	key_type k;
	for(int i = 0; i < 100; i++) {
		k = rand();
		if(k > (~(key_type)0)/4) return;
	}
	printf("rand() range error\n");
	exit(0);
}

void init_key_range_mask(key_type mask)
{
	key_mask = mask;
}

inline key_type key_rand()
{
	return rand() & key_mask;
}

const uint checkmask = 0x7ffff;
uint batch;

struct workpos{
	key_type *arr;
	uint size;
};

void* myrun_put(void *workpos_ptr)
{
	key_type *req_key = ((workpos*)workpos_ptr)->arr;
	uint nput = ((workpos*)workpos_ptr)->size;

	char q_kv_buf[MAX_BATCH][KV_SIZE(test_len_val)];
	Query q_array[MAX_BATCH];
	memset(q_array, 0, sizeof(q_array));
	for(uint i = 0; i < batch; i++) {
		q_array[i].resp_kv = (KV*)q_kv_buf[i];
		q_array[i].inter_state = INT_STATE_FIN;
		q_array[i].req_type = 0;
	}

	thread_metadata tm;

	uint iter_time = 0, pre_iter_time = 0;
	uint pre_req_id;
	ull tp, tn;
	ull t_start, t_end;

	printf("PUT\n");
	t_start = get_time_ns();

	pre_req_id = 0;
	tp = get_time_ns();
	for(uint q_id = 0, req_id = 0; req_id < nput; q_id = (q_id+1==batch) ? 0 : q_id+1) {
		//if((req_id & 1023) == 1023) {
		//	printf("%d\n", req_id);
		//}
		iter_time++;


		Query &q = q_array[q_id];
		if(q.inter_state == INT_STATE_INIT) {
			iterate_init(q, tm);
		}
		else if(q.inter_state == INT_STATE_NODE) {
			iterate_node(q, tm);
		}
		else if(q.inter_state == INT_STATE_FIN) {
			q.req_type = REQ_PUT;
			q.resp_type = 0;
			q.inter_state = INT_STATE_INIT;
			q.key = req_key[req_id];
			q.put_node = (KV_Node*)tm.kv_node_allocator.allocate(KV_NODE_SIZE(test_len_val));
			q.put_node->init();
			q.put_node->kv.key = q.key;
			q.put_node->kv.len_val = test_len_val;
			*(key_type*)q.put_node->kv.val = q.key;
			q.put_node->modification_version ++;// make it static 
			iterate_init(q, tm);
			req_id ++;
		}
		if((req_id & checkmask) == checkmask && req_id != pre_req_id) {
			tn = get_time_ns();
			ull dt = tn - tp;
			tp = tn;
			uint delta_req_id = req_id - pre_req_id;
			pre_req_id = req_id;
			double tpt = 1000.0 * delta_req_id / dt;
			printf("%.2lf MOPS\n", tpt);

			uint d_iter_time = iter_time - pre_iter_time;
			pre_iter_time = iter_time;
			//printf("%.2lf\n", 1.0 * d_iter_time / delta_req_id);
		}
	}
	t_end = get_time_ns();
	printf("TOTAL %.2lf Mop/s\n", 1e3*nput/(t_end - t_start));
	
	return NULL;
}

void* myrun_get(void* workpos_ptr)
{
	key_type *req_key = ((workpos*)workpos_ptr)->arr;
	uint nput = ((workpos*)workpos_ptr)->size;

	char q_kv_buf[MAX_BATCH][KV_SIZE(test_len_val)];
	Query q_array[MAX_BATCH];
	memset(q_array, 0, sizeof(q_array));
	for(uint i = 0; i < batch; i++) {
		q_array[i].resp_kv = (KV*)q_kv_buf[i];
		q_array[i].inter_state = INT_STATE_FIN;
		q_array[i].req_type = 0;
	}

	thread_metadata tm;

	uint iter_time = 0, pre_iter_time = 0;
	uint pre_req_id;
	ull tp, tn;
	ull t_start, t_end;

	printf("GET\n");
	t_start = get_time_ns();


	pre_req_id = 0;
	tp = get_time_ns();
	for(uint q_id = 0, req_id = 0; req_id < nput; q_id = (q_id+1==batch) ? 0 : q_id+1) {

		iter_time++;

		Query &q = q_array[q_id];
		if(q.inter_state == INT_STATE_INIT) {
			iterate_init(q, tm);
		}
		else if(q.inter_state == INT_STATE_NODE) {
			iterate_node(q, tm);
		}
		else if(q.inter_state == INT_STATE_FIN) {
			/*if(q.req_type == REQ_GET) {//check
				assert(q.resp_type == RESP_NO_KEY || *(key_type*)q.q_kv->val->val == q.q_kv->key);
			} */
			if(q.req_type == REQ_GET && 
				(q.resp_type == RESP_HAS_KEY && q.resp_kv->key != *(key_type*)q.resp_kv->val)) {
				printf("GET FAILED\n");
				exit(1);
			}
			//printf("%d %d\n", q.resp_kv->key, *(key_type*)q.resp_kv->val);
			q.req_type = REQ_GET;
			q.resp_type = 0;
			q.inter_state = INT_STATE_INIT;
			q.key = req_key[req_id];
			q.put_node = NULL;

			iterate_init(q, tm);
			req_id ++;
		}
		if((req_id & checkmask) == checkmask && req_id != pre_req_id) {
			tn = get_time_ns();
			ull dt = tn - tp;
			tp = tn;
			uint delta_req_id = req_id - pre_req_id;
			pre_req_id = req_id;
			double tpt = 1000.0 * delta_req_id / dt;
			printf("%.2lf MOPS\n", tpt);

			uint d_iter_time = iter_time - pre_iter_time;
			pre_iter_time = iter_time;
			//printf("%.2lf\n", 1.0 * d_iter_time / delta_req_id);
		}
	}
	t_end = get_time_ns();
	printf("TOTAL %.2lf Mop/s\n", 1e3*nput/(t_end - t_start));
	return NULL;
}

void* myrun_mix(void *workpos_ptr)
{
	key_type *req_key = ((workpos*)workpos_ptr)->arr;
	uint nput = ((workpos*)workpos_ptr)->size;

	char q_kv_buf[MAX_BATCH][KV_SIZE(test_len_val)];
	Query q_array[MAX_BATCH];
	memset(q_array, 0, sizeof(q_array));
	for(uint i = 0; i < batch; i++) {
		q_array[i].resp_kv = (KV*)q_kv_buf[i];
		q_array[i].inter_state = INT_STATE_FIN;
		q_array[i].req_type = 0;
	}

	thread_metadata tm;

	uint iter_time = 0, pre_iter_time = 0;
	uint pre_req_id;
	ull tp, tn;

	printf("MIX\n");
	pre_req_id = 0;
	tp = get_time_ns();
	for(uint q_id = 0, req_id = 0; req_id < nput; q_id = (q_id+1==batch) ? 0 : q_id+1) {
		//if((req_id & 1023) == 1023) {
		//	printf("%d\n", req_id);
		//}
		iter_time++;


		Query &q = q_array[q_id];
		if(q.inter_state == INT_STATE_INIT) {
			iterate_init(q, tm);
		}
		else if(q.inter_state == INT_STATE_NODE) {
			iterate_node(q, tm);
		}
		else if(q.inter_state == INT_STATE_FIN) {
			if(q.req_type == REQ_GET && 
				(q.resp_type == RESP_HAS_KEY && q.resp_kv->key != *(key_type*)q.resp_kv->val)) {
				printf("GET FAILED\n");
				exit(1);
			}
			//printf("%d %d\n", q.resp_kv->key, *(key_type*)q.resp_kv->val);
			if(req_key[req_id ^ 1] & 1) {
				q.req_type = REQ_GET;
				q.resp_type = 0;
				q.inter_state = INT_STATE_INIT;
				q.key = req_key[req_id];
				q.put_node = NULL;

				iterate_init(q, tm);
				req_id ++;
			}
			else {
				q.req_type = REQ_PUT;
				q.resp_type = 0;
				q.inter_state = INT_STATE_INIT;
				q.key = req_key[req_id];
				q.put_node = (KV_Node*)tm.kv_node_allocator.allocate(KV_NODE_SIZE(test_len_val));
				q.put_node->init();
				q.put_node->kv.key = q.key;
				q.put_node->kv.len_val = test_len_val;
				*(key_type*)q.put_node->kv.val = q.key;
				q.put_node->modification_version ++;// make it static 
				iterate_init(q, tm);
				req_id ++;
			}
		}
		if((req_id & checkmask) == checkmask && req_id != pre_req_id) {
			tn = get_time_ns();
			ull dt = tn - tp;
			tp = tn;
			uint delta_req_id = req_id - pre_req_id;
			pre_req_id = req_id;
			double tpt = 1000.0 * delta_req_id / dt;
			printf("%.2lf MOPS\n", tpt);

			uint d_iter_time = iter_time - pre_iter_time;
			pre_iter_time = iter_time;
			//printf("%.2lf\n", 1.0 * d_iter_time / delta_req_id);
		}
	}
	
	return NULL;
}

int main(int argc, char **argv) // batch = 12
{
	
	if(argc != 3) {
		printf("usage: \"./selftest NTHREADS BATCH_SIZE\"\n");
		return 0;
	}
	// check batch size
	int nt = atoi(argv[1]);
	batch = atoi(argv[2]);
	assert(batch <= MAX_BATCH);

	// check rand function
	srand(time(0));
	check_rand();

	// choose key range
	init_key_range_mask(0x7fffffff);


	const uint nput = 1e7;
	// initialize all request keys
	key_type *req_key = new key_type[nput];
	for(uint i = 0; i < nput; i++) req_key[i] = key_rand();

	thread_metadata tm;
	// initialize tree
	init_tree(tm);

	pthread_t *pt = new pthread_t[nt];
	for(int i = 0; i < nt; i++) {
		uint L = i*nput/nt;
		uint R = (i+1)*nput/nt;
		workpos *ptr = new workpos;
		ptr->arr = req_key + L;
		ptr->size = R - L;
		
		pthread_create(&pt[i], NULL, myrun_put, ptr);
	}
	for(int i = 0; i < nt; i++) {
		pthread_join(pt[i], NULL);
	}

	for(int i = 0; i < nt; i++) {
		uint L = i*nput/nt;
		uint R = (i+1)*nput/nt;
		workpos *ptr = new workpos;
		ptr->arr = req_key + L;
		ptr->size = R - L;
		
		pthread_create(&pt[i], NULL, myrun_get, ptr);
	}
	for(int i = 0; i < nt; i++) {
		pthread_join(pt[i], NULL);
	}

	for(int i = 0; i < nt; i++) {
		uint L = i*nput/nt;
		uint R = (i+1)*nput/nt;
		workpos *ptr = new workpos;
		ptr->arr = req_key + L;
		ptr->size = R - L;
		
		pthread_create(&pt[i], NULL, myrun_mix, ptr);
	}
	for(int i = 0; i < nt; i++) {
		pthread_join(pt[i], NULL);
	}
	return 0;
}
