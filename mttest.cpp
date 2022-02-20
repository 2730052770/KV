#include <pthread.h>
#include "cstdlib"
#include "kv.h"
#include "test.h"
using namespace std;

template<int n>
uint solve(KVS &kvs, uint old, Query *q) {
	return kvs.solve<n>(old, q);
}

uint (*solve_table[]) (KVS &, uint, Query*) = {
	0, solve<1>, solve<2>, solve<3>, solve<4>, 
	solve<5>, solve<6>, solve<7>, solve<8>,
	solve<9>, solve<10>, solve<11>, solve<12>,
};

pthread_t th[MAX_BATCH];
volatile int cnt, nthread;



void * run(void * arg)
{
	uint tid = (ull)arg>>32;
	uint batch = (int)(ull)arg;
	printf("tid = %d, batch = %d\n", tid, batch);

	Query q[MAX_BATCH];
	memset(q, 0, sizeof(q));

	
	const uint nput = 1e7, checkmask = 0xfffff;
	ll *k = new ll[nput];
	srand(time(0) + tid);
	for(uint i = 0; i < nput; i++) k[i] = (ll)rand() << 32 ^ rand();
	
	
	for(uint i = 0; i < MAX_BATCH; i++) q[i].q_kv = (KV*)malloc(MAX_TEST_KV_SIZE);
	
	KVS kvs;
	
	uint num = 0, old = 0;
	ull tp = get_time_ns(), tn, dt;
	printf("PUT\n");
	for(uint i = 0; i < nput; i++) {
		q[num].req_type = REQ_PUT;
		q[num].resp_type = RESP_EMPTY;
		q[num].q_kv->len_value = 8;
		q[num].q_kv->len_key = 8;
		((ll*)q[num].q_kv->content)[0] = k[i];
		((ll*)q[num].q_kv->content)[1] = k[i];
		num++;
		if(num == batch) {
			old = num = solve_table[batch](kvs, old, q);
		}
		
		if((i & checkmask) == checkmask) {
			tn = get_time_ns();
			dt = tn - tp;
			tp = tn;
			printf("%d: %.2lf Mop/s\n", tid, 1e3*(checkmask+1)/dt);
		}
	}
	
	
	while(num) {
		old = num = solve_table[num](kvs, old, q);// clear them all
	}
	printf("%d: finish\n", tid);
	
	cnt++;
	while(cnt != nthread);
	
	random_shuffle(k, k+nput);
	tp = get_time_ns();
	
	printf("GET\n");
	for(uint i = 0; i < nput; i++) {
		q[num].req_type = REQ_GET;
		q[num].resp_type = RESP_EMPTY;
		q[num].q_kv->len_value = 0;
		q[num].q_kv->len_key = 8;
		((ll*)q[num].q_kv->content)[0] = k[i];
		num++;
		if(num == batch) {
			old = num = solve_table[batch](kvs, old, q);
		}
			
		if((i & checkmask) == checkmask) {
			tn = get_time_ns();
			dt = tn - tp;
			tp = tn;
			printf("%d: %.2lf Mop/s\n", tid, 1e3*(checkmask+1)/dt);
		}
	}
	printf("%d: finish\n", tid);
	return NULL;
}

int main(int argc, char **argv)
{
	if(argc != 3) {
		printf("usage: \"./selftest NTHREAD BATCH_SIZE\"\n");
		return 0;
	}
	nthread = atoi(argv[1]);
	uint batch = atoi(argv[2]);
	
	for(int i = 0; i < nthread; i++)
		pthread_create(th+i, NULL, run, (void*)((ull)i<<32|batch));
		
	for(int i = 0; i < nthread; i++)
		pthread_join(th[i], NULL);
	
	return 0;
 }
