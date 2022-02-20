#include "cstdlib"
#include "kv.h"
#include "test.h"
using namespace std;

KVS kvs;

template<int n>
uint solve(uint old, Query *q) {
	return kvs.solve<n>(old, q);
}

uint (*solve_table[]) (uint, Query*) = {
	0, solve<1>, solve<2>, solve<3>, solve<4>, 
	solve<5>, solve<6>, solve<7>, solve<8>,
	solve<9>, solve<10>, solve<11>, solve<12>,
};

int main(int argc, char **argv)
{
	
	if(argc != 2) {
		printf("usage: \"./selftest BATCH_SIZE\"\n");
		return 0;
	}
	uint batch = atoi(argv[1]);
	
	static char global_kv_buf[MAX_BATCH*MAX_TEST_KV_SIZE];
	Query q[MAX_BATCH];
	memset(q, 0, sizeof(q));

	
	const uint nput = 3e7, checkmask = 0xfffff;
	ll *k = new ll[nput];
	srand(time(0));
	for(uint i = 0; i < nput; i++) k[i] = (ll)rand() << 32 ^ rand();
	
	
	for(uint i = 0; i < MAX_BATCH; i++) q[i].q_kv = (KV*)(global_kv_buf + i * MAX_TEST_KV_SIZE);
	
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
			old = num = solve_table[batch](old, q);
			//printf("%d\n", old);
		}
		//assert(num < batch);
		
		if((i & checkmask) == checkmask) {
			tn = get_time_ns();
			dt = tn - tp;
			tp = tn;
			printf("%.2lf Mop/s\n", 1e3*(checkmask+1)/dt);
		}
	}
	
	
	while(num) {
		old = num = solve_table[num](old, q);// clear them all
	}
	// now, old == num == 0
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
			old = num = solve_table[batch](old, q);
			
			//return 0;
			/*for(uint j = old; j < num; j++) {
				if(q[j].resp_type == RESP_EMPTY)
					assert(false);
			}*/
		}
		//assert(num < batch);
			
		if((i & checkmask) == checkmask) {
			tn = get_time_ns();
			dt = tn - tp;
			tp = tn;
			printf("%.2lf Mop/s\n", 1e3*(checkmask+1)/dt);
		}
	}
	return 0;
 }
