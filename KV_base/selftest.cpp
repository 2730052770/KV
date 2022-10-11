#include <cstdlib>
#include "kv.h"
#include "test.h"
#include "kv.cpp"
using namespace std;

KVS kvs;
const uint nkey = 1e7, checkmask = 0xfffff;
static char global_kv_buf[MAX_BATCH*MAX_TEST_KV_SIZE];
Query q[MAX_BATCH];
ll k[nkey];
uint batch;

template<int n>
uint solve(uint old, Query *qy) {
	return kvs.solve<n>(old, qy);
}

uint (*solve_table[]) (uint, Query*) = {
	0, solve<1>, solve<2>, solve<3>, solve<4>, 
	solve<5>, solve<6>, solve<7>, solve<8>,
	solve<9>, solve<10>, solve<11>, solve<12>,
};

void uniform_test()
{
	uint num = 0, old = 0;
	ull t_start = get_time_ns();
	ull tp = get_time_ns(), tn, dt;
	printf("PUT\n");
	for(uint i = 0; i < nkey; i++) {
		q[num].req_type = REQ_PUT;
		q[num].resp_type = RESP_EMPTY;
		q[num].q_kv->len_value = 4;
		q[num].q_kv->len_key = 4;
		((uint*)q[num].q_kv->content)[0] = (uint)k[i];
		((uint*)q[num].q_kv->content)[1] = (uint)k[i];
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
	ull t_end = get_time_ns();
	printf("TOTAL %.2lf Mop/s\n", 1e3*nkey/(t_end - t_start));
	
	
	while(num) {
		old = num = solve_table[num](old, q);// clear them all
	}
	// now, old == num == 0
	random_shuffle(k, k+nkey);
	tp = get_time_ns();
	
	t_start = get_time_ns();

	printf("GET\n");
	for(uint i = 0; i < nkey; i++) {
		q[num].req_type = REQ_GET;
		q[num].resp_type = RESP_EMPTY;
		q[num].q_kv->len_value = 0;
		q[num].q_kv->len_key = 4;
		((uint*)q[num].q_kv->content)[0] = (uint)k[i];
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
	t_end = get_time_ns();
	printf("TOTAL %.2lf Mop/s\n", 1e3*nkey/(t_end - t_start));
}

void zipf_test()
{
	const uint nreq = 1e7;
	double *r = new double[nkey];
	ll *req = new ll[nreq];
	int cnt0 = 0, cnt1 = 0;
	r[0] = 1;
	for(uint i = 1; i < nkey; i++) r[i] = 1.0/(i+1) + r[i-1];
	for(uint i = 0; i < nkey; i++) r[i] = r[i] / r[nkey-1];
	
	static const int nbucket = 8;
	static int bucket[nbucket];
	
	for(uint i = 0; i < nreq; i++) {
		double p = 1.0*rand()/RAND_MAX;
		int L = 0, R = nkey-1, ans = -1;
		while(L <= R) {
			int m = (L+R) >> 1;
			if(r[m] >= p) ans = m, R = m-1;
			else L = m+1;
		}
		assert(ans != -1);
		req[i] = k[ans];
		bucket[req[i]&(nbucket-1)]++;
		if(ans == 0) cnt0++;
		if(ans == 1) cnt1++;
	}
	for(int i = 0; i < nbucket; i++) {
		printf("bucket[%d] = %d, ", i, bucket[i]);
	}
	puts("");
	printf("number of the first key = %d, second key = %d\n", cnt0, cnt1);
	uint num = 0, old = 0;
	ull tp = get_time_ns(), tn, dt;
	printf("PUT\n");
	for(uint i = 0; i < nreq; i++) {
		q[num].req_type = REQ_PUT;
		q[num].resp_type = RESP_EMPTY;
		q[num].q_kv->len_value = 4;
		q[num].q_kv->len_key = 4;
		((uint*)q[num].q_kv->content)[0] = (uint)req[i];
		((uint*)q[num].q_kv->content)[1] = (uint)req[i];
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
	random_shuffle(k, k+nkey);
	tp = get_time_ns();
	
	printf("GET\n");
	for(uint i = 0; i < nreq; i++) {
		q[num].req_type = REQ_GET;
		q[num].resp_type = RESP_EMPTY;
		q[num].q_kv->len_value = 0;
		q[num].q_kv->len_key = 4;
		((uint*)q[num].q_kv->content)[0] = (uint)req[i];
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
	
}

int main(int argc, char **argv)
{
	
	if(argc != 2) {
		printf("usage: \"./selftest BATCH_SIZE\"\n");
		return 0;
	}
	batch = atoi(argv[1]);

	srand(time(0));

	for(uint i = 0; i < nkey; i++) k[i] = rand() & 0x7fffffff;
	
	for(uint i = 0; i < MAX_BATCH; i++) q[i].q_kv = (KV*)(global_kv_buf + i * MAX_TEST_KV_SIZE);
	
	uniform_test();
	
	zipf_test();
	
	return 0;
 }
