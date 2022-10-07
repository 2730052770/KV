#include "main.h"
#include "test.h"


const int NITER = 65536;
ull rand_x[NITER], rand_y[NITER];// may produce 17GB KV
int raw, col;

void init_rand()
{
	for(int i = 0; i < NITER; i++) 
		rand_x[i] = STL_randull(), rand_y[i] = STL_randull();
}

ull new_rand()
{
	return rand_y[raw] ^ rand_x[col++];
}

ull old_rand()
{
	static ull seed = 114514ull << 40 ^ 1919810;
	ull tot = raw * NITER + col;
	ull id = fastrand(&seed) % tot;
	int old_raw = id / NITER;
	int old_col = id % NITER;
	return rand_y[old_raw] ^ rand_x[old_col];
}

int main(int argv, char **argc)
{
	if(argv != 2) {
		puts("use: ./test1 $BATCHSIZE");
		return 0;
	}

	int shmid = shmget(SHMKEY, M, 0666 | IPC_CREAT);
	if(shmid == -1) {
		puts("shm error");
		return 0;
	}
	volatile share_mem *sm = (share_mem *)shmat(shmid, SHMVA, 0);
	if((long long)sm == (long long)-1) {
		puts("shm error");
		return 0;
	}
	
	int batch = atoi(argc[1]);
	sm->n_split = 0;
	
	for(int i = 0; i < batch; i++) {
		volatile TEST_Q* volatile*tq = sm->tq;
		tq[i] = (TEST_Q*)((char*)((TEST_Q**)tq+batch) + CACHELINEROUNDUP(TEST1_Q_SIZE)*i);
		tq[i]->resp_type = RESP_INIT; 
	}
	
	init_rand();
	
	
	int ACK = sm->S_ACK;
	while(ACK == sm->S_ACK) usleep(1000), sm->START++;
	
	ull st = get_time_ns(), t1 = st, t2;
	ull dt_get = 0, dt_put = 0, pre_dt_get = 0, pre_dt_put = 0;
	ll kv_sum_get = 0, kv_sum_put = 0, pre_kv_sum_get = 0, pre_kv_sum_put = 0;
	ll pre_sum = 0;
	int turn = 1;
	int n_wr = 0;
	int n_split = 0;
	
	for(int id = 0; ; id = id+1==batch ? 0 : id+1) {
		
		if(unlikely(n_wr>=NITER)) {
			t2 = get_time_ns();
			if(turn) kv_sum_put += n_wr, dt_put += t2-t1;
			else kv_sum_get += n_wr, dt_get += t2-t1;
			
			if(kv_sum_put+kv_sum_get > 8e7) break;
			t1 = t2;
			turn ^= 1;
			n_wr = 0;
			if(turn == 0)// next is a new round PUT-GET
				raw ++, col = 0;
			if(raw == NITER)
				break;
		}
		if((kv_sum_put+kv_sum_get) >= pre_sum + (1<<20)) {
			pre_sum = kv_sum_put+kv_sum_get;
			double tpt_p = 1e3*(kv_sum_put-pre_kv_sum_put)/(dt_put-pre_dt_put);
			double tpt_g = 1e3*(kv_sum_get-pre_kv_sum_get)/(dt_get-pre_dt_get);
			pre_kv_sum_put = kv_sum_put;
			pre_kv_sum_get = kv_sum_get;
			pre_dt_put = dt_put;
			pre_dt_get = dt_get;
			int t_split = sm->n_split;
			printf("tpt_p = %.2lf MOPS, tpt_g = %.2lf MOPS, new_split = %d, %lld OPS IN TOTAL\n", tpt_p, tpt_g, t_split-n_split, kv_sum_put+kv_sum_get);
			n_split = t_split;
		}
			
		volatile TEST_Q *tq = sm->tq[id];
		if(tq->resp_type == RESP_EMPTY || tq->resp_type == RESP_READ) continue;
		
		//fence
		//__sync_synchronize();
		
		/*
		
			check return value here
		
		*/
		
		
		
		n_wr ++;
		volatile KV *kv = &tq->kv;
		
		// NOTE THAT THERE IS NO DELETE-TEST
		
		if(turn) {// PUT
			tq->req_type = REQ_PUT;
			
			kv->len_key = TEST1_KEY_LEN;
			kv->len_value = TEST1_VALUE_LEN;
			
			ull rd = new_rand();// to be writen
			*(volatile ull*)kv->content = rd;
			char byte = (char)rd;
			volatile_set(kv->content + 8, byte, kv->len_key-8);// other bytes of key

			volatile_set(kv->content + kv->len_key, byte, kv->len_value);// bytes of value
		}
		else {// GET
			tq->req_type = REQ_GET;
			
			kv->len_key = TEST1_KEY_LEN;
			kv->len_value = 0;
			ull rd = old_rand();
			*(volatile ull*)kv->content = rd;
			char byte = (char)rd;
			volatile_set(kv->content + 8, byte, kv->len_key-8);// other bytes of key
		}
		//__sync_synchronize();
		tq->resp_type = RESP_EMPTY;
	}
	printf("AVG TPT = %.2lf MOPS\n", 1000.0*(kv_sum_get+kv_sum_put)/(get_time_ns()-st));
	ACK = sm->E_ACK;
	while(ACK == sm->E_ACK) usleep(1000), sm->END++;
	return 0;
}
