#include "main.h"
#include "test.h"


const int NITER = 65536;
const int window = 32;
const int delay = 32;
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

int main()
{
	assert(NITER >= 2*delay);
	
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
	
	sm->window = window;
	sm->n_split = 0;
	
	for(int i = 0; i < window; i++) {
		volatile TEST_Q* volatile*tq = sm->tq;
		tq[i] = (TEST_Q*)((char*)((TEST_Q**)tq+window) + TEST1_Q_SIZE*i);
		tq[i]->resp_type = RESP_INIT; 
	}
	
	init_rand();
	
	
	int ACK = sm->S_ACK;
	while(ACK == sm->S_ACK) usleep(1000), sm->START++;
	
	ull t1 = get_time_ns(), t2;
	ll kv_sum = 0;
	int turn = 1;
	int n_wr = 0;
	int n_split = 0;
	
	for(int id = 0; ; id = id+1==window ? 0 : id+1) {
		
		if(unlikely(n_wr>=NITER)) {
			kv_sum += n_wr;
			if(kv_sum > 1e8) break;
			t2 = get_time_ns();
			double dt = (t2 - t1)/1e9;
			t1 = t2;
			double tpt = 1e-6*n_wr/dt;
			int t_split = sm->n_split;
			printf("op = %s, tpt = %.2lf MOPS, dt = %2lf ms, new_split = %d, %lld OPS IN TOTAL\n", turn?"PUT":"GET", tpt, dt, t_split-n_split, kv_sum);
			n_split = t_split;
			if(turn == 0)// next is a new round PUT-GET
				raw ++, col = 0;
			if(raw == NITER)
				break;
			turn ^= 1;
			n_wr = 0;
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
	ACK = sm->E_ACK;
	while(ACK == sm->E_ACK) usleep(1000), sm->END++;
	return 0;
}
