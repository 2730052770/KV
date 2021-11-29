#include "main.h"
#include "test.h"


const int NITER = 65536;
const int window = 16;
ull rand_x[NITER], rand_y[NITER];// may produce 17GB KV
int raw, col;

uint STL_randuint() {
	return rand() << 16 ^ rand();
}

ull STL_randull() {
	return (ull)STL_randuint() << 32 ^ STL_randuint();
}

inline ull fastrand(ull *seed){
	ull x = *seed;
	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	*seed = x;
	return x * 0x2545F4914F6CDD1D;
}

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
	
	sm->window = window;
	sm->n_split = 0;
	volatile KV* volatile*kv = sm->kv;
	
	for(int i = 0; i < window; i++) {
		kv[i] = (KV*)((char*)((TEST_KV*)kv+window) + TEST_KV_SIZE*i);
		kv[i]->len_key = 0; // len_key == 0 means read complete
	}
	
	init_rand();
	
	
	int ACK = sm->ACK;
	while(ACK == sm->ACK) usleep(1000), sm->signal++;
	
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
		
		if(kv[id]->len_key != 0) continue;
		
		//fence
		//__sync_synchronize();
		
		
		uc len_key = TEST_KEY_LEN;
		
		n_wr ++;
		if(turn) {// PUT
			kv[id]->len_value = TEST_VALUE_LEN;// if delete, len_value == -1
			ull rd = new_rand();// to be writen
			*(volatile ull*)kv[id]->content = rd;
			char byte = (char)rd;
			volatile_set(kv[id]->content + 8, byte, len_key-8);// other bytes of key
			if(kv[id]->len_value != (us)-1)
				volatile_set(kv[id]->content + len_key, byte, kv[id]->len_value);// bytes of value
			
		}
		else {// GET
			kv[id]->len_value = 0;
			ull rd = old_rand();
			*(volatile ull*)kv[id]->content = rd;
			char byte = (char)rd;
			volatile_set(kv[id]->content + 8, byte, len_key-8);// other bytes of key
		}
		//__sync_synchronize();
		
		kv[id]->len_key = len_key;
		
		//printf("%d\n",n_wr);
		
	}
	return 0;
}
