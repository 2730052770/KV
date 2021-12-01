#include "main.h"
#include "test.h"
#include <string>
#include <algorithm>
#include <map>
using namespace std;
const int NITER = 1<<16;
const int window = 32;
const int key_num = 1e5;
const int q_num = 1e7;
// big = (1e5, 1e8)
// mid = (1000, 1e7)
// small = (100, 1e7)
int n_new_key;
ull keys[key_num], keys_have_put[key_num];
string answer[window];


int main()
{
	// allocate share memory
	int seed = time(0);
	srand(seed);
	printf("seed = %d\n",seed);
	
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
	
	// init share memory
	sm->window = window;
	sm->n_split = 0;
	
	
	for(int i = 0; i < window; i++) {
		volatile TEST_Q* volatile*tq = sm->tq;
		tq[i] = (TEST_Q*)((char*)((TEST_Q**)tq+window) + TEST2_Q_SIZE*i);
		tq[i]->resp_type = RESP_INIT; 
	}
	// shake hands
	int ACK = sm->ACK;
	while(ACK == sm->ACK) usleep(1000), sm->signal++;
	
	//
	ull t1 = get_time_ns(), t2;
	ll kv_sum = 0;
	int n_wr = 0;
	int n_split = 0;
	
	// generate keys
	loop:
	for(int i = 0; i < key_num; i++) {
		keys[i] = STL_randull();
	}
	sort(keys, keys+key_num);
	if(unique(keys, keys+key_num)-keys != key_num) goto loop;
	
	
	
	map<ull, string>mp;
	
	int n_put = 0, n_get = 0;
	
	for(int id = 0; ; id = id+1==window ? 0 : id+1) {
		
		if(unlikely(n_wr>=NITER)) {
			kv_sum += n_wr;
			if(kv_sum > q_num) break;
			t2 = get_time_ns();
			double dt = (t2 - t1)/1e9;
			t1 = t2;
			double tpt = 1e-6*n_wr/dt;
			int t_split = sm->n_split;
			printf("%.1lf%%: new_split = %d, n_put = %d, n_get = %d, tot_key_in_map = %d, tot_index = %d, %lld OPS IN TOTAL\n", 100.0*kv_sum/q_num, t_split-n_split, n_put, n_get, (int)mp.size(), t_split, kv_sum);
			n_put = n_get = 0;
			n_split = t_split;
			n_wr = 0;
		}	
		
		volatile TEST_Q *tq = sm->tq[id];
		if(tq->resp_type == RESP_EMPTY || tq->resp_type == RESP_READ) continue;
		
		if(tq->resp_type != RESP_INIT && tq->req_type == REQ_GET) {
			assert(tq->resp_type == RESP_HAS_KEY);
			assert(answer[id].size() == tq->kv.len_value);
			for(int i = 0; i < tq->kv.len_value; i++)
				assert(answer[id][i] == tq->kv.content[8+i]);
		}
		
		
		
		n_wr ++;
		volatile KV *kv = &tq->kv;
		
		// NOTE THAT THERE IS NO DELETE-TEST
		int if_put = rand()&1;
		//printf("%d %d\n", if_put, n_new_key);
		

		
		if(if_put || !n_new_key) {// PUT
		
			n_put++;
		
			int ord = STL_randuint()%key_num;
			ull key = keys[ord];
			if(mp.find(key) == mp.end()) keys_have_put[n_new_key++] = key;
		
			string s = gen_str(1, MAX_TEST2_V_SIZE);
			mp[key] = s;
		
			tq->req_type = REQ_PUT;
			
			kv->len_key = 8;
			kv->len_value = s.size();
			
			*(volatile ull*)kv->content = key;
			volatile_cpy(kv->content + 8, (char*)s.c_str(), s.size());// other bytes of key
			
			//printf("PUT %llu\n", key);
		}
		else {// GET
		
			n_get++;
		
			int ord = STL_randuint()%n_new_key;
			ull key = keys_have_put[ord];
			
			answer[id] = mp[key];
		
			tq->req_type = REQ_GET;
			
			kv->len_key = 8;
			kv->len_value = 0;
			*(volatile ull*)kv->content = key;
			
			//printf("GET %llu\n", key);
		}
		//__sync_synchronize();
		tq->resp_type = RESP_EMPTY;
		
		
		while(tq->resp_type == RESP_EMPTY);
		//__sync_synchronize();
		//usleep(1000);
	}
	return 0;
}
