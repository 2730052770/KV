#include<bits/extc++.h>
#include "main.h"
#include "test.h"
using namespace std;
using namespace __gnu_pbds;
const int NITER = 1<<17;
const int window = 64;
const double max_offset = 0.1;
int key_capacity;
int q_num;
// big = (1e5, 1e8)
// mid = (1000, 1e7)
// small = (100, 1e7)
typedef tree<ull, string, less<ull>, rb_tree_tag, tree_order_statistics_node_update> mymap;
mymap mp;
string answer[window];
uint answertype[window];

#define GET 0
#define UPDATE 1
#define PUT_NEW 2
#define DELETE 3
#define GET_NOT_EXIST 4
#define DELETE_NOT_EXIST 5

void generate_KV(uint id, volatile KV *kv, bool key_exists, char req_type)
{
	ull key;
	string value;
	uint ord;
	if(key_exists) {
		ord = STL_randuint()%mp.size();
		auto p = mp.find_by_order(ord);
		key = p->first;
		answertype[id] = RESP_HAS_KEY;
		if(req_type == REQ_GET) answer[id] = p->second;
		if(req_type == REQ_DELETE) mp.erase(p);
	}
	else {
		do key = STL_randull(); while(mp.find(key) != mp.end());
		answertype[id] = RESP_NO_KEY;
	}
	kv->len_key = 8;
	*(ull*)kv->content = key;
	if(req_type == REQ_PUT) {
		value = gen_str(1, MAX_TEST2_V_SIZE);
		kv->len_value = value.size();
		volatile_cpy(kv->content + 8, (char*)value.c_str(), value.size());
		
		mp[key] = value;
	}
	else kv->len_value = 0;
}

int main(int argc, char **argv)
{
	if(argc != 3) {
		printf("usage \"./test3 KEY_NUM Q_NUM\"\n");
		return 0;
	}
	key_capacity = atoi(argv[1]);
	q_num = atoi(argv[2]);
	
	// allocate share memory
	int seed = time(0);//1638685516;
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
	int ACK = sm->S_ACK;
	while(ACK == sm->S_ACK) usleep(1000), sm->START++;
	
	//
	ll kv_sum = 0;
	int n_wr = 0;
	int n_split = 0;

	int n_get = 0, n_update = 0, n_put_new = 0, n_delete = 0, n_get_null = 0, n_delete_null = 0;
	
	for(int id = 0; ; id = id+1==window ? 0 : id+1) {
		
		if(unlikely(n_wr>=NITER)) {
			kv_sum += n_wr;
			if(kv_sum > q_num) break;
			int t_split = sm->n_split;
			printf("%.1lf%%: new_split = %d, tot_key_in_map = %d, tot_index = %d, %lld OPS IN TOTAL\n", 100.0*kv_sum/q_num, t_split-n_split, (int)mp.size(), t_split, kv_sum);
			printf("GET %d, UPD %d, NEW %d, DEL %d, GETNLL %d, DELNLL %d\n", n_get, n_update, n_put_new, n_delete, n_get_null, n_delete_null);
			n_get = n_update = n_put_new = n_delete = n_get_null = n_delete_null = 0;
			n_split = t_split;
			n_wr = 0;
		}	
		
		volatile TEST_Q *tq = sm->tq[id];
		if(tq->resp_type == RESP_EMPTY || tq->resp_type == RESP_READ) continue;
		
		if(tq->resp_type != RESP_INIT) {
			if(tq->resp_type != answertype[id]) {
				printf("%d %d %d %d\n", tq->req_type, answertype[id], tq->resp_type, n_wr);
			}
			assert(tq->resp_type == answertype[id]);
			//cout<<"checktype"<<endl;
			if(tq->req_type == REQ_GET && answertype[id] == RESP_HAS_KEY) {
				assert(answer[id].size() == tq->kv.len_value);
				for(int i = 0; i < tq->kv.len_value; i++)
					assert(answer[id][i] == tq->kv.content[8+i]);
				//cout<<"check get, answer = "<<answer[id][0]<<"..."<<endl;
			}
		}
		
		
		
		n_wr ++;
		volatile KV *kv = &tq->kv;
		
		// NOTE THAT THERE IS NO DELETE-TEST
		int type = rand()%6;
		if((type == GET || type == UPDATE) && mp.empty()) type = PUT_NEW;
		else if(type == DELETE && mp.size() < (1-max_offset)*key_capacity) type = PUT_NEW;
		else if(type == PUT_NEW && mp.size() > (1+max_offset)*key_capacity) type = DELETE;
		

		switch(type) {
			case GET:
				n_get++;
				tq->req_type = REQ_GET;
				generate_KV(id, kv, 1, REQ_GET);
				break;
			case DELETE:
				n_delete++;
				tq->req_type = REQ_DELETE;
				generate_KV(id, kv, 1, REQ_DELETE);
				break;
			case GET_NOT_EXIST:
				n_get_null++;
				tq->req_type = REQ_GET;
				generate_KV(id, kv, 0, REQ_GET);
				break;
			case DELETE_NOT_EXIST:
				n_delete_null++;
				tq->req_type = REQ_DELETE;
				generate_KV(id, kv, 0, REQ_DELETE);
				break;
			case PUT_NEW:
				n_put_new++;
				tq->req_type = REQ_PUT;
				generate_KV(id, kv, 0, REQ_PUT);
				break;
			case UPDATE:
				n_update++;
				tq->req_type = REQ_PUT;
				generate_KV(id, kv, 1, REQ_PUT);
				break;
			
		}
		//__sync_synchronize();
		tq->resp_type = RESP_EMPTY;
		
		
		while(tq->resp_type == RESP_EMPTY);
		//__sync_synchronize();
		//usleep(1000);
	}
	ACK = sm->E_ACK;
	while(ACK == sm->E_ACK) usleep(1000), sm->END++;
	return 0;
}
