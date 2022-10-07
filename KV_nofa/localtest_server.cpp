
#include "kvp.h"
#include "test.h"
#include "main.h"
#include "kv.h"

int main(int argc, char **argv)
{
	int batch;
	if(argc != 2) {
		printf("usage: \"./localtest_server BATCH_SIZE\"\n");
		return 0;
	}
	batch = atoi(argv[1]);

	static char global_kv_buf[MAX_BATCH*MAX_TEST_KV_SIZE];
	Query q[MAX_BATCH];
	memset(q, 0, sizeof(q));

	KVS kvs;
	
	int shmid = shmget(SHMKEY, M, 0666 | IPC_CREAT);
	if(shmid == -1) {
		puts("shm error");
		return 0;
	}
	share_mem *sm = (share_mem *)shmat(shmid, SHMVA, 0);
	if((long long)sm == (long long)-1) {
		puts("shm error");
		return 0;
	}
	int signal = sm->START;
	while(signal == sm->START) usleep(1000);
	sm->S_ACK ++;
	signal = sm->END;
	
	int num = 0, old = 0;
	int window = sm->window;
	
	int emptyloop = 0, validloop = 0;
	for(int id = 0; ; id = id+1==window ? 0 : id+1) {
		//如果要避免多个PUT在同一个BUCKET导致的各种情况，则可以要求每个batch中每个BUCKET至多一个PUT
		//进一步地，如果要防止同一个batch中的GET/PUT乱序，最好加强要求每个BUCKET中PUT和GET不共存
		//注意！treetag不同也可能会分到同一个BUCKET
		//puts("loop");
		
		
		volatile TEST_Q *tq = sm->tq[id];
		if(tq->resp_type != RESP_EMPTY && signal == sm->END) {
			emptyloop++;
			continue;
		}
		if(signal != sm->END) break;
		
		validloop++;
		if((validloop & 0xfffff) == 0) {
			printf("%d %d\n", emptyloop, validloop);
		}
		
		//__sync_synchronize();
		//fence
		
		
		Query *qy = q + num;
		qy->tq = tq;
		qy->req_type = tq->req_type;
		qy->resp_type = RESP_EMPTY;
		if(!qy->q_kv) qy->q_kv = (KV*)(global_kv_buf + MAX_TEST_KV_SIZE*num);
		
		volatile KV *kv = &tq->kv;
		
		//qy->q_kv = (KV*)kv;
		volatile_cpy(qy->q_kv, kv, KV_SIZE(kv->len_key, kv->len_value));
		
		tq->resp_type = RESP_READ;
		//printf("READ\n");
		//printf("%s %llu\n", qy->req_type == REQ_GET ? "GET" : "PUT", *(ull*)qy->q_kv->content);
		
		//__sync_synchronize();
		//fence
		num++;
		
		if(num == batch) {
			switch(num){
				case 1:old = num = kvs. solve<1>(old, q); break;
				case 2:old = num = kvs. solve<2>(old, q); break;
				case 3:old = num = kvs. solve<3>(old, q); break;
				case 4:old = num = kvs. solve<4>(old, q); break;
				case 5:old = num = kvs. solve<5>(old, q); break;
				case 6:old = num = kvs. solve<6>(old, q); break;
				case 7:old = num = kvs. solve<7>(old, q); break;
				case 8:old = num = kvs. solve<8>(old, q); break;
			}
			for(int i = old; i < batch; i++) {
				if(q[i].req_type == REQ_GET && q[i].resp_type == RESP_HAS_KEY) {
					volatile_cpy(&q[i].tq->kv, &q[i].old_block->kv, KV_SIZE(q[i].old_block->kv.len_key, q[i].old_block->kv.len_value));
				}
				q[i].tq->resp_type = q[i].resp_type;
				//usleep(1);
			}
		}
	}
	sm->E_ACK++;
	return 0;
 }
