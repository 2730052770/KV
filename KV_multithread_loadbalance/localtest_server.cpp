#include "kv.cpp"




int main(int argc, char **argv)
{
	int batch;
	if(argc != 2) {
		printf("usage: \"./localtest_server $BATCH_SIZE\"\n");
		return 0;
	}
	batch = atoi(argv[1]);

	static char global_kv_buf[MAX_BATCH*MAX_TEST_KV_SIZE];
	Query q[MAX_BATCH];
	memset(q, 0, sizeof(q));

	init_allocators();
	
	init_tree();
	
	
	int shmid = shmget(SHMKEY, M, 0666 | IPC_CREAT);
	if(shmid == -1) {
		puts("shm error");
		return 0;
	}
	sm = (share_mem *)shmat(shmid, SHMVA, 0);
	if((long long)sm == (long long)-1) {
		puts("shm error");
		return 0;
	}
	int signal = sm->START;
	while(signal == sm->START) usleep(1000);
	sm->S_ACK ++;
	signal = sm->END;
	
	int num = 0, old = 0;

	for(int id = 0; id < batch; id++) {
		q[id].q_kv = (KV*)(global_kv_buf + MAX_TEST_KV_SIZE*id);
		q[id].answer = malloc(1<<20);
	}

	while(1)
	{
		if(signal != sm->END) break;

		for(int id = 0; id < batch; id++ ) {
			
			volatile TEST_Q *tq = sm->tq[id];
			Query *qy = q + id;
			/*
			this should be done in iteration
			if(qy->req_type == REQ_GET && qy->resp_type == RESP_HAS_KEY) 
				volatile_cpy(&q[i].tq->kv, &q[i].old_block->kv, KV_SIZE(q[i].old_block->kv.len_key, q[i].old_block->kv.len_value));
			
			q[i].tq->resp_type = q[i].resp_type;
			*/
			if(tq->resp_type != RESP_EMPTY) continue;
			
			
			//__sync_synchronize();
			//fence
			
			qy->state = state_init;
			qy->tq = tq;
			qy->req_type = tq->req_type;
			qy->resp_type = RESP_EMPTY;
			
			volatile KV *kv = &tq->kv;
			
			//qy->q_kv = (KV*)kv;
			
			volatile_cpy(qy->q_kv, kv, KV_SIZE(kv->len_key, kv->len_value));
			
			tq->resp_type = RESP_READ;
		}
		
		iterate();
	}
		
	sm->E_ACK++;
	return 0;
 }
