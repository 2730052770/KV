#include "global.h"
#include "log.h"
#include "kvp.h"

void Log_pair::record(uc type, KV *kv, ull time) {
	acquire_log();
	Log *log = l[log_id_inuse];
	size_t presize = log->ptr-log->buf;
	log->record(type, kv, time);
	if((log->ptr-log->buf) * 8 >= Log::bufsize && presize * 8 < Log::bufsize) 
		pthread_cond_signal(&cond);
	release_log();
}

void* Log_pair::run(void *_) {
	pthread_mutex_lock(&mutex);
	while(1) 
	{
		acquire_log();
		log_id_inuse ^= 1;
		Log *log = l[log_id_inuse ^ 1];// unused one
		release_log();
		
		if(inform_change_file) {// when make checkpoint
			inform_change_file = 0;
			close(fd);
			file_id_inuse ^= 1;
			fd = open(file_name[file_id_inuse], O_WRONLY|O_CREAT|O_TRUNC, 0666);
		}
		write(fd, log->buf, log->ptr - log->buf);
		log->ptr = log->buf;
		
		if(l[log_id_inuse]->ptr - l[log_id_inuse]->buf < Log::bufsize/8)
			pthread_cond_timedwait(&cond, &mutex, &tm);
		// do
	}
	return NULL;
}

void Log_pair::init(int _tid) {//在run logger前先建立一个加载老的checkpoint并建立一个全新的checkpoint(建立完了再删除原来的两个)

	tm.tv_sec = 0;
	tm.tv_nsec = 100000000;

	tid = _tid;

	x.init();
	y.init();
	l[0] = &x;
	l[1] = &y;
	log_id_inuse = 0;
	file_id_inuse = 0;
	cond = PTHREAD_COND_INITIALIZER;
	mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_spin_init(&spinlock, PTHREAD_PROCESS_SHARED);// = PTHREAD_SPINLOCK_INITIALIZER;
	
	
	for(int fid = 0; fid < 2; fid++) {
		sprintf(file_name[fid], "./log-T%d-%d", tid, fid);
	}
	fd = open(file_name[0], O_WRONLY|O_CREAT|O_TRUNC, 0666);
	
	pthread_create(&thread, NULL, run, NULL);
}
