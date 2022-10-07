#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <linux/fs.h>
#include <linux/fd.h>
#include <linux/hdreg.h>
#include <libaio.h>

#include "global.h"
#include "kvp.h"

struct Meta_log{
	// time
	
};

struct Log{
	static const size_t bufsize = 16*M;//must be multiple of logic block size (typically 512 bytes)
	char *buf, *end, *ptr;
	
	inline void init() {
		ptr = buf = (char*)mmap_hugepage(bufsize);
		end = buf + bufsize;
	}
	
	inline void record(uc type, KV *kv, ull time) {
		*(uc*)ptr = type;
		ptr += sizeof(type);
		*(ull*)ptr = time;
		ptr += sizeof(time);
		size_t kv_size = KV_SIZE(kv->len_key, kv->len_value);
		memcpy(ptr, kv, kv_size);
		ptr += kv_size;
	}
};

struct Log_pair{
	Log x;
	char unusedx[remaining(sizeof(Log))];
	Log y;
	char unusedy[remaining(sizeof(Log))];
	Log *l[2];
	int log_id_inuse;// exchange when log thread wake up
	int inform_change_file;
	int file_id_inuse;// exchange when building checkpoint
	pthread_t thread;
	int fd;
	int tid;
	char file_name[2][20];
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_spinlock_t spinlock;
	
	
	static timespec tm;
	
	void record(uc type, KV *kv, ull time);
	
	void* run(void * _);
	
	void init(int _tid);
	
	inline void acquire_log() {
		pthread_spin_lock(&spinlock);
	}
	inline void release_log() {
		pthread_spin_unlock(&spinlock);
	}
};


#endif
