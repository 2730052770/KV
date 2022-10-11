#include "mica.h"


// 6 thread, 1 thread = 1 index + 1 log
// index = 4M bucket, 1 bucket = 8 entries, 1 entry = 8B = 16b(verify) + 48b(offset)
// log = 128M

void binding(int id) {
  cpu_set_t mask;		// big improvement
  CPU_ZERO(&mask);    //置空
  CPU_SET(id, &mask);   //设置亲和力值
  if (sched_setaffinity(0, sizeof(mask), &mask))//设置线程CPU亲和力
  {
    puts("could not set CPU affinity\n");
    exit(1);
  }
}

void makekill(int id) {
  int pid = (int) getpid();
  fprintf(stderr,"pid: %d\n",pid);
  FILE *kill_all = fopen("kill_all.sh",id?"a":"w");
  fprintf(kill_all, "kill -9 %d\n", pid);
  fclose(kill_all);
}

void * allocate_hugepage_memory(size_t length)
{
  void *buf = mmap(NULL, length, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | 0x40000 /*HUGEPAGE*/, -1, 0);
  if(buf == MAP_FAILED){
    puts("map failed");
    exit(1);
  }
  memset(buf, 0, length);
  return buf;
}

void init_table(struct Table *tb, int id)
{
  tb->index = allocate_hugepage_memory(sizeof(struct Bucket) * NBUCKET);
  tb->log = allocate_hugepage_memory(LOGSIZE);
  tb->id = id;
  tb->nbkt = NBUCKET;
  tb->bkt_mask = NBUCKET-1;
  tb->nentry = NENTRY;
  tb->logsize = LOGSIZE;
  tb->logmask = LOGSIZE-1;
  tb->loghead = LOGSIZE;
  tb->nget_success = 0;
  tb->nget_failed = 0;
  tb->nput = 0;
  tb->nevict = 0;
}

int main(int argc, char *argv[]) {
  
  int id = atoi(argv[1]);
  if(argc != 2 || id < 0 || id > 11) {
    puts("id error");
    exit(1);
  }
  
  makekill(id);
  puts("makekill OK");
  
  binding(id);
  puts("binding OK");
  
  struct Table tb;
  init_table(&tb, id);
  puts("init OK");
  run_test(&tb);
  
  puts("run finished");
  return 0;
}
