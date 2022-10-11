#include "mica.h"


/*
void * read_testfile(char *name)
{
  size_t len = TEST_FILE_SIZE;
  int fd = open(name, O_RDONLY);
  if(fd < 0){puts("can't open"); exit(1);}
  char* buf = (char *)mmap(0, len, PROT_READ | PROT_WRITE | 0x40000, MAP_PRIVATE, fd, 0);
  if(buf == MAP_FAILED){puts("map faild"); exit(1);}
  close(fd);
  for(size_t i = 0; i < len; i++) {// do nothing but load file
    char t = buf[i];
    buf[i] = buf[i^1];
    buf[i^1] = t;
  }
  return buf;
}*/
volatile ull *outport;


inline static ull myrand(ull *seed){
  ull x = *seed;	
  x ^= x >> 12; // a
  x ^= x << 25; // b
  x ^= x >> 27; // c
  *seed = x;
  return x * 0x2545F4914F6CDD1D;
}

void insert_one(struct Table *kv, struct Op *op) 
{
  int i;
  uint bkt = op->key.bkt & kv->bkt_mask;
  struct Bucket *bkt_ptr = & kv->index[bkt];
  uint tag = op->key.tag;
  
  kv->nput ++;
  
  int eid = -1;
  for(i = 0; i < 8; i++) {
    if(bkt_ptr->entry[i].tag == tag || bkt_ptr->entry[i].in_use == 0) {
      eid = i;
    }
  }
  
  if(eid == -1) {
    eid = tag & 7;
    kv -> nevict++;
  }
  
  struct Entry * e = &bkt_ptr->entry[eid];
  e -> in_use = 1;
  e -> offset = kv -> loghead;
  e -> tag = tag;
  //printf("%d\n",e -> tag);
  
  char * log_ptr = &kv->log[kv->loghead & kv->logmask];
  
  ll len_to_copy = sizeof(struct Key) + sizeof(char)*2 + op->val_len;
  
  ll len_takeup = (len_to_copy + 7) & ~7;
  
  if(unlikely(kv->logsize - (kv->loghead & kv->logmask) < len_takeup)) {
    //printf("%lld %lld %lld\n", kv->logsize, kv->loghead , kv->logmask);
    kv->loghead = (kv->loghead + kv->logsize) & ~kv->logmask;
    puts("wrap around");
  }
  
  memcpy(log_ptr, op, len_to_copy);
  kv->loghead += len_takeup;
  //kv->loghead = (kv->loghead + 7) & ~7;
  
  /*
  if(unlikely(kv->logsize - (kv->loghead & kv->logmask) <= 32ll)) {
    kv -> loghead = (kv->loghead + kv->logsize) & ~kv->logmask;
    puts("wrap around");
  }*/
}

void run(struct Table *kv, struct Op **op, int n, struct Resp *resp)
{
  uint bkt[MAX_BATCH_SIZE];
  struct Bucket *bkt_ptr[MAX_BATCH_SIZE];
  uint tag[MAX_BATCH_SIZE];
  int key_in_store[MAX_BATCH_SIZE];
  struct Op *kv_ptr[MAX_BATCH_SIZE];
  
  int i, j;
  
  for(i = 0; i < n; i++) {
    bkt[i] = op[i]->key.bkt & kv->bkt_mask;
    bkt_ptr[i] = &kv->index[bkt[i]];
    __builtin_prefetch(bkt_ptr[i], 0, 0);
    tag[i] = op[i]->key.tag;
    
    key_in_store[i] = 0;
    kv_ptr[i] = NULL;
  }
  
  for(i = 0; i < n; i++) {
    for(j = 0; j < kv->nentry; j++) {
      //printf("%d %d\n", tag[i], bkt_ptr[i]->entry[j].tag);
      if(bkt_ptr[i]->entry[j].in_use == 1 &&
         bkt_ptr[i]->entry[j].tag == tag[i]) {
         
         ll log_offset = bkt_ptr[i]->entry[j].offset & kv->logmask;
         
         kv_ptr[i] = (struct Op*)&kv->log[log_offset];
         
         __builtin_prefetch(kv_ptr[i], 0, 0);
         __builtin_prefetch((char*)kv_ptr[i] + 64, 0, 0);
         
         if(kv->loghead - bkt_ptr[i]->entry[j].offset >= kv->logsize)
           kv_ptr[i] = NULL;
         
         break;
      }
    }
    // += kv_ptr[i] == NULL;
  }
  for(i = 0; i < n; i++) {
    if(kv_ptr[i] != NULL) {
      ll* key_ptr_log = (ll*) kv_ptr[i];
      ll* key_ptr_req = (ll*) op[i];
      if(key_ptr_log[0] == key_ptr_req[0] &&
         key_ptr_log[1] == key_ptr_req[1]) {
         
       
         
        key_in_store[i] = 1;
        
        if(op[i]->opcode == OP_GET) {
          kv -> nget_success++;
          //puts("GS");
          
          resp[i].type = RESP_GET_SUCCESS;
          resp[i].val_len = kv_ptr[i]->val_len;
          resp[i].val_ptr = kv_ptr[i]->value;
        } else {
          kv -> nput ++;
          
          assert(op[i]->val_len == kv_ptr[i]->val_len);
          memcpy(kv_ptr[i]->value, op[i]->value, kv_ptr[i]->val_len);
          
          resp[i].type = RESP_PUT;
          resp[i].val_len = 0;
          resp[i].val_ptr = NULL;
        }
      }
    }
    
    if(key_in_store[i] == 0) {
      if(op[i]->opcode == OP_GET) {
        kv -> nget_failed ++;
         
        resp[i].type = RESP_GET_FAILED;
        resp[i].val_len = 0;
        resp[i].val_ptr = NULL;
      } else{
        insert_one(kv, op[i]);
        
        resp[i].type = RESP_PUT;
        resp[i].val_len = 0;
        resp[i].val_ptr = NULL;
      }
    }
  }
}

void run_test(struct Table *kv)
{
  struct Op op[MAX_BATCH_SIZE];
  struct Op *op_ptr[MAX_BATCH_SIZE];
  struct Resp resp[MAX_BATCH_SIZE];
  
  const int printcnt = NBUCKET * NENTRY * 0.8;
  int start_get = 0;
  time_t tm = clock();
  ull original_seed = (19190817 << kv->id) ^ 114514;
  ull seed = original_seed;
  
  for(int i = 0; i < MAX_BATCH_SIZE; i++) op_ptr[i] = op + i;
  
  while(1) {
    for(int i = 0; i < MAX_BATCH_SIZE; i++) { // 16key + 16val
      ((ll*)&op[i])[0] = myrand(&seed);
      ((ll*)&op[i])[1] = myrand(&seed);
      if(start_get) {
        op[i].opcode = OP_GET;
      }
      else {
        op[i].opcode = OP_PUT;
        op[i].val_len = 16;
        memcpy(op[i].value, &op[i], 16);
      }
    }
    
    run(kv, op_ptr, 32, resp);
    
    
    if(kv->nput + kv->nget_success + kv->nget_failed >= printcnt){
    
      printf("sid: %d\npid: %d\n", kv->id, getpid());
      
      printf("gets %d, getf %d, put %d, index evict %d\n", 
        kv->nget_success, kv->nget_failed, kv->nput, kv->nevict);
    
      if(!start_get) 
        printf("index %.1lf%%\n", 
          1.0*(kv->nput-kv->nevict)/(NBUCKET*NENTRY)*1e2);
    
      kv->nget_success = 0;
      kv->nget_failed = 0;
      kv->nput = 0;
      kv->nevict = 0;
    
      start_get = 1;
    
      seed = original_seed;
    
      time_t nw = clock();
      double deltaT = 1.0*(nw-tm)/CLOCKS_PER_SEC;
      tm = nw;
      printf("%.1lf MOPS\n", printcnt/deltaT/1e6);
    }
  }
  
}
