#ifndef KVP_H
#define KVP_H

#include "global.h"

struct KV{// 3
	us len_value;
	uc len_key; 
	char content[];// key + value
}__attribute__((packed));


#define KV_SIZE(_key_len,_value_len) (sizeof(KV)+(_key_len)+(_value_len))
#define MY_BLOCK_SIZE(_key_len,_value_len) (KV_SIZE((_key_len), (_value_len))+sizeof(Block)-sizeof(KV))


#endif
