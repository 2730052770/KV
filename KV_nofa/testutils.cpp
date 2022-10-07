#include "kvp.h"
#include "main.h"
#include "test.h"
#include <time.h>
#include <string>
using namespace std;

uint STL_randuint() {
	return rand() << 16 ^ rand();
}

ull STL_randull() {
	return (ull)STL_randuint() << 32 ^ STL_randuint();
}

ull fastrand(ull *seed){
	ull x = *seed;
	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	*seed = x;
	return x * 0x2545F4914F6CDD1D;
}

string gen_str(int l, int r) 
{
	string s;
	int len = rand()%(r-l+1)+l;
	s.resize(len);
	for(int i = 0; i < len; i++)
		s[i] = (char)(rand()%10) + '0';
	return s;
}

