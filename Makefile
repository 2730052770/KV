all: main test1 test2 test3

test: debug_main test1 test2 test3

debug: debug_main debug_test1 debug_test2 debug_test3

debug_test3: test3.cpp main.h test.h testutils.cpp
	g++ -o test3 test3.cpp testutils.cpp -g3 -Wfatal-errors -Wall -Wextra -Wshadow

debug_test2: test2.cpp main.h test.h testutils.cpp
	g++ -o test2 test2.cpp testutils.cpp -g3 -Wfatal-errors -Wall -Wextra -Wshadow

debug_test1: test1.cpp main.h test.h testutils.cpp
	g++ -o test1 test1.cpp testutils.cpp -g3 -Wfatal-errors -Wall -Wextra -Wshadow

debug_main: kv.cpp allocator.cpp main.h test.h testutils.cpp
	g++ -o main kv.cpp allocator.cpp testutils.cpp -g3 -Wfatal-errors -Wall -Wextra -Wshadow

test3: test3.cpp main.h test.h testutils.cpp
	g++ -o test3 test3.cpp testutils.cpp -O3 -Wfatal-errors -Wall -Wextra -Wshadow

test2: test2.cpp main.h test.h testutils.cpp
	g++ -o test2 test2.cpp testutils.cpp -O3 -Wfatal-errors -Wall -Wextra -Wshadow

test1: test1.cpp main.h test.h testutils.cpp
	g++ -o test1 test1.cpp testutils.cpp -O3 -Wfatal-errors -Wall -Wextra -Wshadow

main: kv.cpp allocator.cpp main.h test.h testutils.cpp
	g++ -o main kv.cpp allocator.cpp testutils.cpp -O3 -Wfatal-errors -Wall -Wextra -Wshadow

.PHONY: clean	
clean:
	rm main test1 test2 test3
