LIST=localtest_server test1 test2 test3 selftest
WARNINGS=-Wall -Wextra -Wshadow -Wfatal-errors

all: $(LIST)

TEST_CLIENT_H=main.h global.h test.h
TEST_CLIENT_CPP=testutils.cpp
DEBUG_OR_OPT=-g3

test%: test%.cpp $(TEST_CLIENT_H) $(TEST_CLIENT_CPP)
	g++ -o $@ $< $(TEST_CLIENT_CPP) $(DEBUG_OR_OPT) $(WARNINGS)

TEST_SERVER_H=allocator.h main.h global.h test.h 
TEST_SERVER_CPP=kv.cpp allocator.cpp testutils.cpp 

%: %.cpp $(TEST_SERVER_H) $(TEST_SERVER_CPP)
	g++ -o $@ $< $(TEST_SERVER_CPP) $(DEBUG_OR_OPT) $(WARNINGS)

test_allocator: test_allocator.cpp allocator.cpp allocator.h
	g++ test_allocator.cpp allocator.cpp -o test_allocator

.PHONY: hugepage
hugepage:
	./config_hugepage.sh

.PHONY: clean	
clean:
	rm $(LIST)
