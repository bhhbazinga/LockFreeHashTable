CXX = g++
CXXFLAGS = -Wall -Wextra -pedantic -std=c++2a -g -fsanitize=address

EXEC = test

all: $(EXEC)

$(EXEC):  test.cc lockfree_hashtable.h
	$(CXX) $(CXXFLAGS) test.cc -o $@  

clean:
	rm -rf  $(EXEC)