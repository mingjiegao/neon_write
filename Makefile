# 手动编译（无需 cmake）:
#   make -f Makefile
#
# 或者直接:
#   g++ -std=c++17 -Wall -Wextra -g -I. -o test_neon_write test_main.cpp

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -g -I.
TARGET = test_neon_write
SRCS = test_main.cpp

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET)

test: $(TARGET)
	./$(TARGET)

.PHONY: all clean test
