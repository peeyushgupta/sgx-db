CC = $(TOOLPREFIX)g++
AS = $(TOOLPREFIX)gas
LD = $(TOOLPREFIX)ld
OBJCOPY = $(TOOLPREFIX)objcopy
OBJDUMP = $(TOOLPREFIX)objdump

CFLAGS :=-std=c++11 -Wall -g -D_GNU_SOURCE -pthread -lm -fno-pic -O2 
#CFLAGS += -DVERBOSE_DEBUG -DVERBOSE_ASSERT
#CFLAGS += -DNDEBUG

INCLUDES := -I. 

SRCS = \
	test.c\
	awe_mapper.c\
	test.c\

TEST_SRCS = \
	test.c\


OBJS = $(SRCS:.c=.o)
TEST_OBJS = $(TEST_SRCS:.c=.o)

TESTS= tests
MAIN = db 


.PHONY: depend clean

#all:    $(MAIN) $(TESTS)
all:    $(TESTS)

$(TESTS): $(TEST_OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TESTS) $(TEST_OBJS) $(LFLAGS) $(LIBS)


$(MAIN): $(OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -o $(MAIN) $(OBJS) $(LFLAGS) $(LIBS)

.cpp.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	rm -f *.o *~ $(MAIN) $(TESTS)

depend: $(SRCS) $(TEST_SRCS)
	makedepend $(INCLUDES) $^


