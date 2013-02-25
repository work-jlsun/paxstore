##################################################################
CC = g++
CFLAGS   +=  -O0  -g  -Wall  -I./include  
#LDIR =  -L/usr/local/BerkeleyDB.5.2/lib/
LINK = -levent -lleveldb  -lpthread  -lzookeeper_mt

object3 = client.o tools.o random.o rangetable.o cfg.o cfgfile.o

object7 = leaderelection.o  log_manager.o logicaltruncatedtable.o 	tools.o \
		  logworker.o pqueue.o proposer.o valuelist.o  followerlist.o libevent_tcp_server.o \
		  leaderfailover.o zkcontrol.o commitsyncworker.o leaderthreads.o follower.o leadercatchup.o \
		  frecover.o log_writer.o file_manager.o log_reader.o log_encode.o metalogstore.o data_store.o serverread.o \
		  rangetable.o cfg.o cfgfile.o

myobject = leaderthreads.o pqueue.o valuelist.o follower.o tools.o libevent_tcp_server.o  proposer.o \
           logworker.o followerlist.o  client.o \
           frecover.o leadercatchup.o commitsyncworker.o logicaltruncatedtable.o leaderelection.o  leaderfailover.o\
		   zkcontrol.o log_manager.o log_writer.o file_manager.o log_reader.o log_encode.o metalogstore.o data_store.o serverread.o random.o \
		   rangetable.o cfg.o cfgfile.o

sources =  leaderthreads.cc pqueue.cc valuelist.cc tools.cc libevent_tcp_server.cc  proposer.cc \
              logworker.cc followerlist.cc follower.cc client.cc \
              frecover.cc leadercatchup.cc commitsyncworker.cc logicaltruncatedtable.cc  leaderelection.cc \
              leaderfailover.cc zkcontrol.cc log_manager.cc log_writer.cc file_manager.cc log_reader.cc log_encode.cc\
              metalogstore.cc data_store.cc serverread.cc random.cc rangetable.cc cfg.cc cfgfile.cc
##################################################################
all: leaderelectiontests  clientstart 

clientstart:  $(object3)
	$(CC)   $(LDIR)  $(LINK)  -o  $@  $^

leaderelectiontests: $(object7)
#	$(CC)   $(LDIR)  $(LINK)  -o  $@  $^
	$(CC)   -o  $@  $^ $(LINK)
	
$(myobject):%.o: %.cc
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm leaderelectiontests clientstart *.o
##################################################################
#include $(sources:.cc=.d)	

%.d: %.cc
	set -e; rm -f $@; \
	$(CC) -MM $(CFLAGS) $< >; $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$	
