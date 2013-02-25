/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#include "leaderthreads.h"

#include <pthread.h>
#include "proposer.h"
#include "libevent_tcp_server.h"
#include "logworker.h"
#include "leadercatchup.h"
#include "commitsyncworker.h"
#include "serverread.h"
#include "cfg.h"

void* tcpserver_thread(void * arg){		// receive the client's write request
	TcpServer server;
	DisableSigPipe();
	LOG(HCK,("start tcpserver  thread\n"));
	server.Start();
	LOG(HCK,("tcp server event loop exit\n"));
	return NULL;

}
void* proposer_thread(void * arg){		// send proposal message to follower
	ProposerWorker  pw;
	DisableSigPipe();
	LOG(HCK,("start proposer  thread\n"));	
	pw.Start();
	LOG(HCK,( "proposer event loop exit\n"));		
	return NULL;
}

void* logwriter_thread(void *arg){		// write the leader write request into log
	log_worker lw;
	DisableSigPipe();
	LOG(HCK,("start log_worker  thread\n"));			
	lw.start();
	LOG(HCK,("log_worker event loop exit\n"));		
	return NULL;
}


void leadercatchup_cleanup_thread(void * arg){
	int fd =   *(int *)arg;
	close(fd);
	LOG(HCK,("cleanup\n"));
}



void* leadercatchup_thread(void *arg){		// read the catch up request and send the result to followers
	LeaderCatchup lc;
	DisableSigPipe();
	if (!lc.Init()){
		LOG(HCK,("LeaderCatchup Init error\n"));				
		assert(0);
	}
	int listen_fd = lc.listenfd();
	pthread_cleanup_push( leadercatchup_cleanup_thread, (void *)&listen_fd);
	LOG(HCK,("start LeaderCatchup_thread  thread\n"));				
	lc.Start();
	LOG(HCK,("LeaderCatchup  event loop exit\n"));				
	pthread_cleanup_pop(0);  
	
	return NULL;
}

void* commitsync_thread(void *arg){		// send the cmt message to followers
	 CommitSyncer cs;
	 DisableSigPipe();
	 LOG(HCK,("start commitsync_thread  thread\n"));	 
	 cs.Start();
	 LOG(HCK,( "commitsync_thread  event loop exit\n"));	 
	 
}

void* read_thread(void *arg){		// read the data from leveldb and send it to client
	 ServerRead lr;
	 if (!lr.Init(true)){
		LOG(HCK,("ServerRead init error\n"));
		assert(0);
	}
	 DisableSigPipe();
	 LOG(HCK,("start read_thread  thread\n"));	 
	 lr.Start();
	 LOG(HCK,( "read_thread  event loop exit\n"));	 
	 return NULL; 
}
