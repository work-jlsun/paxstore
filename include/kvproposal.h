/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: structures used in this project
*/

#ifndef __KVPROPOSAL__
#define  __KVPROPOSAL__
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <assert.h>

#include <event.h>
#include <pthread.h>

#include "cfg.h"

typedef unsigned long long  U64;
typedef unsigned int  U32;
typedef unsigned char U8;

struct FileMetaData {
  U32 refs;
  U64 number;
  U64 file_size;         // File size in bytes
  U64 smallest;       // Smallest internal key served by table
  U64 largest;        // Largest internal key served by table
  U64 epoch;

  FileMetaData() : refs(0), file_size(0) { }
};

typedef struct KeyValue{
        char* key;
        char* value;
	KeyValue(){
		key = NULL;
		value = NULL;
	}
}KeyValue;

typedef struct LeaderValue{
	U32 leader_id;
	char leader_ip[20];		
}LeaderValue;

class ClientEvent{
public:
	int fd_;				//the fd for read and write
	struct sockaddr_in client_addr_;	 //the net address
	struct event ev_read_;		//the read event
	pthread_mutex_t  mutex_;
	bool   active_;

	ClientEvent(int sfd, struct sockaddr_in *ca){    //pin
		if ( pthread_mutex_init(&mutex_, NULL)  != 0){
			assert(0);
		}
		fd_ = sfd;
		client_addr_ = *ca;
		active_ = true;	
		LOG(HCK,("in ce new\n"));
	}
	virtual ~ClientEvent(){
		if ( pthread_mutex_destroy(&mutex_)  != 0){
			assert(0);
		}	
	}
	void lock(void){
		pthread_mutex_lock(&mutex_);
	}
	void unlock(void){
		pthread_mutex_unlock(&mutex_);
	}
};

 class ClientKeyValue{
public:	
	class ClientEvent *ce;	//usd to send back the message
        KeyValue kv;			// will call the KeyValue default constructor
	ClientKeyValue(void){
		// do nothing;
	}
	virtual   ~ClientKeyValue(void){
		//LOG(VRB, ("\n"));
		//do nothing, the pointed data will not delete by it
	}
};

typedef struct ProposalNum{
        U64 epoch;
        U64 sequence;
}ProposalNum;

typedef struct proposal{
        ProposalNum pn;
        class ClientKeyValue kv;
	proposal(){
		pn.epoch = pn.sequence = 0;
		// will call the default constructor of    ClientKeyValue
	}
	void notifyclient(void){
		 char ret = COMMITED;
		 kv.ce->lock();
		if ( kv.ce->active_ == true ){		//fixme , it need to be thread safe
			int sendret;
			sendret = send(kv.ce->fd_, &(ret), sizeof(ret), 0);		//fixme if the client send "close" before i send it , will it be send ok?
			LOG(HCK,("notify client ok. sendret:%d\n",sendret ));
			kv.ce->unlock();
			return;
		}else{		 
			LOG(HCK,("client disconnected\n"));
			kv.ce->unlock();
			delete  kv.ce;    // destructor the ce
			return;
		}				
	}
}proposal; 

typedef struct fproposal{
	ProposalNum pn;
	KeyValue kv;
}fproposal;

#define MAX_PRESPONSE_MSG_SIZE 	48    //(total_len + fid + fepoch + sequnce)
typedef struct ProposalResponseInfo{
	U32 followerid_;
	U64 epoch_;
	U64 sequence_;
	void Init( U32 fid, U64 fepoch, U64 fsequence){
		followerid_ = fid;
		epoch_ = fepoch;
		sequence_ = fsequence;
	}
}ProposalResponseInfo;
#endif
