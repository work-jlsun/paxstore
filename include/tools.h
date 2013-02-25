/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: all kinds of tool functions 
*/

#ifndef __TOOLS__
#define __TOOLS__
#include "kvproposal.h"
#include <zookeeper/zookeeper.h>

static void global_watcher(zhandle_t *, int type, int state, const char *path,void*v){	
      if (state == ZOO_CONNECTED_STATE){
	  	LOG(HCK,("ZOO_CONNECTED_STATE\n"));
      }else{
      		LOG(HCK,("state:%d\n",state));	
       }
      LOG(HCK, ("i am global trigger, look at me\n"));	
      /*add-code-here*/
}

void PrintZooKeeperError(int error);
void PrintZKCallBackErrors(zhandle_t *zh, int type, int state, const char *path,void* ctx);
U64 NowMicro();

class PassArg{
public:
	PassArg():first_(NULL),second_(NULL){
	}	
	PassArg(void* fst, void*sec){
		first_ = fst;
		second_ = sec;
	}
	virtual ~PassArg(){
	}
	void Init(void*fst, void*sec){
		first_ = fst;
		second_ = sec;
	}
	void* first(void){
		return first_;
  	}
	void * second(void){
		return second_;
	}
private:	
	void* first_;
	void *second_;	
};

class SocketPair{
public:
	SocketPair();
	virtual ~SocketPair();
	int GetReadSock(void){
		return fd_[1];
	}
	int GetWriteSock(void){
		return fd_[0];
	}
	void SendMessage(void){
		send(fd_[0],"a", 1,0);
		LOG(VRB,("\n"));
	}
	void  GetMessage(void){
		int tmp;
		recv(fd_[1],&tmp,1, 0);
	}
	void SendMessageOk(void){
		U8 value = 1;
		send(fd_[0], &value, 1,0);
	}
	void SendMessageErr(void){
		U8 value = 0;
		send(fd_[0], &value, 1,0);
	}
	bool  GetMessageOk(void){
		U8 value;
				int recvv;
		recvv = recv(fd_[1],&value,1 , 0);
				LOG(HCK,("RECVV:%d---------------------value:%c     value:%u\n",recvv,value,value));
		if(value == 1){
			return true;
		}else{
			return false;
		}
	}
	/*send message in the same machine, need not hton and ntoh*/
	void SendValU64(U64 val){
		send(fd_[0], &val, sizeof(val), 0);
	}
	U64 GetValU64(void){
		U64 val;
		recv(fd_[1], &val, sizeof(val), 0);
		return val;
	}
	bool IfTriggered(void){
		return triggered_;
	}
	void SetTriggered(void){
		triggered_ = true;
	}

	void ClearTrigger(void){
		triggered_ = false;
	}

	bool IfCmtShouldSync(void){
		if (0 == __sync_fetch_and_add(&is_cmt_synced_, 0)){
			return true;
		}else{
			return false;
		}
	}
	void SetCmtShouldSync(void){
		//__sync_fetch_and_add(&is_cmt_synced_, 1);
		//here do not need the lock ,because IfCmtSynced and ClearCmtSynced
		//have been ensure here will have been no race condition
		is_cmt_synced_ = 1;
	}
	void ClearCmtShouldSync(void){
		__sync_fetch_and_add(&is_cmt_synced_, -1);
	}
private:
	int8_t  is_cmt_synced_;
	bool triggered_;
	int fd_[2];
	DISALLOW_COPY_AND_ASSIGN(SocketPair);	
};

#define MAXSLEEP 16
int ConnectRetry(int,  struct sockaddr *, socklen_t );
ssize_t Readn(int sfd, char *buf, ssize_t len );
ssize_t  ReadvRec(int sfd, char *buf, U32 len, int &errorcode);

U64  Htonl64(U64   host) ;
U64  Ntohl64(U64   net);

bool ListenerInit(int &listenfd, int port);
bool TcpSend(int sfd, char* buf, U32 size);
void DisableSigPipe(void);
#endif
