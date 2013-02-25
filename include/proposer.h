/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: proposal thread
*/

#ifndef __PROPOSER__
#define  __PROPOSER__
#include <event.h>

#include "kvproposal.h"

extern class SocketPair     pwsockpair;  //the sock pair between  proposer and logwriter
extern class SocketPair     cmtprosockpair;  // the sock pair between proposer and CommitSyncer
extern  class ProposerAndCatchuperMsger pcmsger;
	
class ProposerAndCatchuperMsger{
public:
	ProposerAndCatchuperMsger(){
		stopflag_ = 0;
	}
	virtual ~ProposerAndCatchuperMsger(){};
	bool  TestStopFlag(void){
		U8 tmp = __sync_fetch_and_add(&stopflag_,0);
		if(   tmp == 0){
			return false;
		}else if (tmp > 0){
			__sync_fetch_and_add(&stopflag_, -1);
			return true;
		}else{
			assert(0);
		}
	}
	void  SetStopFlag(void){
		__sync_fetch_and_add(&stopflag_,1);		
	}	
private:
	U8 stopflag_;             //  >0 , stop send  0: not stop
	DISALLOW_COPY_AND_ASSIGN(ProposerAndCatchuperMsger);
};

/*the proposer  worker*/
#define PROPOSER_SLEEP_TIME		3	// the proposal thread wait time for follower to catchup	
class ProposerWorker{
friend class FollowerList;
friend void ClientMsgHandle(int, short, void *);
public:
	ProposerWorker();
	~ProposerWorker();
	void Start(void);
private:					
	struct event_base *base_; 
	struct event 	client_msg_event_;
	struct event     logwriter_msg_event_;
	DISALLOW_COPY_AND_ASSIGN(ProposerWorker);
};
void ClientMsgHandle(int, short, void *);
#endif
