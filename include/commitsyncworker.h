/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#ifndef __COMMITSYNCWORKER__
#define __COMMITSYNCWORKER__
#include <sys/time.h>
#include <assert.h>

#include <event.h>

#include "cfg.h"
#include "tools.h"
#include "pqueue.h"
#include "followerlist.h"


#define COMMIT_UNSYNED_NUM  100
#define COMMIT_TIMEOUT_SECS	30
#define COMMIT_MESSAGE_SIZE 24
void CommitSyncerCb(int sfd, short event, void* args);
void TimeoutCb(int sfd, short event, void* args);

class CommitSyncer{
friend void CommitSyncerCb(int sfd, short event, void* args);
friend void TimeoutCb(int sfd, short event, void* args);
public:
	CommitSyncer();
	~CommitSyncer();
	void Start(void);
	bool EncodeCommitSyncMessage(char* buf, ProposalNum cmtpn, U32 &size);
	bool SendCmtSequence(U64 cmt_sequence);
	void UpdateLocalCmtSequence(U64 cmt_sequence);
	void SetLastSyncTime(void){
		gettimeofday(&last_sync_time_, NULL);
	}
	bool IfTimeOuted(void){
		struct timeval  tval;
		gettimeofday(&tval, NULL);
		if ( (tval.tv_sec - last_sync_time_.tv_sec) >= COMMIT_TIMEOUT_SECS){ // this is about 30 seconds
			return true;		
		}else{
			return false;
		}
	}
	/*
	*	
	*		
	*/
	void ResetTimeOutEvent(void){
		struct timeval  tval;
		struct timeval tval2;
		struct timeval dval;
		gettimeofday(&tval, NULL);	
		
		tval2.tv_sec = last_sync_time_.tv_sec + COMMIT_TIMEOUT_SECS; // this is the next wake times
		
		if ( (tval2.tv_sec - tval.tv_sec) >= 1 ){
			dval.tv_sec = tval2.tv_sec - tval.tv_sec;   // add one is because , ensure that it is timouted
			dval.tv_usec = 0;
		}else{
			dval.tv_sec = 1;
			dval.tv_usec = 0;
		}
		event_add(&timeout_event_,&dval);
		return;
	}
private:	
	bool _SendCmtSequence(class FollowerInfo *fi,   U32 size);
	struct timeval last_sync_time_; 
	struct event_base *base_;
	struct event  commit_num_event_;
	struct event timeout_event_;
	char  sbuf_[COMMIT_MESSAGE_SIZE];
	DISALLOW_COPY_AND_ASSIGN(CommitSyncer);
};
#endif
