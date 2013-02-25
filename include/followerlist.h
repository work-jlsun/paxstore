/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: manage leader state and send proposal to followers and send ack to client
*/

#ifndef __FOLLOWERLIST__
#define __FOLLOWERLIST__
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <vector>

#include <event.h>
#include <pthread.h>	

#include "kvproposal.h"

class ProposerWorker;
class FailOverItem;

extern  class FollowerList	flws;
extern char*  flist[2];
extern char proposal_send_buf[MAX_TCP_MSG_SIZE];
extern char proposal_receive_buf[MAX_TCP_MSG_SIZE];

/*the follower info*/
class FollowerInfo{
public:		
	FollowerInfo(){};
	~FollowerInfo();
	void Init(U32 fid, char *fip){
		id_  = fid;
		if ( !(ip_ = (char *)malloc(strlen(fip) + 1)) ){
			assert(0);
		}
		strcpy(ip_,fip);
		old_fd_ = new_fd_ = 0;
		state_ = NEWITEM;
		tryconnecttime = 0;
	}
	U32 id_;
	char *ip_;
	struct sockaddr_in  old_follower_addr_;
	struct sockaddr_in  new_follower_addr_;
	int tryconnecttime;		// tell this node that how many times we have connect to this follower and don't success.
	int old_fd_;
	int new_fd_;
	U32 state_;
	struct event fevent_;
};

/*the connect results*/
bool DecodeProposalResponse(char*, ProposalResponseInfo &);
void ProposalMessageHandle(int sfd, short event, void *arg);
void LogworkerMessageHandle(int sfd, short event, void *arg);
/*the followerclass*/
class FollowerList{
friend void ProposalMessageHandle(int sfd, short event, void *arg);
friend void LogworkerMessageHandle(int sfd, short event, void *arg);
public:
	typedef std::vector<class FollowerInfo*>::size_type   size_type;
	FollowerList();// this is just for test
	virtual ~FollowerList();
	class FollowerInfo* &operator[](U32 index){
		return fvec_[index];
	}
	class  FollowerInfo* GetFollowerInfo(size_type index){
		return fvec_[index];
	}
	
	void erase(U32 index){
		  delete  fvec_[index]; // delete the FollowerInfo
		  fvec_.erase((fvec_.begin() + index));	//delete the pointer in the vec
		  //num--; //decrease the number of FollowerInfo
	}
	
	size_type FollowerLiveNum(void){
		size_type live_num = 0;;
		std::vector<class FollowerInfo*>::const_iterator iter;	
		for (iter = fvec_.begin(); iter != fvec_.end(); iter++){
			if ( ( (*iter)->state_ == NEWITEM ) || ( (*iter)->state_ == CONNECTED) ){
				live_num++;
			}
		}
		return live_num;
	}
	bool IsNewItem(size_type i){
		if (fvec_[i]->state_ == NEWITEM){
			return true;
		}else{
			return false;
		}
	}
	void NewFollowerEnqueue(class FollowerInfo * finfo){
		std::vector<class FollowerInfo*>::const_iterator iter;	
		for (iter = fvec_.begin(); iter != fvec_.end(); iter++){
			if( 0 == strcmp(finfo->ip_,(*iter)->ip_) ){   //the same ip exist before
				if ( ( (*iter)->state_ == NEWITEM ) || ( (*iter)->state_ == CONNECTED) ){
					LOG(HCK,("this follower is live existed in the followerlist\n"));
					delete finfo;
					return;
				}
				
				if ( ( (*iter)->state_ == CLOSED ) || ( (*iter)->state_ == ZOOKEEPER_DELETED) ){
					LOG(HCK,("this follower is CLOSED  existed in the followerlist\n"));
				
					//(*iter)->state_ = NEWITEM;
					//return;
				}
			}
		}
		fvec_.push_back(finfo);
	}	
	size_type   FollowerNum(void){
		//return num;
		return fvec_.size();	
	}
	void LockFlist(void){
		pthread_mutex_lock(&list_mutex_);
	}
	void UnlockFlist(void){
		pthread_mutex_unlock(&list_mutex_);
	}
	short   TryConnect(U32 );
	short   TryConnectButNotNewConnection(U32 );
	bool   SendProposals(class QProposal *, class ProposerWorker *);
	bool   EncodeProposal(QProposal *  , U32 & );
	bool   EncodeProposal(FailOverItem *failover_item, U32 &);
	void LockCatchupMutex(void){
		pthread_mutex_lock(&catchup_mutex_);
	}
	void UnlockCatchupMutex(void){
		pthread_mutex_unlock(&catchup_mutex_);
	}

	void ClearOldFollowers(void){
		std::vector<class FollowerInfo*>::const_iterator iter;	
		for (iter = fvec_.begin(); iter != fvec_.end(); iter++){
			for (iter = fvec_.begin(); iter != fvec_.end(); iter++){
				delete (*iter);	
			}
		}
	}
	
private:
	bool SendProposal(class FollowerInfo * , U32);
	U32 num_;   //the number of followes
	std::vector<class FollowerInfo*>  fvec_;
	pthread_mutex_t list_mutex_;
	pthread_mutex_t catchup_mutex_;
}; 
#endif
