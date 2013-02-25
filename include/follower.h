/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: follower receive proposal messages from leader and send back ack
  *            : receive cmt message and write data into storage engine
  *		   : receive read request and read data from storage and send it to client
*/

#ifndef PAXSTORE_FOLLOWER_
#define PAXSTORE_FOLLOWER_
#include <stdlib.h>
#include <string.h>
#include <event.h>

#include <kvproposal.h>
#include "log_manager.h"
#include "tools.h"
#include "cfg.h"
#include "logicaltruncatedtable.h"

#define MY_RANGE_NUM 1
extern char FollowerReceiveBuffer[MAX_TCP_MSG_SIZE];
extern class LogManager logstore; // the name of this is same as logger worker
extern class FState flstates;
extern class FProposalQueue GlobalFproposal;


class FState{
public:
	FState(U64 fepoch, U64 cmt, U64 lsn,U32 rn):
		epoch_(fepoch),lsn_sequence_(lsn),cmt_sequence_(cmt),rangenum_(rn)
		{};
	FState():epoch_(0),lsn_sequence_(0),cmt_sequence_(0),rangenum_(0){};
	virtual ~FState(){};
	
	U64 epoch(){
		return epoch_;
	}
	/*
	bool set_epoch(U64  nepoch){    //fix me 
		if( nepoch == (epoch_ + 1)){
			epoch_ = nepoch;
			return true;
		}else if (nepoch < epoch_){
			assert(0);
		}else{
			return false;
		}
	}
	*/
	//change the protocol , now we can set the epoch with gap in follower
	bool set_epoch(U64  nepoch){    //fix me 
		if (nepoch < epoch_){
			assert(0);
			return false;
		}else{
			epoch_ = nepoch;		
			return true;
		}
	}
	
	U64 lsn_sequence(){ return lsn_sequence_; }
	void inc_lsn_sequence(void){  lsn_sequence_++; }	
	U64 cmt_sequece(void){return cmt_sequence_;}
	void update_cmt_sequence(U64 cq){ cmt_sequence_ = cq;}
	U32 rangenum(void){
		return rangenum_;	
	}
	bool Init(U64 ep, U64 lsn, U64 cmt, U32 rg){
		if(lsn < cmt){
			return false;
		}
		epoch_ = ep;
		lsn_sequence_ = lsn;
		cmt_sequence_ = cmt;
		rangenum_ = rg;		
		return true;
	}
private:
	U64 epoch_;
	U64 lsn_sequence_;
	U64 cmt_sequence_;
	U32 rangenum_;      // the range num is the key_range_num
};

#define FPROPOSAL_QUEUE_SIZE 1024

class FProposalQueue{
public:
	FProposalQueue();
	virtual ~FProposalQueue();
	void AllClear(void){}	//no needed ,because the QProposal has it's own construct and destruct	
	unsigned int GetFproposalOffset(U64);
	fproposal * GetFproposal(U64 sequence){
			unsigned int  index = GetFproposalOffset(sequence);
			return &(fpr_[index]);
	}
	bool SetFproposal(U64 sequence,fproposal fpr){
		unsigned int  index = GetFproposalOffset(sequence);
		fpr_[index].pn.epoch = fpr.pn.epoch;
		fpr_[index].pn.sequence = fpr.pn.sequence;
		U32 key_len,value_len;
		key_len = strlen(fpr.kv.key);
		value_len = strlen(fpr.kv.value);
		fpr_[index].kv.key = (char *)malloc(key_len+1) ;
		fpr_[index].kv.value = (char *)malloc(value_len+1);
		memset(fpr_[index].kv.key,0,key_len+1);
		memset(fpr_[index].kv.value,0,value_len+1);
		memcpy(fpr_[index].kv.key,fpr.kv.key,key_len);		
		memcpy(fpr_[index].kv.value,fpr.kv.value,value_len);
		return true;
	}
private:
	fproposal fpr_[FPROPOSAL_QUEUE_SIZE];
	pthread_rwlock_t	plock_;	//is is a user-producer lock,to protect the buffer
};

inline unsigned int FProposalQueue::GetFproposalOffset(U64 sequence){
	return sequence % FPROPOSAL_QUEUE_SIZE;
}

class ProposalEvent{
public:
	int fd;				//the fd for read and write
	struct sockaddr_in proposal_addr_;	 //the net address
	struct event ev_read_;		//the read event
	bool   active_;
	ProposalEvent(int sfd, struct sockaddr_in *ca){    //pin
		fd = sfd;
		proposal_addr_ = *ca;
		active_ = true;	
		LOG(HCK,("in pe new\n"));
	}
	virtual ~ProposalEvent(){}
private:
	DISALLOW_COPY_AND_ASSIGN(ProposalEvent);
};

void FollowerOnAccept(int, short, void*);
void FollowerOnRead(int, short, void*);
class Follower{
friend void FollowerOnAccept(int, short, void*);
friend void FollowerOnRead(int, short, void*);
public:
	Follower(U32 range_num);	
	Follower();
	virtual ~Follower();
	bool FListenInit();	
	bool Init(void);
	bool Start(void);			
	U32 range_num(void){
		return range_num_;
	}
	bool UpdateLogicalTable(U64 epoch, U64 sequence)
	{		
		if ( logical_table->UpdateLogicalTable( epoch,  sequence) ){
			return true;
		}else{
			return false;
		}
	}
	void close_listenfd(void){
		close(listenfd_);
	}
	int listenfd(void){
		return listenfd_;
	}

	bool WriteFpr(U64 begin_seq,U64 end_seq);
private:
	int range_num_;
	int listenfd_;
	int port_; //server listening port
	struct event_base *base_;
	struct event ev_accept_;
	class InMemLogicalTable *logical_table;
	DISALLOW_COPY_AND_ASSIGN(Follower);
};
U32 GetMsgType(char*);
bool DecodeProposal(char*,proposal &);
void DecodeCmtMsg(char * , ProposalNum& );
bool EncodeProposalResponse(ProposalResponseInfo &pr, U32 & size );
bool SendPreposalAck(struct fproposal &, int);
void FcmtMsgHandle(Follower * fl);
void FproposalMsgHandle(int sockfd, Follower *fl);

void* recovery_thread(void *arg);
void follower_cleanup_handler(void *p);
void* follower_thread(void *arg);
void* follower_read_thread(void * arg);
#endif
