/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: leader reads the data that follower catchup request and send back to follower
*/

#ifndef  __LEADERCATCHUP__
#define   __LEADERCATCHUP__
#include <event.h>	

#include "kvproposal.h"
#include "cfg.h"

extern bool ifsendfollowerlist;	// to tell leaderread thread to send leader's followerlist ip to client

class InMemLogicalTable;

//#define CATCHUP_RETURN_ACK_SIZE   MAX_TCP_MSG_SIZE * 3    //max three times bigger 
// #define CATCHUP_ITEM_BUF_SIZE  MAX_READWRITE_BUFSIZE
#define  LEADER_WAIT_NUM  10 //leader wait when catchup closer
#define LEADR_WAIT_CATCHUP_SUCCESS 3 //when send endflag to follower,  wait follower receive and trange the next state

#define CATCHUP_ITEM_BUF_SIZE  MAX_READWRITE_BUFSIZE
class  CatchupItem{
public:
	U8 ifsynced;
	ProposalNum pn;
	U32 key_len;
	U32 value_len;	
	//char buf[CATCHUP_ITEM_BUF_SIZE];
	KeyValue kv;
	U32 citem_encode_size(void){
		U32 total_size = sizeof(ifsynced) +
					    sizeof(pn.epoch) +
					    sizeof(pn.sequence) +
					    sizeof(key_len)	+
					    sizeof(value_len)	+
					    key_len +
					    value_len;
		return total_size;
	}
};

void LeaderCatchupOnAccept(int fd, short ev, void * arg);
void LeaderCatchupOnRead(int fd, short ev, void * arg);

class LeaderCatchup{	
friend void LeaderCatchupOnAccept(int fd, short ev, void * arg);
friend void LeaderCatchupOnRead(int fd, short ev, void * arg);
public:
	LeaderCatchup();
	~LeaderCatchup();
	bool Init(void);
	bool Start(void);	
	int listenfd(void){
		return listenfd_;	
	}
private:
	bool DecodeCatchupMessage(char *buf, U64 &catchup_sequence);
	bool  EncodeCatchupAckHeader(char *buf, U32 total_len,  U32 proposal_numbers, U8 endflag);
	bool EncodeCatchupAckBody(char *buf, class CatchupItem *citem, U32 &remain_size, U32 &use_size);
	U32 EncodeHeaderSize(void);
	bool SendCatchupMessageAck(U64 catchup_sequence, char* catch_buf, U32 len, ClientEvent * ce);
	bool SendCatchupEndflag(char *catch_buf, U64 catchup_sequence, ClientEvent *ce);
	//bool GetProposalBySequence(U64 seq,  class  CatchupItem *citem);
	bool GetProposalBySequence(U64 seq,  class  CatchupItem *citem,int fd);	// 该函数修改过，增加了fd字段

	U64 GetEpochFromMetafile(U64 seq);
	int listenfd_;
	int port_; //catchup listen port
	struct event_base *base_;
	struct event  ev_accept_;
	class CatchupItem *citem_;
	char catch_up_ack_buf_[CATCHUP_RETURN_ACK_SIZE];
	class InMemLogicalTable  *logical_table;
};

U64 GetEpochFromMetafile(U64 seq);
#endif 
