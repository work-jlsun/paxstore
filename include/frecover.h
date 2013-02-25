/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: to recovery data when follower start
*/

#ifndef __FRECOVER__
#define __FRECOVER__
#include <stdlib.h>
#include <string.h>

#include "cfg.h"
#include "kvproposal.h"
#include "msgformat.h"
#include "zkcontrol.h"
#include "logicaltruncatedtable.h"

extern char leaderip[16];
extern class SocketPair rfmsger; //the messange between the frecover and the follower

class FCatchupItem{
public:
	U8 ifcmted;
	ProposalNum pn;
	U32 key_len;
	U32 value_len;
	KeyValue 	     kv;		
};
/*
*maybe every have it's own 
*FRecovery differant by the range value
*/
class  FRecovery{
public:
	FRecovery():range_(-1),catch_sock_(0),catch_sequence_(1){
		logical_table_ = new InMemLogicalTable();  
		if(logical_table_ == NULL){
			assert(0);
			// i shoud consider if i shoud throw a exception
		}
	}
	~FRecovery(){
	}
	void InitArgsFromZooKeeper(class PassArgsToFollower *args){
		range_ = args->range_num_;
		strcpy(leader_ip_, args->leader_ip_);
	}
	void Start(void);
	bool FRecoveryInsertData(U64 begin_seq,U64 end_seq);

private:
	bool RemoteCatchUp(void);
	bool SendCatchupMessage(void);
	U32 DecodeHeaderSize(struct catchup_header &);
	bool DecodeCatchupAckHeader(char* buf, struct catchup_header  &ch);
	U32 DecodeCatchupAckBody(char* buf,class FCatchupItem &fci);
	bool  EncodeCatchupMessage(char* , U32 &, U64 );
	bool LogAndUpdateMetainfo(class FCatchupItem &fci);
	void set_catchup_sequence(U64 cseq){
		catch_sequence_ = cseq;
	}	
	/*
	*if can't get the ip trougth zookeeper, return and elect leader
	*/
	/*
	bool GetLeaderIp(void){  //fixme, actually here we shoud contact the zookeeper cluster
		leader_ip_ = (char *)malloc(strlen(leaderip) + 1);
		memcpy(leader_ip_,leaderip, strlen(leaderip) + 1);
		return true;
	}
	*/
	int range_;
	char leader_ip_[20];
	int catch_sock_;
	//U64 cmt_sequence;
	U64 catch_sequence_;		
	char catch_up_ack_buf_[CATCHUP_RETURN_ACK_SIZE];
	class InMemLogicalTable *logical_table_;
	DISALLOW_COPY_AND_ASSIGN(FRecovery);
};
#endif
