/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: 
*/

#ifndef  __LEADERFAILOVER__
#define __LEADERFAILOVER__
#include <stdlib.h>

#include <deque>

#include <logicaltruncatedtable.h>
#include "log_manager.h"
#include "followerlist.h"
#include "logworker.h"

class InMemLogicalTable;
class FollowerList;

class FailOverItem{
public:	
	FailOverItem(){}
	virtual ~FailOverItem(){
		LOG(HCK,("-->"));
		if (kv_){
			free(kv_);
		}
		LOG(HCK,("<--"));		
	}
	bool Init(LogKV * log_kv,ProposalNum &pn);
	U32 key_len(void){
		return key_len_;
	}
	U32 value_len(void){
		return value_len_;
	}
 	char * kv(void){
		return kv_;
	}
	ProposalNum & pn(void){
		return  pn_;
	}
private:
	ProposalNum pn_;
	U32 key_len_;
	U32 value_len_;
	char *kv_;
};

class LeaderFailover{
friend void LeadrFailoverUnitTest1(void);
public:
	LeaderFailover(U32 range_num, class ZooKeeperWatcher *zk_watcher);	
	virtual ~LeaderFailover();
	
	void InitInfos(U64 current_epoch, U64 cmted_sequence, U64 logged_lsn){
		current_epoch_ = current_epoch;
		logged_lsn_ = logged_lsn;
		cmted_sequence_ = cmted_sequence;
	}
	void InitialNextUseP12Info(U64 epoch, P12Info & p12_info);
	
	bool CheckGapWithZooKeeperAndUpdate(int &flag, U64 &nextepoch, U64 &endseq);	
	bool ReadUnCMTLog(void);
	bool WaitFollowerForQuorum(FollowerList *follwer_list, FollowerList::size_type &num);
	void  UpdateCMT(U64 epoch, U64 sequence, P12Info &p12_info);	
	void  UpdateCMT(FailOverItem *failover_item, P12Info &p12_info);	
	bool  UpdateZookeeperEpoch(U64 new_epoch, U64 end_sequence  );	
	bool UpdateLogicalTurncatedTable(U64 epoch, U64 sequence);
	bool StartCatchupThread(void);
	void InitialSomeP12State(class P12Info  &p12_info);	
	bool StartFailover(FollowerList *follwer_list);	
	bool FindOneReadyFollower(FollowerList *follwer_list, FollowerList::size_type &num);
	void PrintFailitems(void);
	pthread_t catchupthread_tid(void){
		return catchupthread_tid_;
	}
private:
	LeaderFailover(){}
	U32 range_num_;
	U64 current_epoch_;
	U64 cmted_sequence_;
	U64 logged_lsn_;
	std::deque<class FailOverItem*>  failitem_list_;
	char sbuf_[LEADER_FAILOVER_SUCCESS_MSG_SIZE];
	class ZooKeeperWatcher *zk_watcher_;
	bool if_local_warmedup_;
	class  InMemLogicalTable  logical_truncated_table_;
	pthread_t catchupthread_tid_;	//the leaderfailover sometimes need a  catchup thread

};
void LeadrFailoverUnitTest1(void);
#endif
