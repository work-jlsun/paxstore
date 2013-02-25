/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: select the leader and decide the role of all nodes 
*/

#ifndef __LEADERELECTION__
#define  __LEADERELECTION__
#include <assert.h>

#include <pthread.h>
#include <zookeeper/zookeeper.h>

#include "cfg.h"
#include "kvproposal.h"

class LogManager;
class InMemLogicalTable;
class MetaLogStore;
class LeaderFailover;
class FollowerList;
class StoreEngineManager;

enum ElectSate{
	ELECT_AT_MIDDLE = 0,
	ELECT_AT_END,
};

class ZooKeeperWatcher {
friend void ZKUnitTest2(void);
friend void ZKUnitTest4(void);
friend void ZKUnitTest5(void);
public:
	ZooKeeperWatcher(class MetaLogStore*, class LogManager*);
	ZooKeeperWatcher(class MetaLogStore*, class LogManager *, class RangeTable & rangetable, int range);
	~ZooKeeperWatcher();
	bool Init(void);
	bool Start(void);
	char* election_base_dir(void){
		return (char *)election_base_dir_;
	}
	void SetElectState(ElectSate elect_state){
		elect_state_ = elect_state;
	}
	bool TestElectState(ElectSate elect_state){
		if (elect_state_ == elect_state){
			return true;
		}else{
			return false;
		}
	}
	zhandle_t * zookeeper_handle(void){
		return zk_;
	}
	bool IfLeaderExist(struct String_vector *v);	
	bool IfLeaderIsMe(void);
	bool AddChildrenWatch(void);
	bool ZooWGetChildren(zhandle_t *zk, const char* dir, watcher_fn wfunc, struct String_vector &childrens);
	bool ZooGetChildren(zhandle_t *zk, const char* dir, int Watch, struct String_vector &childrens);
	bool LeaderElectStart(void);
	bool ExistQuorum(String_vector & childrens);	
	bool CheckMyExistenceAndReUpload(String_vector & childrens);
	bool IfIAmLeader(String_vector & childrens);	
	bool IfIAmLeader2InQuorum(String_vector & childrens);
	bool SetMeTheLeader(void);
	void LockEntry(void){
		if (!pthread_mutex_lock(&mutex_)){
			return;
		}else{
			assert(0);
		}
	}
	void UnLockEntry(void){
		if (!pthread_mutex_unlock(&mutex_)){
			return;
		}else{
			assert(0);
		}
	}
	bool UpdateEpochVector(U64 new_epoch, U64 end_sequence);
	bool GetLastItem(int32_t &epoch_num, U64 &epoch, U64 &sequence);
	bool GetUpdateEpochAndSeq(void);
	void InitLeaderFailoverInfo(void);
	void StopFormerFollower(void);
	bool StartNewFollower(void);	
	bool StartLeaderFailover(FollowerList *follwer_list);
	bool  StartNewLeader(void);	

	bool  LocalRecovery();
	bool RecoveryData(U64 begin_seq,U64 end_seq);

	InMemLogicalTable* GetLogicTable();
	bool DeleteEpochDir(char * dirname);	
	char * epoch_base_dir(void){
		return epoch_base_dir_;
	}
	void ZKUnitTest1(void);	
	class LeaderFailover *leader_failovers_;

private:
	ZooKeeperWatcher(){};
	int32_t ReadFreshLSN(U64 epoch, U64 sequence, U64 &lsn);	
	bool LockEpochDir(void);
	bool UnLockEpochDir(void);
	bool DecodeEpoch(char * s , U64 & epoch_value);
	bool DecodeEndSeq(char * s , U64 & endseq);
	bool ExamEpochVectorOk(U64 new_epoch);
	bool UpLoadNewEpochVectorItem(U64 new_epoch, U64 end_sequence);
	bool DecodeZooKeeperSequence(char *s, U64 &zk_seuquence);
	bool DecodeID(char * s , U32 & id);
	bool DecodeLsn(char * s , U64 & lsn);
	bool GetLeaderInfo(void);
	bool CreateBase(const char *);
	void WaitAfterResetDir(void);
	bool UpLoadMyNewLsn(U64 epoch, U64 lsn_sequence);
	bool _GetLastItem(int32_t &epoch_num, U64 &epoch, U64 &sequence);

	bool DeleteLeaderElectDir(struct String_vector & childrens);

	U32 range_num_;
	U32 my_id_;
	char my_ip_[20];
	U32  leader_id_;
	char  leader_ip_[20];
	char	      election_base_dir_[50];
	char	      epoch_base_dir_[50];
	zhandle_t *zk_;
	U32 sleeptime_after_clear_;
	ElectSate elect_state_;	
	pthread_mutex_t mutex_;   // the mutex protect the function LeaderElectstart( it is non reentrant function see point40-7 )
	class  InMemLogicalTable  *logical_truncated_table_;
	class MetaLogStore* meta_log_store_;
	class LogManager* log_store_;	
	U64 cmted_epoch_;
	U64 cmted_sequece_;
	U64 update_epoch_;
	U64 update_sequence_;
	class ZKControl *zk_control_;
	DISALLOW_COPY_AND_ASSIGN(ZooKeeperWatcher);
};

static void global_watcher(zhandle_t *, int type, int state, const char *path,void*v);
void PrintZKErrors(zhandle_t *zh, int type, int state, const char *path,void* ctx);
U32 print_childrens( struct String_vector *v);
int deallocate_String_vector(struct String_vector *v);
void PrintZKCallBackErrors(int error);
static void FindLeaderUpdate(zhandle_t *zk, int type, int state, const char *path, void *watcherCtx);
#endif
