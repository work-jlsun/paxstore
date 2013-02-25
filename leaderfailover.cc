/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#include "leaderfailover.h"

#include "cfg.h"
#include "leaderelection.h"
#include "kvproposal.h"
#include "logicaltruncatedtable.h"
#include "metalogstore.h"
#include "leaderthreads.h"

bool FailOverItem::Init(LogKV * log_kv, ProposalNum &pn){
	int len;
	pn_ = pn;
	key_len_ = log_kv->key_len;
	value_len_ = log_kv->value_len;
	len = key_len_ + value_len_;
	kv_  = (char *)malloc(len);
	if (NULL == kv_){
		LOG(HCK,("FAilOverItem need more mem\n"));
		return false;
	}
	memcpy(kv_, log_kv->kv, len);
	return true;
}

LeaderFailover::LeaderFailover(U32 range_num, class ZooKeeperWatcher *zk_watcher){
	if_local_warmedup_ = false;	
	range_num_ = range_num;
	zk_watcher_  = zk_watcher;
}

LeaderFailover::~LeaderFailover(){
	if_local_warmedup_ = true;
	int i;
	i = failitem_list_.size();
	while( i-- ){
		delete failitem_list_[i];
	}
	//and so on
}

void LeaderFailover::PrintFailitems(void){
	int i;
	i = failitem_list_.size();
	if (i == 0){
		return;
	}
	while(i--){
		LOG(HCK,("epoch: %llu, sequence:%llu\n",failitem_list_[i]->pn().epoch, failitem_list_[i]->pn().sequence));	
	}
	return;
}

/*
*function:find one readyfollower from the follower list (the ready follower have all the sended and logged item
*of the follower(we also can simply call it catcheduped followers) 
*
*/
bool LeaderFailover::FindOneReadyFollower(FollowerList *follwer_list, FollowerList::size_type &num){
	for(FollowerList::size_type  i = 0; i != follwer_list->FollowerNum(); i++){
			if (follwer_list->IsNewItem(i)){
				num = i;
				return true;
			}
	}
	return false;
}

/*
* the leader failover also need a quorum.when a follower have catchuped
* it will  have all the log entry of this leader(new elected),
*attention: it may contain more log entrys than the leader(see point 42)
*/
bool LeaderFailover::WaitFollowerForQuorum(FollowerList *follwer_list, FollowerList::size_type &num){
	while ( 1 ){
		sleep(WAIT_TIME);		
		follwer_list->LockCatchupMutex();
		follwer_list->LockFlist();	
		if (follwer_list->FollowerLiveNum() + 1 >= QUORUM_NUM ) {
			if(!FindOneReadyFollower(follwer_list,num)){
				LOG(HCK,("have one ready, but not find\n") );
				follwer_list->UnlockFlist();			
				follwer_list->UnlockCatchupMutex();			
				assert(0);
			}else{
				follwer_list->UnlockFlist();			
				follwer_list->UnlockCatchupMutex();
				return true;
			}
		}else{
			 follwer_list->UnlockFlist();
			 follwer_list->UnlockCatchupMutex();
		}
	}
}

 /*
 *update the cmt  metainfo of the log
 *this include the "durability db and the in mem info (p12info)"
 */
void 
LeaderFailover::UpdateCMT(FailOverItem *failover_item, P12Info &p12_info){
	//update the durability store
	mlstore.StoreMetaInfo(range_num_, failover_item->pn());
	
	//update the in mem info
	p12_info.commited_sequence_ = failover_item->pn().sequence;	
}

/*
 *update the cmt  metainfo of the log
 *this include the "durability db and the in mem info (p12info)"
 */
void LeaderFailover::UpdateCMT(U64 epoch, U64 sequence, P12Info &p12_info){
	//update the durability store
	ProposalNum pn;
	pn.epoch;	
	pn.sequence;	

	//LOG(HCK,("BEGIN RECOVERYDATA.\n"));
	//LOG(HCK,("BEGIN:%llu,END:%llu.\n",cmted_sequence_,sequence));
	//LOG(HCK,("END RECOVERYDATA.\n"));
	pn.epoch = epoch;
	pn.sequence = sequence;
	zk_watcher_->RecoveryData(cmted_sequence_,sequence);	// write the data between cmt and sequence into leveldb
	mlstore.StoreMetaInfo(range_num_, pn);	
	// LOG(HCK,("epoch:%llu.\tp12_info.epoch:%llu.-------------------------------------\n",epoch,p12_info.epoch()));
	//update the in mem info
	p12_info.commited_sequence_ = sequence;	
}

/*
* the reproposal stag have complished, the i will update the epoch table 
*this epoch table update will set the sequence end of this epoch ,
*the bigger sequence with this epoch will be stale
*/
bool LeaderFailover::UpdateZookeeperEpoch(U64 new_epoch, U64 end_sequence  ){	
	if (zk_watcher_->UpdateEpochVector( new_epoch, end_sequence)){
		LOG(HCK,("UpdateEpochVector ok\n"));
		return true;
	}else{
		LOG(HCK,("UpdateEpochVector error\n"));
		//assert(0);
		return false;
	}
}

/*
*
*update local logicalTurncatedtable after update  the zookeeper epoch vector 
*
*/
bool LeaderFailover::UpdateLogicalTurncatedTable(U64 epoch, U64 sequence){
	if ( if_local_warmedup_== false) {
		LOG(HCK,("BEGIN WARMUP.\n"));
		if ( !logical_truncated_table_.LogicalTableWarmUp() ){
			LOG(HCK,("LogicalTableWarmUp is error\n"));
			return false;
		}
		if_local_warmedup_ = true;
		LOG(HCK,("WARMUP END.\n"));
	}
	LOG(HCK,("-->"));
	if ( !logical_truncated_table_.UpdateLogicalTable(epoch,sequence) ){
		LOG(HCK,("UpdateLogicalTable is error\n"));
		return false;
	}
	LOG(HCK,("<--"));
	return true;
}

/*
*
*attention: catchup thread is  High coupling with Global_P12Info
*		  and is also coupling with Global_ProposalQueue("when in the online catchup condition
		  will get the sended but haven't loged proposals from  the Global_ProposalQueue ")
		  1:Global_P12Info.next_unused_sequence()
		  2:Global_P12Info.lsn_sequence()
		  3:Global_ProposalQueue()
		  those three have been used
*
*/
bool LeaderFailover::StartCatchupThread(void){
	int err;
	err = pthread_create(&(catchupthread_tid_), NULL ,leadercatchup_thread, NULL);
	if (err != 0){
		LOG(HCK,("pthread_create err\n"));
		return false;
	}
	return true;
}

/*
*
* initial same state items of the Global_P12Info, case it is used in the catchup thread
* this function shoud call after
*/
void LeaderFailover::InitialSomeP12State(class P12Info  &p12_info){
	p12_info.epoch_ = current_epoch_;
	p12_info.next_unused_sequence_ = logged_lsn_ + 1;
	p12_info.lsn_sequence_ = logged_lsn_;		
	p12_info.commited_sequence_ = cmted_sequence_;
	//other infos are not used in catchup thread
	p12_info.highest_promise_sequence_ = cmted_sequence_;
	p12_info.open_cnt_ = 0;
	p12_info.range_num_ = range_num_;
	//p12_info.last_synced_cmt_sequence_ = 
}

/*
*
*check if there is  not completely accomplished failover's
*
*atttention here: a follower that have been selected be the leader
*		 	   must have  less than one epoch gap with the zookeeper
*			   if bigger than one gap , this is not right		
*
* argcs: the flag is too see if have a gap,if have set the flag = 1
* 		when there is gap endseq store the "end sequence of the gap"
*/
bool LeaderFailover::CheckGapWithZooKeeperAndUpdate(int &flag, U64 &nextepoch, U64 &endseq){
	//first get my local logical table
	if ( false == if_local_warmedup_){
		if (!logical_truncated_table_.LogicalTableWarmUp()){
			LOG(HCK,("LogicalTableWarmUp err\n"));
			return false;
		}
		if_local_warmedup_ = true;
	}

	//then get the last item of the  zookeeper epoch vector
	int32_t  epoch_num;
	U64 epoch, sequence;
	if ( !zk_watcher_->GetLastItem(epoch_num, epoch, sequence) ){
		LOG(HCK,("GetLadtItem error\n"));
		return false;
	}
	LOG(HCK, ("Get Last Item epoch: %llu----------------------------------------\n",epoch));
	if ( 0 == epoch_num) {
		if ( logical_truncated_table_.empty() ){
			//this condition is also have no gap , 			
			LOG(HCK,("the logical truncated table is tmpty new\n"));
			LOG(HCK,("this may be the in the system startup ,or first leader down\n"));
			flag = 0;
			return true;		
		}else{
			LOG(HCK,("the zookeeper epoch vector is empty, but the local is not , error\n"));
			return false;
		}
	}else if (   logical_truncated_table_.Size()  ==  epoch){  // local have leak one item from the zookeeper
		LOG(HCK,("right just one gap\n"));
		nextepoch = epoch;
		endseq = sequence;
		flag = 2;
		return true;
	}else if (logical_truncated_table_.Size() - 1 == epoch){ //have no gap
		LOG(HCK,("right just no gap\n"));
		nextepoch = epoch;
		endseq = sequence;
		flag = 1;
		return true;		
	}
	else{
		LOG(HCK,("there is some thing wrong-->"));
		LOG(HCK, ("zookeeper epoch:%llu, local epoch:%llu\n", epoch, logical_truncated_table_.Size() ));
		assert(0);
		return false;
	}	
}

/*
*after the leaderfailover have been complished
*init same states that will be used in the next proposal progress
*/
void LeaderFailover::InitialNextUseP12Info(U64 epoch, P12Info & p12_info){
	p12_info.epoch_  = epoch;
	p12_info.last_synced_cmt_sequence_ = p12_info.commited_sequence_;
}

/*
*
*start leader failover
*
*
*/
bool 
LeaderFailover::StartFailover(FollowerList *follwer_list){   
		FollowerList::size_type num;
		int flag;
		U64 nextepoch, endseq;
		/*
		if (!ReadUnCMTLog()) {
			LOG(HCK,("ReadUnCMTLog err\n"));
			return false;			
		}
		*/
		// the i alse shoud recover some in mem stateinfo
		//because the catchupthead will use those stateinfo
		InitialSomeP12State(Global_P12Info);

		//check with the zookeeper here
		if (!CheckGapWithZooKeeperAndUpdate(flag, nextepoch, endseq)){
			LOG(HCK,("CheckGapWithZooKeeperAndUpdate err\n"));
			return false;
		}else{
			LOG(HCK,("CheckGapWithZooKeeperAndUpdate ok flag = %d\n", flag));
		}
		if (flag == 2) {			//there is a gap
			// update  the cmt
			LOG(HCK,("enter flag == 2\n"));
			 UpdateCMT(nextepoch,  endseq, Global_P12Info);			
			//update Logical turncated table
			LOG(HCK,("nextepoch:%llu,endseq:%llu.\n",nextepoch,endseq));
			if ( !UpdateLogicalTurncatedTable(nextepoch,  endseq) ) {
				LOG(HCK,("flag = 1,UpdateLogicalTurncatedTable error\n"));
				return false;
			}		
			LOG(HCK,("END updata nextepoch.\n"));
			/*if there is a gap, then(there must have no new proposal with new epoch,so
			*no need to catchup, just set a new logical truncated item with a new epoch())
			*but this new logical truncated item have no proposals,have the some endseq
			*as the formal one
			*/
			if( !UpdateZookeeperEpoch(nextepoch + 1, endseq) ){
				LOG(HCK,("flag = 1,UpdateZookeeperEpoch error\n"));
				return false;
			}
			//update Logical turncated table
			if ( !UpdateLogicalTurncatedTable(nextepoch + 1, endseq)){
				LOG(HCK,("flag = 1,UpdateLogicalTurncatedTable error\n"));
				return false;
			}			
			if ( !StartCatchupThread() ) {
				LOG(HCK,("StartCatchupThread error\n"));
				return false;
			}
			InitialNextUseP12Info(nextepoch + 2, Global_P12Info);			
			LOG(HCK,("leader failover ok with a gap\n"));
			return true;
		}
		else if(flag == 1) {		//this is also a condition of no gap
			goto normal;
		}else if (flag == 0){		//this is also a condition of no gap, at system startup
			if ( logged_lsn_ == 0 ){
				/*no need to wait catchup( there is no items to reproposal) , just do others 
				* attention, the sequence is bigger than 0 (the first seuence num is 1)
				*then start the catchup thread
				*/			
				//update the zookeeper epoch
				if( !UpdateZookeeperEpoch( 0, 0)){
					LOG(HCK,("UpdateZookeeperEpoch error\n"));
					return false;
				}else{
					LOG(HCK,("UpdateZookeeperEpoch ok\n"));			
				}
				
				//update Logical turncated table
				if ( !UpdateLogicalTurncatedTable(0, 0)){
					LOG(HCK,("UpdateLogicalTurncatedTable error\n"));
					return false;
				}else{
					LOG(HCK,("UpdateLogicalTurncatedTable ok\n"));
				}
				
				//also start up the catchup thread ,but no need to wait qourum
				if (!StartCatchupThread()){
					LOG(HCK,("StartCatchupThread err\n"));
					return false;
				}else{
					LOG(HCK,("StartCatchupThread ok\n"));					
				}
				
				// init some p12_infos
				InitialNextUseP12Info(1, Global_P12Info);
				LOG(HCK,("the startup failover progress is ok now\n"));				
				return true;
			}
			else{
				/* it must can not run here, the zookeeper epoch vector 
				*is empty this can just can happend in the system start up
				*/
				LOG(HCK,("zookeeper epoch vector is empty, but have log, error\n"));
				assert(0);
			}			
		}				
normal:		
		//lsn == endseq there have no  proposal need to reproposal
		if ( Global_P12Info.lsn_sequence()== endseq  ) {			
			// LOG(HCK,("-----------------WHAT A FUCKING ERROR-----------------.\n"));
			if( !UpdateZookeeperEpoch(nextepoch + 1, endseq) ){
					LOG(HCK,("flag = 1,UpdateZookeeperEpoch error\n"));
						return false;
			}
			//update Logical turncated table
			if ( !UpdateLogicalTurncatedTable(nextepoch + 1, endseq)){
					LOG(HCK,("flag = 1,UpdateLogicalTurncatedTable error\n"));
						return false;
			}			
			if ( !StartCatchupThread() ) {
					LOG(HCK,("StartCatchupThread error\n"));
					return false;
			}
			InitialNextUseP12Info(nextepoch + 2, Global_P12Info);
			// LOG(HCK, ("now the epoch is %llu\n", Global_P12Info.epoch()));
			LOG(HCK,("leader failover ok with a gap\n"));
			return true;			
		}		
		

		//lsn > endseq  shoud reproposal the items after endseq
		
		//then start the catchup thread
		LOG(HCK,("tnter normal mode\n"));
		if (!StartCatchupThread()){
			LOG(HCK,("StartCatchupThread err\n"));
			return false;		
		}else{
			LOG(HCK,("StartCatchupThread ok\n"));
		}
		//got the last log entry
		//ProposalResponseInfo proposal_response_info;
		//FailOverItem *failover_item = failitem_list_.front();
		//encode the proposal to message buffer	
		LOG( VRB,("-->SendFailoverProposals\n"));	
		while(1){
				LOG(HCK,("go to wait quorom\n"));
				WaitFollowerForQuorum(follwer_list, num);
				LOG(HCK,("wait quorom ok\n"));
				//then i will  update my cmt. and then will update the epoch(increase the epoch)
				UpdateCMT(current_epoch_,  logged_lsn_, Global_P12Info);
				if( !UpdateZookeeperEpoch(current_epoch_, logged_lsn_)){
					LOG(HCK,("UpdateZookeeperEpoch error\n"));
					return false;
				}else{
					LOG(HCK,("UpdateZookeeperEpoch ok\n"));
				}
				//update Logical turncated table
				if ( !UpdateLogicalTurncatedTable(current_epoch_, logged_lsn_)){
					LOG(HCK,("UpdateLogicalTurncatedTable error\n"));
					return false;
				}else{
					LOG(HCK,("UpdateLogicalTurncatedTable ok\n"));
				}
				InitialNextUseP12Info(current_epoch_ + 1, Global_P12Info);
				LOG(HCK,("leader failover ok with no gap\n"));			
				break;
		}
		return true;
}
