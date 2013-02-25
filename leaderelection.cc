/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: select the leader and decide the role of all nodes 
*/

#include "leaderelection.h"

#include <string.h>

#include <iostream>
#include <list>
#include <set>
#include <vector>
#include <algorithm>
#include <errno.h>

#include "followerlist.h"
#include "metalogstore.h"
#include "log_manager.h"
#include "logicaltruncatedtable.h"
#include "logworker.h"
#include "zkcontrol.h"
#include "leaderfailover.h"
#include "data_store.h"
#include "tools.h"
#include "rangetable.h"

InMemLogicalTable * ZooKeeperWatcher::GetLogicTable(){
	return logical_truncated_table_;
}

U32 print_childrens( struct String_vector *v){
	LOG(HCK,("-->\n"));
	U32 item_num = 0;
	if (v->data) {
	    int32_t i;
	    for (i = 0; i != v->count; i++) {
	   	if(v->data[i] != NULL){ 
			LOG(HCK,("%s\n",v->data[i]));
			item_num++;
		}
	    }			
	}
	LOG(HCK,("<--\n"));
	return item_num;
}

 
int deallocate_String_vector(struct String_vector *v) {
    if (v->data) {
	LOG(HCK,("-->\n"));	
        int32_t i;
        for(i=0;i<v->count; i++) {
            deallocate_String(&v->data[i]);
        }
        free(v->data);
        v->data = 0;
	LOG(HCK,("<--\n"));
    }
    return 0;
}

ZooKeeperWatcher::ZooKeeperWatcher(class MetaLogStore* meta, class LogManager *log){
	range_num_ = RANGE_NUM;
	my_id_	    = MY_ID;
	strcpy(my_ip_, MY_IP);
	leader_id_ = 0;
	leader_ip_[0] = '\0';
	meta_log_store_ = meta;
	log_store_ = log;
	logical_truncated_table_ = new InMemLogicalTable();
	if (logical_truncated_table_ == NULL){
		LOG(HCK,("new InMemLogicalTable error\n"));
		assert(0);
	}
	leader_failovers_ = new LeaderFailover(range_num_, this);
	if (leader_failovers_  ==  NULL){
		LOG(HCK,("new LeaderFailover error\n"));
		assert(0);
		
	}
	zk_control_ = new ZKControl();
	if (zk_control_ == NULL){
		LOG(HCK,("new zk_control_ error\n"));
		assert(0);		
	}
	
	snprintf(election_base_dir_, sizeof(election_base_dir_), "%s/KeyRange%u", 
				ROOT_ELECTION_DIR, range_num_);
	LOG(HCK,("election_base_dir_:%s\n",election_base_dir_));
	snprintf(epoch_base_dir_, sizeof(epoch_base_dir_), "%s/KeyRange%u", 
				EPOCH_FOR_RANGE_BASE, range_num_);
	
	LOG(HCK,("epoch_base_dir_:%s\n",epoch_base_dir_));
	sleeptime_after_clear_ = SLEEP_AFTER_CLEAR;
	elect_state_ =  ELECT_AT_END;	
	if ( pthread_mutex_init(&mutex_, NULL)  != 0){
		assert(0);
	}	
	
}

ZooKeeperWatcher::ZooKeeperWatcher(class MetaLogStore* meta, class LogManager *log, class RangeTable &rangetable, int range_num){
	/*
	range_num_ = RANGE_NUM;
	my_id_	    = MY_ID;
	strcpy(my_ip_, MY_IP);
	*/
	range_num_ = range_num;
	my_id_	   = rangetable.my_id();
	std::string ip;
	rangetable.GetServerIP(my_id_, ip);
	strcpy(my_ip_, ip.c_str());

	//init the ports
	class RangeItem  rangeitem  = rangetable.GetRangeItem(range_num); 
	DEFAULT_PROT_NUM =  rangeitem.leader_write_port_; 
	LEADER_CATCHUP_PORT_NUM =  rangeitem.leader_catchup_port_; 
	FOLLOWERPORT = rangeitem.follower_listen_port_;
	READ_PORT_NUM = rangeitem.read_port_;
	LOG(HCK,("DEFAULT_PROT_NUM:%d\nLEADER_CATCHUP_PORT_NUM:%d\nFOLLOWERPORT:%d\nREAD_PORT_NUM%d\n",DEFAULT_PROT_NUM,LEADER_CATCHUP_PORT_NUM,FOLLOWERPORT,READ_PORT_NUM));

	leader_id_ = 0;
	leader_ip_[0] = '\0';
	meta_log_store_ = meta;
	log_store_ = log;
	logical_truncated_table_ = new InMemLogicalTable();
	if (logical_truncated_table_ == NULL){
		LOG(HCK,("new InMemLogicalTable error\n"));
		assert(0);
	}
	leader_failovers_ = new LeaderFailover(range_num_, this);
	if (leader_failovers_  ==  NULL){
		LOG(HCK,("new LeaderFailover error\n"));
		assert(0);
		
	}
	zk_control_ = new ZKControl();
	if (zk_control_ == NULL){
		LOG(HCK,("new zk_control_ error\n"));
		assert(0);		
	}
	
	snprintf(election_base_dir_, sizeof(election_base_dir_), "%s/KeyRange%u", 
				ROOT_ELECTION_DIR, range_num_);
	LOG(HCK,("election_base_dir_:%s\n",election_base_dir_));
	snprintf(epoch_base_dir_, sizeof(epoch_base_dir_), "%s/KeyRange%u", 
				EPOCH_FOR_RANGE_BASE, range_num_);
	
	LOG(HCK,("epoch_base_dir_:%s\n",epoch_base_dir_));
	sleeptime_after_clear_ = SLEEP_AFTER_CLEAR;
	elect_state_ =  ELECT_AT_END;	
	if ( pthread_mutex_init(&mutex_, NULL)  != 0){
		assert(0);
	}	
	
}

void ZooKeeperWatcher::InitLeaderFailoverInfo(void){
	int32_t ret;
	ProposalNum  pn;
	leader_failovers_->InitInfos(update_epoch_, cmted_sequece_, update_sequence_);
}

int32_t ZooKeeperWatcher::ReadFreshLSN(U64 epoch, U64 sequence, U64 &lsn){
	int ret_flag=0;
	ret_flag = log_store_->LogGetLsn(epoch, sequence, lsn);
	return ret_flag;
}

/*
*the leader elect need infomation [epoch:lsn]
*and this information shoud generater from the 
*the meta info the logstore and so on
*
*
*
*/
bool ZooKeeperWatcher::GetUpdateEpochAndSeq(void){
	// first read the epoch vector
	if ( !logical_truncated_table_->LogicalTableWarmUp()){
		LOG(HCK, ("LogicalTableWarmUp error\n"));
		return false;
	}
	//get the last item
	int32_t ret1;
	U64 epoch, sequence;
	ret1 = logical_truncated_table_->GetLastEpochVectorItem(epoch, sequence);	// find the last epoch and sequence from logical truncate table
	if (ret1 == -1){
		LOG(HCK, ("GetLastEpochVectorItem error\n"));
		return false;
	}
	ProposalNum pn;
	int32_t ret2;
	ret2 = meta_log_store_->GetCommitPn(range_num_, pn);
	if (ret2 == GETERROR){
		LOG(HCK, ("GetCommitPn error\n"));		
		return false;
	}else if  (ret2 == GETEMPTY){
		cmted_epoch_ = 0;
		cmted_sequece_  = 0;
	}else{
		cmted_epoch_ = pn.epoch;
		cmted_sequece_ = pn.sequence;
	}
	
	if (ret1  ==  0){   //  the local epoch is  empty , it must happed int system startup 
		if(ret2 != GETEMPTY){
			LOG(HCK,("it is in the system startup, but  cmt is exist\n"));
			assert(0);
		}else{
			LOG(HCK,("the logical table is empty ,  cmt info is also empty\n"));
			update_epoch_ = 0;
			update_sequence_ = 0;
			return true;
		}	
	}else{		//the local epoch is not empty
		int32_t ret3;
		U64 lsn;
		LOG(HCK,("local is not empty, epoch:%llu, sequence:%llu\n", epoch, sequence));
		if (ret2 == GETEMPTY){
			pn.epoch = 0;
			pn.sequence = 0;
		}
		if (epoch ==  pn.epoch){		//actually the log lsn is shoud started at [1,1]	
			LOG(HCK,(" epoch vector biggest epoch is same as metastore cmt epoch\n"));
			ret3 = ReadFreshLSN(epoch + 1, sequence + 1, lsn);
			if (ret3 == -1){
				return false;
			}else if (ret3 == 0){
				update_epoch_ = epoch;		//  
				//update_epoch_ = epoch + 1;		// 
				update_sequence_ = sequence;
				return true;
			}else if (1 == ret3) { 
				update_epoch_ = epoch + 1;
				update_sequence_ = lsn;		// the last record of the log file
				return true;
			}
		}else if (epoch + 1 == pn.epoch ){
			LOG(HCK,("metastore cmt epoch is bigger (+1)\n"));
			if (sequence  >=  pn.sequence){
				assert(0);
			}
			ret3 = ReadFreshLSN(pn.epoch, pn.sequence + 1, lsn);
			if (ret3 == -1){
				return false;
			}else if (ret3 == 0){
				update_epoch_ = pn.epoch;
				update_sequence_ = pn.sequence;
				return true;
			}else if (ret3 == 1) {
				update_epoch_ = pn.epoch;
				update_sequence_ = lsn;	// here it has been the last record of log file
				return true;
			}
		}else{
			LOG(HCK,("the epoch vector (epoch:sequence) shoud be bigger than cmt in our condition\n"));
			assert(0);
			return false;
		}
	}	
}

void ZooKeeperWatcher::StopFormerFollower(void){
	if (zk_control_->my_state() == FOLLOWER){
		LOG(HCK,("the former state is follower, stop the former instance\n"));
		zk_control_->StopFollower();
		return;
	}else if (zk_control_->my_state() == INITIAl){
		LOG(HCK,("the former state is INITIAl,  no need do the stop\n"));	
		return;
	}else{
		LOG(HCK,("if it will became a follower, the former state must be follower or initial\n"));
		assert(0);
	}
}

bool  ZooKeeperWatcher::StartNewFollower(void){
	class PassArgsToFollower pass_args;
	pass_args.init(range_num_,  leader_id_,  leader_ip_);
	if (!zk_control_->StartFollower(pass_args)){
		return false;	
	}
	return true;
}

bool ZooKeeperWatcher::StartLeaderFailover(FollowerList *follwer_list){
	//we shoud clear all the old followers in the follower list
	follwer_list->ClearOldFollowers();
	if ( !leader_failovers_->StartFailover( follwer_list)){
		LOG(HCK,("leader_failovers_ ->StartFailover error\n"));
		return false;
	}else{
		return true;
	}
}
bool  ZooKeeperWatcher::StartNewLeader(void) {
	if (!zk_control_->StartLeader(leader_failovers_->catchupthread_tid())){	// start the leader catch up thread
		return false;	
	}
	return true;
}

bool ZooKeeperWatcher::Init(void){
	zk_ = zookeeper_init("192.168.3.21:2180,192.168.3.21:2181,192.168.3.21:2182",
					global_watcher, RECV_TIME, 0, NULL,0);
	sleep(2);
	//fixeme  the zookeeper_init is an async call, when it returns , it not represent ok
	if (zk_ == NULL){
		LOG(HCK,("zookeeper init error\n"));
		return false;
	}
	return true;
}

bool ZooKeeperWatcher::Start(void) {
	int i = 0;
	if (!LeaderElectStart()){
		LOG(HCK,("LeaderElectStart error\n"));
		return false;
	}
        //is it is ok,here need syn ,fixme , we will use pthread_cond  later
        /*
        while(std::cin >> i){
                if (i == 1){
                        break;
                }
        }
        */
        while(true){
                sleep(60);
        }
        LOG(HCK,("fuck the problem\n"));
}

ZooKeeperWatcher::~ZooKeeperWatcher(){
	if(my_ip_ != NULL){
		free(my_ip_);
	}
	if(leader_ip_ != NULL){
		free(leader_ip_);
	}	
	if (election_base_dir_ != NULL){
		free(election_base_dir_);
	}
	if ( zookeeper_close(zk_) == ZOK ) {
		LOG(HCK,("close success\n"));
	}
	if ( pthread_mutex_destroy(&mutex_)  != 0){
		assert(0);
	}	
	if (logical_truncated_table_ != NULL){
		delete logical_truncated_table_;
	}
}
/*
*  key:   lsn:xxxxx:id:xxxx:seq:sequence_num{"xxxx" it the lsn number of the server, "sequence_num" is generated by zookeeper}
*  value:  id { "it is the server id"}
* 
*/
inline bool ZooKeeperWatcher::DecodeZooKeeperSequence(char *s, U64 &zk_seuquence){
	U64 value;
	char *start;
	char *endptr;
	if (s == NULL){
		assert(0);
		return false;
	}
	start = strchr(s, 'q');
	if (start != NULL) {
		value  = strtoull(start + 2, &endptr, 10);
		zk_seuquence = value;
	}else{
		assert(0);
		return false;
	}		
	return true;
}
/*
*key:   lsn:xxxx:id:xxxx:seq:xxxx
*/
inline bool ZooKeeperWatcher::DecodeID(char * s , U32 & id){
	U32 value;
	U32 len;
	char *start;
	char *end;
	char *endptr;
	char cbuf[40];
	if (s == NULL){
		return false;
	}
	start = strchr(s, 'd');
	if (start != NULL) {
		end =  strchr(start + 2 , ':');
		if(end != NULL){
			len = end - start -2;
			strncpy(cbuf, start+2, len);
			cbuf[len] = '\0';
			value  = strtoull(cbuf, &endptr, 10);
			id = value;
		}else{
			assert(0);
			return false;
		}
	}else{
		assert(0);
		return false;
	}		
	return true;
}

inline bool ZooKeeperWatcher::DecodeLsn(char * s , U64 & lsn){
	U64 value;
	U32 len;
	char *start;
	char *end;
	char *endptr;
	char cbuf[40];
	if (s == NULL){
		assert(0);
		return false;
	}
	start = strchr(s, 'n');
	if (start != NULL) {
		end =  strchr(start + 2 , ':');
		if(end != NULL){
			len = end - start -2;
			strncpy(cbuf, start+2, len);
			cbuf[len] = '\0';
			value  = strtoull(cbuf, &endptr, 10);
			lsn = value;
		}else{
			assert(0);
			return false;
		}
	}else{
		assert(0);
		return false;
	}		
	return true;
}

inline bool  ZooKeeperWatcher::GetLeaderInfo(void){
	int rc;
	char cbuf[100];
	struct Stat stat1;
	LeaderValue   leader_value;
	int len = sizeof(leader_value);
	snprintf(cbuf, sizeof(cbuf),"%s/%s",election_base_dir_, LEADER_PREFIX);   //fixme , the range num	
	rc = zoo_get(zk_, cbuf, 0, (char *)&leader_value, &len, &stat1);
	if ( (int)ZOK != rc ){
		LOG(HCK,(" :zoo_get error\n"));
		PrintZooKeeperError(rc);
		return false;   //is the node is not exist,(may be happend,also,return false, then the  IfLeaderExist will also return false, it is also right);
	}
	LOG(HCK, ("leader_id:%u   leader_ip:%s\n",leader_value.leader_id, leader_value.leader_ip));
	leader_id_ =  leader_value.leader_id;
	strcpy(leader_ip_, leader_value.leader_ip);
	return true;
}

/*
*to see  if the leader have exist
*if the leader have exist  store the leader info in
*leader_id_ and leader_ip_
*/
bool ZooKeeperWatcher::IfLeaderExist(struct String_vector *v){		
	LOG(HCK,("-->\n"));
	for (U32 i = 0; i != v->count; i++){
		if(v->data[i] != NULL){
			if (strncmp(LEADER_PREFIX, v->data[i], strlen(LEADER_PREFIX)) == 0){
				LOG(HCK,("the leader have exist\n"));
				if ( !GetLeaderInfo()){
					return false;
				}	
				LOG(HCK,("<--\n"));
				return true;
			}	
		}
	}
	LOG(HCK,("<--\n"));
	return false;
}

bool ZooKeeperWatcher::IfLeaderIsMe(void){
	if (my_id_ == leader_id_ &&  (strcmp(my_ip_, leader_ip_) == 0)){
		return true;
	}else{
		return false;
	}
}

/*
*create the base  keyRange dir 
*/
bool 
ZooKeeperWatcher::CreateBase(const char *name){
	struct Stat stat1;
	int rc;
	int n = 5;
	char  node_name[100];
	snprintf(node_name, sizeof(node_name),"%s", name);   //fixme , the range num		
	while (n--) {
		rc = zoo_exists(zk_, node_name, 0, &stat1);
		if (rc == (int)ZOK){
			LOG(HCK,(" node exist\n"));
			return true;
		} else if( rc == (int)ZNONODE ) {
	        	rc = zoo_create(zk_, node_name, "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
			if (rc == (int)ZOK){
				LOG(HCK,(" node created\n"));				
				return true;
			} else if (rc == (int)ZNODEEXISTS ){
				return true;
			}else{
				PrintZooKeeperError(rc);
				continue;
			}
		} else{
			PrintZooKeeperError(rc);
			continue;
		}
	}
	assert(0);
	return false;
}
/*
*because  the time of  node "leader" disapper time is not all ok , so ,wait same time , so the next action(create node)
*the newly created node will not be delete by others  in the group 
*/
void ZooKeeperWatcher::WaitAfterResetDir(void){
	sleep(sleeptime_after_clear_);
	return;
}

inline bool ZooKeeperWatcher::UpLoadMyNewLsn(U64 epoch, U64 lsn_sequence){
	int rc;
	char  node_name[100];
	LOG(HCK,("-->\n"));	
	snprintf(node_name, sizeof(node_name), "%s/epoch:%llu:lsn:%llu:id:%u:seq:",election_base_dir_, 
		epoch, lsn_sequence,my_id_);
	
	rc = zoo_create(zk_, node_name, my_ip_, sizeof(my_ip_), &ZOO_OPEN_ACL_UNSAFE, 
					 ZOO_SEQUENCE |ZOO_EPHEMERAL , 0, 0);
	if (rc != ZOK){
		PrintZooKeeperError(rc);
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;	
}

// this function has not been called
bool ZooKeeperWatcher::AddChildrenWatch(void){
	int rc;
	struct String_vector  childrens;
	LOG(HCK,("-->\n"));	
	memset(&childrens, 0, sizeof(childrens));
	rc = zoo_wget_children(zk_, election_base_dir_, FindLeaderUpdate, this, &childrens);
	deallocate_String_vector(&childrens); //just add a wa
	if (rc != ZOK){
		PrintZooKeeperError(rc);
		return false;
	}
	LOG(HCK,("<--\n"));	
	return true;
}

bool ZooKeeperWatcher::ZooWGetChildren(zhandle_t *zk, const char* dir, watcher_fn wfunc, struct String_vector &childrens){
	int rc;
	LOG(HCK,("-->\n"));
	memset(&childrens, 0, sizeof(childrens));
	rc = zoo_wget_children(zk, election_base_dir_, wfunc, this, &childrens);
	if (rc != (int)ZOK) {
		LOG(HCK,("the base is create error or disconnect? wget error\n"));
		PrintZooKeeperError(rc);
		assert(0);
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;
}

bool ZooKeeperWatcher::ZooGetChildren(zhandle_t *zk, const char* dir, int Watch, struct String_vector &childrens){
	int rc;
	LOG(HCK,("-->\n"));
	memset(&childrens, 0, sizeof(childrens));
	rc = zoo_get_children(zk,dir, 0,  &childrens);  
	if (rc != (int)ZOK) {
		LOG(HCK,("the base is create error or disconnect? wget error\n"));
		PrintZooKeeperError(rc);
		assert(0);
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;
}

/*
*check if there exist and quorum
*
*/
bool ZooKeeperWatcher::ExistQuorum(String_vector & childrens){
	U32 id;
	std::set<U32>   member_ids;
	std::set<U32>::const_iterator   citer;
	if (childrens.count < QUORUM_NUM){
		return false;
	}
	for(U32 i = 0; i != childrens.count; i++){
		if (!DecodeID(childrens.data[i],id)){
			LOG(HCK,("the key format is not right\n"));
			assert(0);
			return false;			
		}
		member_ids.insert(id);
	}
	if (member_ids.size()  >= QUORUM_NUM){
		LOG(HCK,("ok, there exist an quorum\n"));
		return true;
	}else {
		LOG(HCK,("there not exist an quorum\n"));
		return false;
	}		
}

//lsn:xxxx:id:xxxx:seq:xxx
/*
*if there  have not exist an quorum, and my lsn Node have not exist(beceuse of other clear)
*i will re Upload my lsn;
*/
bool ZooKeeperWatcher::CheckMyExistenceAndReUpload(String_vector & childrens){
	U32 id;	
	for(U32 i = 0; i != childrens.count; i++){
		if (!DecodeID(childrens.data[i], id)){
			LOG(HCK,("the key format is not right\n"));
			assert(0);
			return false;
		}
		if (id == my_id_){
			LOG(HCK,("i am exist\n"));
			return true;
		}
	}	
	//not find myself , so i will reupdate;
	LOG(HCK,("i am not exist, upload my self\n"));
	/*it will be call in the miidle of leader elect, so no need to read 
	* the update_epoch_ and updated_sequence_ again
	*/
	if (UpLoadMyNewLsn(update_epoch_, update_sequence_)){
		return true;
	}else{
		return false;
	}
}

/*
*when there is an quorum, and then i check if i am the leader(lsn is the biggest)
* and if there exist the same biggest lsn , se choose the one have the smallest sequence num(generated by
*the zookeeper)
*/
//lsn:xxxx:id:xxxx:seq:xxx
bool ZooKeeperWatcher::IfIAmLeader(String_vector & childrens){
	U64 tmp_lsn = 0;
	U64 biggest_lsn = 0;		// lsn will not choose the "0" , it will choose start "1"
	U32 biggest_lsn_id = 0;
	U64	tmp_sequence;
	U64 smallest_sequence =0;
	char *result_node_string = NULL;
	for(U32 i = 0; i != childrens.count; i++){
		if (!DecodeLsn(childrens.data[i], tmp_lsn)){ return false;}
		if ( 0 == biggest_lsn){
                        biggest_lsn = tmp_lsn;
                        if(!DecodeZooKeeperSequence(childrens.data[i], smallest_sequence)){ return false;}
                        result_node_string = childrens.data[i];
                        continue;
		}
		if (tmp_lsn >= biggest_lsn){
			if (0 == biggest_lsn){
				biggest_lsn = tmp_lsn;
				if(!DecodeZooKeeperSequence(childrens.data[i], smallest_sequence)){ return false;}
				result_node_string = childrens.data[i];
                        	continue;
			}else{
				if (tmp_lsn > biggest_lsn){
					biggest_lsn = tmp_lsn;
					if(!DecodeZooKeeperSequence(childrens.data[i], smallest_sequence)){ return false;}
					result_node_string = childrens.data[i];
				}else if (tmp_lsn == biggest_lsn){
					if(!DecodeZooKeeperSequence(childrens.data[i], tmp_sequence)){ return false;}
					if(tmp_sequence < smallest_sequence){
						result_node_string = childrens.data[i];
						smallest_sequence = tmp_sequence;
						result_node_string = childrens.data[i];
					}else if (tmp_sequence == smallest_sequence){
						LOG(HCK,("zookeeper appear same sequence node , zookeeper error\n"));
						assert(0);
					}
				}
			}
		}
	}	
	if (NULL == result_node_string){ 
		LOG(HCK,("result_node_string is NULL , cannot be happend\n"));
		assert(0);
		return false;
	}
	 
	if (!DecodeID(result_node_string,biggest_lsn_id)){
		LOG(HCK,("DecodeLsn error\n"));
		return false;		
	}
	if (biggest_lsn_id == my_id_){
		LOG(HCK,("OK, I have become the leader\n"));
		return true;
	}else{
		LOG(HCK,("OK, I am not the leader\n"));
		return false;
	}
}
/*
*
*in tht condition that the leader is not exist
*judge if i am the leader in the  qurom
*find that we also need an epoch
*epoch:xxxx:lsn:xxxx:id:xxxx:seq:xxx
*/
bool
ZooKeeperWatcher::IfIAmLeader2InQuorum(String_vector & childrens){
	U64 tmp_epoch;
	U64 tmp_lsn;
	U64	tmp_sequence;
	
	U64 biggest_epoch;
	U64 biggest_lsn = 0;		// lsn will not choose the "0" , it will choose start "1"
	U32 biggest_lsn_id = 0;
	U64 smallest_sequence =0;
	char *result_node_string = NULL;
	for(U32 i = 0; i != childrens.count; i++){
		if (!DecodeEpoch(childrens.data[i],  tmp_epoch)){ return false;}
		if (!DecodeLsn(childrens.data[i], tmp_lsn)){ return false;}
        	if (!DecodeZooKeeperSequence(childrens.data[i], tmp_sequence)){ return false;}		

		if (NULL == result_node_string ){	//got the first entry
			biggest_epoch = tmp_epoch;
			biggest_lsn = tmp_lsn;
			smallest_sequence = tmp_sequence;
			result_node_string = childrens.data[i];
			continue;
		}
		if (tmp_epoch >=  biggest_epoch){	
			if (tmp_epoch == biggest_epoch){
				if (tmp_lsn > biggest_lsn ){			//some epoch ,but bigger lsn
					biggest_lsn = tmp_lsn;
					smallest_sequence = tmp_sequence;
					result_node_string = childrens.data[i];
					continue;
				}else if (tmp_lsn == biggest_lsn){		//some epoch , some lsn ,but small sequence 
					if (tmp_sequence < smallest_sequence){
						biggest_lsn = tmp_lsn;
						smallest_sequence = tmp_sequence;
						result_node_string = childrens.data[i];	
						continue;
					}
				}
			}else if (tmp_epoch > biggest_epoch){		//bigger epoch 
				biggest_epoch = tmp_epoch;
				biggest_lsn = tmp_lsn;
				smallest_sequence = tmp_sequence;
				result_node_string = childrens.data[i];				
			}
		}
	}
			
	if (NULL == result_node_string){ 
		LOG(HCK,("result_node_string is NULL , cannot be happend\n"));
		assert(0);
		return false;
	}
	 
	if (!DecodeID(result_node_string,biggest_lsn_id)){
		LOG(HCK,("DecodeLsn error\n"));
		return false;		
	}
	if (biggest_lsn_id == my_id_){
		LOG(HCK,("OK, I have become the leader\n"));
		return true;
	}else{
		LOG(HCK,("OK, I am not the leader\n"));
		return false;
	}
}

/*
*it is just use for the items in  the dir not contain dir.(use for one level directory)
*
*/
bool ZooKeeperWatcher::DeleteLeaderElectDir(struct String_vector & childrens){
	int rc;
	char cbuf[100]; 
	LOG(HCK,("-->\n"));

	for(U32 i = 0; i != childrens.count;  i++){
		if (childrens.data[i] != NULL){
			if ( (strlen(election_base_dir_) + strlen(childrens.data[i]) +1)   > sizeof(cbuf)){
				LOG(HCK,("the fullname of the name is too big\n"));
			}
			snprintf(cbuf, sizeof(cbuf), "%s/%s", election_base_dir_, childrens.data[i]);
			rc = zoo_delete(zk_, cbuf, -1);
			if (rc != ZOK){
				if (rc != ZNONODE){
					PrintZooKeeperError(rc);
					deallocate_String_vector(&childrens);
					return false;
				}
			}
		}
	}
	LOG(HCK,("<--\n"));
	return true;	
}

/*
*it is just use for the items in  the dir not contain dir.(use for one level directory)
*
*/
bool ZooKeeperWatcher::DeleteEpochDir(char * dirname){
	int rc;
	char cbuf[100]; 
	LOG(HCK,("-->\n"));
	struct String_vector childrens;

	if (!ZooGetChildren(zk_, dirname, 0, childrens)){
		return false;
	}	

	for(U32 i = 0; i != childrens.count;  i++){
		if (childrens.data[i] != NULL){
			if ( (strlen(epoch_base_dir_) + strlen(childrens.data[i]) +1)   > sizeof(cbuf)){
				LOG(HCK,("the fullname of the name is too big\n"));
			}
			snprintf(cbuf, sizeof(cbuf), "%s/%s", epoch_base_dir_, childrens.data[i]);
			rc = zoo_delete(zk_, cbuf, -1);
			if (rc != ZOK){
				if (rc != ZNONODE){
					PrintZooKeeperError(rc);
					deallocate_String_vector(&childrens);
					return false;
				}
			}
		}
	}	
	deallocate_String_vector(&childrens);	
	LOG(HCK,("<--\n"));
	return true;	
}

bool ZooKeeperWatcher::SetMeTheLeader(void){
	int rc;
	char cbuf[1024];
	LeaderValue   leader_value;
	leader_value.leader_id = my_id_;
	LOG(HCK,("-->\n"));
	strcpy(leader_value.leader_ip, my_ip_);	
	snprintf(cbuf, sizeof(cbuf), "%s/%s",election_base_dir_,LEADER_PREFIX); 
	rc = zoo_create(zk_, cbuf, (char *)&leader_value, sizeof(leader_value), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, 0, 0);
	if (rc != (int)ZOK) {
		LOG(HCK,(" create ZOO_EPHEMERAL node error\n"));
		if (rc == (int)ZNODEEXISTS){
			LOG(HCK,("the the leader have exist, i am not the leader now\n"));
		}
		PrintZooKeeperError(rc);
		LOG(HCK,("<--\n"));
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;
}

bool ZooKeeperWatcher::LeaderElectStart(void){
	static  U64 enter_times = 0;	// soga,it is a static variable
	enter_times++;
	struct String_vector  childrens;
	LockEntry();
	memset(&childrens, 0, sizeof(childrens));
	if ( !CreateBase((char *)ROOT_ELECTION_DIR) ){
		LOG(HCK,("CreateBase error\n"));
		UnLockEntry();
		assert(0);
		return false;
	}	
	if ( !CreateBase((char *)EPOCH_FOR_RANGE_BASE) ){
		LOG(HCK,("CreateBase error\n"));
		UnLockEntry();
		assert(0);
		return false;
	}		
	if ( !CreateBase((char*)election_base_dir_) ){
		LOG(HCK,("CreateBase error\n"));
		UnLockEntry();
		assert(0);
		return false;
	}	
	if ( !CreateBase((char*)epoch_base_dir_) ){
		LOG(HCK,("CreateBase error\n"));
		UnLockEntry();
		assert(0);
		return false;
	}	
	if (!ZooWGetChildren(zk_, election_base_dir_, FindLeaderUpdate, childrens) ){
		LOG(HCK,("ZooWGetChildren error\n"));
		UnLockEntry();
		assert(0);
		return false;
	}	
	print_childrens(&childrens);
	if ( IfLeaderExist(&childrens) ){
		LOG(HCK,("leader is exist\n"));
		if (IfLeaderIsMe()){
			if (enter_times > 1){
				LOG(HCK,("the leader is me, not changed, do nothing\n"))
				//shoud do something?
				SetElectState(ELECT_AT_END);
				UnLockEntry();				
				return true;
			}else{
				LOG(HCK,("i have just came, have do nothing ,can't be the leader\n "));
				assert(0);
			}
		}else{
			if (enter_times == 1){
				LOG(HCK,("follower enter , do catchup and be a follower\n"));
				class PassArgsToFollower pass_args;
				pass_args.init(range_num_,  leader_id_,  leader_ip_);
				if (zk_control_->StartFollower(pass_args) ) {
					LOG(HCK,("the follower threads start ok\n"));
					SetElectState(ELECT_AT_END);
					UnLockEntry();
					return  true;
				}else{
					LOG(HCK,("the follower threads start error\n"));
					assert(0);
					return false;
				}
			}else{
				LOG(HCK,("the leader is not me and not changed, do nothing\n"));	
				SetElectState(ELECT_AT_END);
				UnLockEntry();
				return true;
			}
		}
	}
	//leader is not exist, first we must clear all the old data(that maybe); 
	if ( !DeleteLeaderElectDir(childrens)){
			UnLockEntry();
			assert(0);
			return false;
	}	
	//sleep for a while    //this function will also be call back , the callback mechanism support this?
	WaitAfterResetDir();

	SetElectState(ELECT_AT_MIDDLE);
	//get the correct epoch and sequence i will update
	if (!GetUpdateEpochAndSeq()){
		LOG(HCK,("GetUpdateEpochAndSeq error\n"));
		return false;
	}
	LOG(HCK,("update_epoch():%llu.************************************\n",update_epoch_));
	//then re upload my lsn
	if ( !UpLoadMyNewLsn(update_epoch_, update_sequence_)){
		UnLockEntry();
		assert(0);
		return false;	
	}
	UnLockEntry();
	return true;
}

bool ZooKeeperWatcher::LockEpochDir(void){
	int rc;
	char cbuf[100];
	LOG(HCK,("-->\n"));
	snprintf(cbuf, sizeof(cbuf), "%s/%s",epoch_base_dir_,LOCK_PREFIX); 
	rc = zoo_create(zk_, cbuf, "", 0, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, 0, 0);
	if (rc != (int)ZOK) {
		LOG(HCK,(" create ZOO_EPHEMERAL node error\n"));
		if (rc == (int)ZNODEEXISTS){
			LOG(HCK,("unbelivable , it is locked\n"));
		}
		PrintZooKeeperError(rc);
		LOG(HCK,("<--\n"));
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;
}

bool ZooKeeperWatcher::UnLockEpochDir(void){
	int rc;
	char cbuf[1024];
	LOG(HCK,("-->\n"));
	snprintf(cbuf, sizeof(cbuf), "%s/%s",epoch_base_dir_,LOCK_PREFIX); 
	rc = zoo_delete(zk_, cbuf,-1);
	if (rc != (int)ZOK) {
		LOG(HCK,(" create ZOO_EPHEMERAL node error\n"));
		if (rc == (int)ZNONODE){
			LOG(HCK,("unbelivable , it have been  unlocked\n"));
		}
		PrintZooKeeperError(rc);
		LOG(HCK,("<--\n"));
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;
}

/*
*key:   epoch:xxxx:endseq:xxxx
*/
inline bool ZooKeeperWatcher::DecodeEpoch(char * s , U64 & epoch_value){
	U64 value;
	U32 len;
	char *start;
	char *end;
	char *endptr;
	char cbuf[40];
	if (s == NULL){
		assert(0);
		return false;
	}
	start = strchr(s, 'h');
	if (start != NULL) {
		end =  strchr(start + 2 , ':');
		if(end != NULL){
			len = end - start -2;
			strncpy(cbuf, start+2, len);
			cbuf[len] = '\0';
			value  = strtoull(cbuf, &endptr, 10);
			epoch_value = value;
		}else{
			return false;
		}
	}else{
		return false;
	}		
	return true;
}

/*
*key:   epoch:xxxx:endseq:xxxx
*/
inline bool ZooKeeperWatcher::DecodeEndSeq(char * s , U64 & endseq){
	U64 value;
	char *start;
	char *endptr;
	if (s == NULL){
		assert(0);
		return false;
	}
	start = strchr(s, 'q');
	if (start != NULL) {
		value  = strtoull(start + 2, &endptr, 10);
		endseq = value;
	}else{
		return false;
	}		
	return true;
}

bool  EpochCompare(U64 i , U64 j ){
	return (i < j);
}

bool ZooKeeperWatcher::ExamEpochVectorOk(U64 new_epoch){
	std::vector<U64>  epochs;
	U64 tmp_epoch;
	struct String_vector childrens;
	if (!ZooGetChildren(zk_, epoch_base_dir_, 0, childrens)){
		return false;
	}	
	
	for(U32 i = 0; i != childrens.count; i++){
		if(childrens.data[i]){
			if (DecodeEpoch(childrens.data[i], tmp_epoch)){
				epochs.push_back(tmp_epoch);
			}else{		
				continue;
			}
		}
	}
	
	std::sort(epochs.begin(), epochs.end(), EpochCompare);
	if (epochs.empty()){
		LOG(HCK,("leaderfailover have't  happen before\n"));
		if(0 == new_epoch){
			return true;
		}else{
			return false;
		}
	}else{
		std::vector<U64>::iterator iter = epochs.begin();
		tmp_epoch = *iter;
		iter++;
		for (; iter != epochs.end(); iter++){
			if ( *iter  == tmp_epoch + 1 ){
				tmp_epoch = *iter;
				continue;
			}else{
				LOG(HCK,("the epoch vector  item is not increase ont by one\n"));
				return false;
			}	
		}
		if (new_epoch  == tmp_epoch +1){
			return true;
		}else{
			LOG(HCK,("new_epoch:%llu,\ttmp_epoch:%llu.\n",new_epoch,tmp_epoch));
			return false;
		}
	}		
}

/*
* the format of the epoch vector item is --> epoch:xxx:endseq:xxxx
*  
*
*/
inline bool ZooKeeperWatcher::UpLoadNewEpochVectorItem(U64 new_epoch, U64 end_sequence){
	int rc;
	char  node_name[100];
	LOG(HCK,("-->\n"));	
	
//	snprintf(node_name, sizeof(node_name), "%s/endseq:%llu:epoch:%llu",epoch_base_dir_,
//		end_sequence,new_epoch);
	snprintf(node_name, sizeof(node_name), "%s/epoch:%llu:endseq:%llu",epoch_base_dir_,
		new_epoch, end_sequence);
	rc = zoo_create(zk_, node_name, "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);	
	if (rc != ZOK){
		PrintZooKeeperError(rc);
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;	
}

bool ZooKeeperWatcher::UpdateEpochVector(U64 new_epoch, U64 end_sequence){
	//lock
	if(!LockEpochDir()){
		LOG(HCK, ("error have been locked\n"));
		return false;
		//assert(0);
	}
	//exam
	if(!ExamEpochVectorOk(new_epoch) ){
		LOG(HCK,("Epoch Vector error, system error\n"));
		return false;
		//assert(0);	
	}
	//upload
	if (!UpLoadNewEpochVectorItem(new_epoch, end_sequence)){
		LOG(HCK,("UpLoadNewEpochVectorItem error"));
		return false;
		//assert(0);	
	}
	//unlock
	if(!UnLockEpochDir()){
		LOG(HCK, ("error have been locked\n"));
		return false;
		//assert(0);
	}	
	return true;
}

// get the epoch number and the biggest epoch and it's corresponding sequence from zookeeper server
bool ZooKeeperWatcher::_GetLastItem(int32_t &epoch_num, U64 &epoch, U64 &sequence){
	std::vector<U64>  epochs;
	U64 tmp_epoch;
	U64 max_epoch;
	char *lastitem = NULL;
	struct String_vector childrens;
	if (!ZooGetChildren(zk_, epoch_base_dir_, 0, childrens)){
		return false;
	}	
	epoch_num = 0;
	for(U32 i = 0; i != childrens.count; i++){
		if(childrens.data[i]){
			LOG(HCK,("%s\n", childrens.data[i]));
			if (DecodeEpoch(childrens.data[i], tmp_epoch)){
				epoch_num++;
				if ( NULL == lastitem ){
				   max_epoch =  tmp_epoch;
				   lastitem = childrens.data[i];				   
				   continue;
				}
				if (tmp_epoch > max_epoch){
					max_epoch = tmp_epoch;
					lastitem = childrens.data[i];
				}		
			}else{		
				continue;
			}
		}
	}	
	if (  NULL == lastitem ){
		//epoch = -1;  //represent that here is no  epoch items in the zookeeper
		LOG(HCK,("the zookeeper epoch vector is empty\n"));
		return true;
	}
	LOG(HCK,("last item:%s\n",lastitem));	
	epoch  = max_epoch;
	if (!DecodeEndSeq(lastitem, sequence)){
		LOG(HCK,("DecodeEndSeq err\n"));
		return false;
	}
	return true;	
}

bool ZooKeeperWatcher::GetLastItem(int32_t &epoch_num, U64 &epoch, U64 &sequence){
	//lock
	bool ret = true;
	if(!LockEpochDir()){
		LOG(HCK, ("error have been locked\n"));
		//ret = false;
		return false;
	}
	if ( !_GetLastItem(epoch_num, epoch, sequence)){
		LOG(HCK, ("_GetLastItem err\n"));
		ret = false;
	}
	//unlock
	if(!UnLockEpochDir()){
		LOG(HCK, ("error can't unlock\n"));
		ret = false;
	}		
	return ret;
}

/*
*who will execute the callback function, when the condition triggered
*
*/
// this function has a close relationship with LeaderElectStart() function; these two functions call each other
static void FindLeaderUpdate(zhandle_t *zk, int type, 
			int state, const char *path, void *watcherCtx) {
	class ZooKeeperWatcher * zk_watcher = (class ZooKeeperWatcher *) watcherCtx;
	String_vector   childrens  = {0,NULL};
	PrintZKCallBackErrors(zk, type, state, path, watcherCtx);
	zk_watcher->LockEntry();
	LOG(HCK,("-->\n"));
	//fixme here , the timeout/disconnct conditions will also  trigger this callback function
	/*
	if (strcmp(zk_watcher->election_base_dir(), path) != 0 ){
		LOG(HCK,("The call back path is not right, maybe , the session callback is error!"));
		assert(0);   //see if got the right trigger	
	}
	*/
	if (zk_watcher->TestElectState(ELECT_AT_END)){
		//it is a new leader failure,we first shoud clear the old state, as i am i am a new join
		zk_watcher->UnLockEntry();
		if (!zk_watcher->LeaderElectStart()){
			LOG(HCK, ("LeaderElectStart error\n"));
			assert(0);
		}else{
			return;
		}
	}
	// so it is in the  middle of elect , do not need the clear again, have clear old items before
	//get the childrens  and set a watch 
	if (!zk_watcher->ZooWGetChildren(zk_watcher->zookeeper_handle(),
								zk_watcher->election_base_dir(), FindLeaderUpdate,  childrens))
	{
		LOG(HCK,("ZooWGetChildren error\n"));
		zk_watcher->UnLockEntry();
		assert(0);		
	}
	print_childrens(&childrens);
	// check if the leader is exist
	if ( zk_watcher->IfLeaderExist(&childrens) ) {
		LOG(HCK,("leader is exist, connect the leader, and do catchup\n"));
		zk_watcher->SetElectState(ELECT_AT_END);
		/*
		*start do the catchup and  become a follower
		*(because the follower can be changeable when it is ok(because of leadfail))
		*so	shoud check the former state, and do some thing 
		*/
		// stop the former follower
		zk_watcher->StopFormerFollower();
		//start the new follower
		if (!zk_watcher->StartNewFollower()) {
			LOG(HCK,("StartNewFollower error assert\n"));
			LOG(HCK,("if i shoud choose other ways?\n"));
			assert(0);
		}else{
			LOG(HCK,("StartNewFollower ok\n"));			
		}
		LOG(HCK,("<--\n"));
		zk_watcher->UnLockEntry();
		return ;
	}	
	//the leader is not exist
	 if (!zk_watcher->ExistQuorum(childrens)){
	 	// is there haven't  form  a quorum, check if my "lsn"node is exist, if not exist reupload. 
		if (!zk_watcher->CheckMyExistenceAndReUpload(childrens)){
			assert(0);
		}
		LOG(HCK,("<--\n"));
		zk_watcher->UnLockEntry();
		return;
	 }
	// ok leader is not exist , but there have formed an  quorum, check if i can be the newleader
	if (!zk_watcher->IfIAmLeader2InQuorum(childrens)){	// 我不是leader
		LOG(HCK,("there is an qurom, but i am not the leader\n"));
		LOG(HCK,("when the leader create the leader node, i will kown the leader, and do something"))
		//wait the future leader to set the "leader node"
		LOG(HCK,("<--\n"));
		zk_watcher->UnLockEntry();	
		return;
	}else{						// 我是leader
		//find that i can be the leader
		LOG(HCK,("there is an qurom, and i can be  leader, i will create  the leader node\n"));
		// if i am the leader before stop the former follower
		zk_watcher->StopFormerFollower();
		// let the other follower know, i am the leader		
		if (zk_watcher->SetMeTheLeader()){
			LOG(HCK,("Set Me the leader ok, now i can be the leader\n"));
			zk_watcher->SetElectState(ELECT_AT_END);
			// init some leaderfailoverinfo before  leaderfailover
			zk_watcher->InitLeaderFailoverInfo();
			// let the leader do failover	
			if (!zk_watcher->StartLeaderFailover(&flws)){
				LOG(HCK,("StartLeaderFailover error assert\n"));
				LOG(HCK,("if i shoud choose other ways?\n"));				
				assert(0);
			}else{
				LOG(HCK,("ok,leader failover success\n"));
			}
			//up the  other leader threads 
			if (!zk_watcher->StartNewLeader()){
				LOG(HCK,("StartLeaderFailover error assert\n"));
				LOG(HCK,("if i shoud choose other ways?\n"));				
				assert(0);
			}else{
				LOG(HCK,("ok,StartNewLeader ok \n"));				
			}	
			zk_watcher->UnLockEntry();
			return;
		}else{
			LOG(HCK,("Set Me the leader error\n"));
			LOG(HCK,("<--\n"));
			zk_watcher->UnLockEntry();
			assert(0);
			return;
		}		
	} 
}

bool ZooKeeperWatcher::RecoveryData(U64 begin_seq,U64 end_seq) {
	logical_truncated_table_->LogicalTableWarmUp();
	std::string key;	
	std::string value;
	U64 epoch;
	CatchupItem citem;
	if(end_seq <= 0) {
		LOG(HCK, ("needn't to local recovery.\n"));
		return true;
	}
	if(begin_seq <0) {
		LOG(HCK, ("recovery error, begin_seq should >0.\n"));
		assert(0);
		return true;
	}
	if (0 == begin_seq)
		begin_seq = 1;
	for(U64 j=begin_seq;j <= end_seq; j++) {
		if(logical_truncated_table_->GetEpochOfTheSeq(j, epoch)) {
			citem.pn.epoch = epoch;
			citem.pn.sequence = j;
			if(logstore.CatchUp(& citem, -2)) {	//-2 represent fd
				key = citem.kv.key;
				value = citem.kv.value;
				if(datastore.WriteData(key, value, j)!=true) {
					LOG(HCK,("insert leveldb error\n"));
					assert(0);
				}
			}
			else {
				LOG(HCK,("catch up epoch:%llu,sequence:%llu,key:%s error.\n",epoch,j,citem.kv.key));
				assert(0);
			}
		}
		else {
			LOG(HCK,("get epoch from logicaltruncated table error\n"));
			assert(0);
		}
	}
	logstore.CatchUpEnd(-2);
	LOG(HCK,("local recovery data ok\n"));
	return true;
	
}

bool ZooKeeperWatcher::LocalRecovery() {
	U64 begin_seq,end_seq;
	ProposalNum pn;
	U32 rangenum;
	int ml_flag = mlstore.GetCommitPn(rangenum, pn);		// fix me
	begin_seq = datastore.LocalRecovery();

	if(ml_flag==GETOK&&pn.sequence!=0) {
		end_seq = pn.sequence;
		if(begin_seq > 0 && begin_seq <= end_seq) {
			if(RecoveryData(begin_seq,end_seq)) {
				LOG(HCK,("local recovery ok\n"));
				return true;
			}
			else {
				LOG(HCK,("local recovery error\n"));
				return false;
			}
		}
		// here,may be appear begin_seq > end_seq ,this because last time ,the cmt hasn't write into cmt file,so this time ,we get a false cmt
		if(begin_seq > end_seq) {
			LOG(HCK,("the last cmt is not true. but it doesn't affect our project.\n"));
			return true;
		}
		//else{
		//	LOG(HCK,("leveldb wrong\n"));
		//	assert(0);
		//}
	}
	else {
		LOG(HCK,("no data need to recovery\n"));
		return true;
	}
}

/*
*every instance have is own working directory 
*/

bool IfIShouldStart(int myid,class RangeItem  rangeitem) {
	int i;
	LOG(HCK,("MYID:%d.\n",myid));
	for(i = 0;i < rangeitem.servers_.size(); i++){
		LOG(HCK,("SERVERID:%d.\n",rangeitem.servers_[i]));
		if(myid == rangeitem.servers_[i])
			return true;
	}
	return false;
}

bool   ChangeWoringDir(std::string base, int rangenum) {
	char path[400];
	char cmd[500];
	sprintf(path, "%s/Range%d", base.c_str(), rangenum);
	if ( -1 == access(path,F_OK) ){
		sprintf(cmd,"mkdir -p %s", path);
		if ( system(cmd) != 0){
			return false;
		}		
	}else{
		printf("store path is exist\n");
	}		
	
	if ( -1 == chdir(path) ){
		return false;
	}else{
		return true;
	}
}

int main(int argc, char* args[]){
	//set the process to deamon
	pid_t pid = fork();
        if ( pid <  0 ){
                LOG(HCK,("fork error errorcode = %d[%s]\n",errno, strerror(errno)));
                return -1;
        }
        if (pid != 0){
                exit(0);
        }
       setsid();

	
	//get the args
	if (argc  < 3){
		LOG(HCK,("arg  num is error: %d %s\n", argc, args[0]));	
		
		return 0;
	}
	//get the range number
	char* rangestr = args[1];
	int rangenum = atoi(rangestr);

	//get the recovery model
	char* ifrecoverptr = args[2] ;
	int 	if_recovery =0 ;
	if_recovery = atoi(ifrecoverptr);
	if ( if_recovery == 0 || if_recovery == 1) {
		//do nothing
	}else{
		LOG(HCK,("the recovery model is error.\n"));
		return -1;
	}
	
	
	//change the working directory accorrding to the rangenum

	class RangeTable table;
	if( table.init()){
		LOG(HCK, ("table init ok\n"));
		//return 0 ;
	}else{
		LOG(HCK,("table init error\n"));
		return -1;
	}

	if ( !ChangeWoringDir(table.StorePath(), rangenum) ) {
		return -1;
	}

	class RangeItem  rangeitem  = table.GetRangeItem(rangenum); 
	if (!IfIShouldStart(table.my_id(), rangeitem)) {
		LOG(HCK,("this server needn't manage this key range data.\n"));
		assert(0);
	}
	sleep(2);	//if it one down and restart, this can ensure all the ZOO_EPHEMERAL info of this node in the zookeeper cluster have diappear
	//int if_recovery=0;
	//LOG(HCK,("please input if_recovery(0/1):\n"));
	//scanf("%d",&if_recovery);
	logstore.log_do_recovery = if_recovery;
	logstore.LogManagerInit();
	mlstore.set_meta_log_do_recovery(if_recovery);
	mlstore.MetaStoreInit();
	datastore.set_store_log_do_recovery(if_recovery);
	datastore.StoreInit();
	datastore.Open();
	//logical_truncated_table_do_recovery = if_recovery;
	if(if_recovery==0){
		logical_truncated_table_do_recovery = 0;
		InMemLogicalTable Imtable;
		Imtable.LogicalTableWarmUp();
		logical_truncated_table_do_recovery = 1;
	}
	class ZooKeeperWatcher*  zkwatcher  = new ZooKeeperWatcher(&mlstore, &logstore, table, rangenum);
	if (!zkwatcher->Init()) {
		LOG(HCK,("zkwatcher init error\n"));
		return 0;
	}
	if(if_recovery==1){
		zkwatcher->LocalRecovery();
	}
	//zkwatcher->logical_truncated_table_->LogicalTableWarmUp();
	zkwatcher->Start();
	return 0;
}
