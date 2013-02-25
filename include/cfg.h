/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: all kinds of parameters
*/

#ifndef PAXSTORE_CFG_
#define PAXSTORE_CFG_
#include <sys/types.h>
#include <stdio.h>

extern int  DEFAULT_PROT_NUM;
extern int  LEADER_CATCHUP_PORT_NUM;
extern int  FOLLOWERPORT;
extern int  READ_PORT_NUM;

#define MAX_KV_SIZE  10000
#define MAX_TCP_MSG_SIZE  MAX_KV_SIZE + 100
#define MAX_READWRITE_BUFSIZE   MAX_KV_SIZE + 50   
//#define FOLLOWERPORT 6661
//#define DEFAULT_PROT_NUM  6666
//#define READ_PORT_NUM 6667
//#define LEADER_CATCHUP_PORT_NUM 6662

#define BACKLOG  10
#define MAXTRYCONNECTTIME 2

#define CLOSED   1
#define CONNECTING   2
#define CONNECTED   3
#define NEWITEM   4
#define ZOOKEEPER_DELETED   5    // it is used by the zookeeper client 

#define  CONNECT_FAIL 		0
#define  FOLLOWER_CLOSE 		1
#define  NEW_CONNECT  		2
#define  RE_CONNECT 			3	
#define  CONNECT_OK 			4    
#define  DELETING				5

#define CATCHUP_MESSAGE  6
#define CATCHUP_MESSAGE_SIZE 24
#define CATCHUP_MESSAGE_ACK 7
#define CATCHUP_RETURN_ACK_SIZE   (MAX_TCP_MSG_SIZE) * 3    //max three times bigger 

#define PROPOSAL_MESSAGE 16
#define PROPOSAL_MESSAGE_ACK 17
#define COMMIT_MESSAGE	18
#define COMMIT_MESSAGE_ACK 19

#define LEADER_FAILOVER_SUCCESS_MSG 20
#define LEADER_FAILOVER_SUCCESS_MSG_ACK 21

#define SENDREAD_MESSAGE 22
#define SENDFOLLOWERIP_MESSAGE 23

//the leader return 
#define  BUSY -1
#define ERROR  -2	
#define COMMITED 0

#define VERBOSITY_LEVEL 2    // this shoud be  > 0
#define ERR 0
#define  HCK 1
#define VRB 2
#define DBG 3

#define LOG(L, S) if(VERBOSITY_LEVEL >= L) {\
   printf("[%s] ", __func__) ;\
   printf S ;\
   fflush(stdout) ;\
}
/*
void
LOG(int32_t L, char* s){
	if(VERBOSITY_LEVEL >= L) {
		printf("[%s] ", __func__) ;
   printf S ;
	}
}
*/
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
	TypeName(const TypeName&);				\
	void operator=(const TypeName&)

// define the log format parameters
enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,
  kFullType = 1,
  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;
static const int kBlockSize = 32768;
static const int MaxLogSize = 10*1024*1024;
static const int kHeaderSize = 4 + 1 + 2;

#define DBNAME "./db"
#define LEVELDB_MEMTABLE_SIZE 10*1024*1024

#define GETERROR   -1
#define GETEMPTY    0
#define GETOK		  1
#define cmt_filename_  "./meta/cmt"	// initialize the cmt file name

#define max_follower 5

#define ROOT_ELECTION_DIR    "/LeaderElection"
#define LEADER_PREFIX	"Leader"
#define LSN_SEQUENCE   1
#define MY_EPOCH		0 
#define EPOCH_FOR_RANGE_BASE    "/EpochForRanges"		
#define LOCK_PREFIX    "lock"

#define RANGE_NUM 1
#define MY_ID 	   3
#define MY_IP 	"192.168.3.23"
#define SLEEP_AFTER_CLEAR 3

#define RECV_CLIENT_TIME 2000
#define PACKAGESIZE  8050

/* recv_timeout:The maximum amount of time that can go by without 
     receiving anything from the zookeeper server */
#define  RECV_TIME 2000

#define  ZOOKEEPER_NODE_ADDRESS "192.168.3.21:2180,192.168.3.21:2181,192.168.3.21:2182"

#define  TOTAL_NUM		3	//the total number of copy
#define QUORUM_NUM		(TOTAL_NUM/2 + 1) 

#define WAIT_TIME   3
#define LEADER_FAILOVER_SUCCESS_MSG_SIZE 24

#define LOGICAL_TRUNCATED_TABLE_DB_PATH	"./logical"
#define LOGICAL_TRUNCATED_TABLE_DB_FILE_NAME	"./logical/truncatedtable"
#define LOGICAL_TRUNCATED_TABLE_NUM_FILE_NAME    "./logical/num"

#define CMT_FILENAME  "./meta/cmt"	// initialize the cmt file name
#define MANIFEST_NAME  "./meta/manifest"		// manifest name
#define LOG_DIR "./log/"		// log directory name
#define META_DIR "./meta/"

#define MAX_PARALLEL	10000
#define PROPOSAL_QUEUE_SIZE 10000

#endif
