/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: leader reads the data that follower catchup request and send back to follower
*/

#include "leadercatchup.h"

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>

#include "tools.h"
#include "pqueue.h"
#include "proposer.h"
#include "followerlist.h"
#include "frecover.h"
#include "log_manager.h"
#include "logworker.h"
#include "logicaltruncatedtable.h"

bool ifsendfollowerlist = false;	// to tell leaderread thread to send leader's followerlist ip to client

LeaderCatchup::LeaderCatchup():port_(LEADER_CATCHUP_PORT_NUM){
	
	citem_  = new class CatchupItem();
	if(citem_ == NULL){
		assert(0);
	}
	base_ = event_base_new();
	if (!base_){
		LOG(HCK,("event_base_new error\n"));
		exit(0);
	}
	logical_table  = NULL;
}
	
LeaderCatchup::~LeaderCatchup(){
	if(citem_ != NULL){
		delete citem_;
	}
	if(base_){
		event_base_free(base_);
	}	
	if (logical_table != NULL){
		delete logical_table;
	}
}

bool LeaderCatchup::Init(void){
	logical_table = new InMemLogicalTable();
	if (logical_table == NULL){
		LOG(HCK,("new InMemLogicalTable error\n"));
		assert(0);
	}
	//read the loggical table to the mem from the db
	if (!logical_table->LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n "))
		assert(0);
	}

	if (base_ !=NULL){
		if( !ListenerInit( listenfd_, port_)) {
			LOG(HCK,("Error: tcp server start error\n"));
			return false;
		}
	}else{
		return false;
	}
	return true;
}

bool LeaderCatchup::Start(void){
	event_set(&ev_accept_, listenfd_, EV_READ | EV_PERSIST, LeaderCatchupOnAccept, this);
	event_base_set(base_,&ev_accept_);
	event_add(&ev_accept_, NULL);
	event_base_dispatch(base_);	
	return true;
}
/*
*	the catchup msg encode format : total_len:msg_type:sequence:
*								U32		U32		U64
*	(the total_len have been used)
*/
inline bool LeaderCatchup::DecodeCatchupMessage(char *buf, U64 &catchup_sequence){
	U32 msg_type;
	memcpy((void *)&(msg_type), buf, sizeof(msg_type));
	msg_type = ntohl(msg_type);
	buf += sizeof(msg_type);

	if (msg_type != CATCHUP_MESSAGE){
		return false;
	}
	memcpy((void *)&(catchup_sequence), buf, sizeof(catchup_sequence));
	catchup_sequence = Ntohl64(catchup_sequence);
	buf += sizeof(catchup_sequence);
	return true;
}

/*
*catchup response message size
*encode format:
				total_len:msg_type:proposal numbers:endflag:[flag?:epoch:sequence:[ketlen:valuelen:key:value][.....][]] 
				    U32	U32			U32			u8        u8
*/
//here their is mang things we shoud take into consideration, include where to find the proposal(log file ?)(pqueue?)
// and we also shoud need the MetaLogStore infor
//
inline 
bool 
LeaderCatchup::EncodeCatchupAckHeader(char *buf, U32 total_len,  U32 proposal_numbers, U8 endflag){
	U32 netformat32;
	U32 total_len_value = total_len - sizeof(U32);
	netformat32 = htonl(total_len_value);
	memcpy((void *)buf, (void *)&netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);	

	netformat32 = htonl(CATCHUP_MESSAGE_ACK);
	memcpy((void *)buf, (void *)&netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);	

	netformat32 = htonl(proposal_numbers);
	memcpy((void *)buf, (void *)&netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);	
	
	memcpy((void *)buf, (void *)&endflag, sizeof(endflag));
	return true;	
}

inline bool LeaderCatchup::EncodeCatchupAckBody(char *buf, class CatchupItem *citem,U32 &remain_size, U32 &use_size){
	U32 netformat32;
	U64 netformat64;
	use_size  = citem->citem_encode_size();	
	if(use_size > remain_size){
		return false;
	}else{
		remain_size  -= use_size;
	}
	//LOG(HCK,("the epoch%llu\n",citem->pn.epoch ));
	//LOG(HCK,("the sequence%llu\n",citem->pn.sequence));
	
	memcpy((void *)buf, (void *)&citem->ifsynced, sizeof(citem->ifsynced));
	buf += sizeof(citem->ifsynced);

	netformat64 = Htonl64(citem->pn.epoch);
	memcpy((void *)buf, (void *)&netformat64, sizeof(netformat64));
	buf += sizeof(netformat64);
		
	netformat64 = Htonl64(citem->pn.sequence);
	memcpy((void *)buf, (void *)&netformat64, sizeof(netformat64));
	buf += sizeof(netformat64);

	netformat32 = htonl(citem->key_len);
	memcpy((void *)buf, (void *)&netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);	

	netformat32 = htonl(citem->value_len);
	memcpy((void *)buf, (void *)&netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);	

	// here we shoud pass the "key_len" and "value_len" in the citem->buf
	memcpy(buf,citem->kv.key,citem->key_len);
	buf += citem->key_len;
	memcpy(buf,citem->kv.value,citem->value_len);
	//memcpy(buf, (void *)(citem->buf  + sizeof(citem->key_len ) + sizeof(citem->value_len)),
	            //citem->key_len + citem->value_len);
	return true;	
}

inline U32 LeaderCatchup::EncodeHeaderSize(void){
	U32 size;
	size =  sizeof(U32) + //total_len
		   sizeof(U32) + //msg_type
		   sizeof(U32) + //proposal numbers
		   sizeof(U8);    //endflag
	return size;
}

/*
*the send catchup message shoud do many things
*(1):find where to get the proposal(the proposal number is 'catchup_sequence')
*(1):when the follower  catch up closer, the leader shoud wait for serveral time, to let the follower catche up ok
*(2):when the cache up completed,shoud add the follower to the send list and wake up the  proposal thread 
*	that maybe in the SendProposals
*(3):we may also shoud update the p12info
*/
bool
LeaderCatchup::SendCatchupMessageAck(U64 catchup_sequence, char* catch_buf, 
											U32 len, ClientEvent * ce){ 
	logical_table->LogicalTableWarmUp();
	U32 header_size = EncodeHeaderSize();
	//class CatchupItem *citem  = new class CatchupItem();
	U32 item_num = 0;
	U32  remain_size = len - header_size;
	U32 use_size;
	char *buf = catch_buf + header_size;
	bool flag;
	if (Global_P12Info.next_unused_sequence() ==  catchup_sequence){
		//try to send endflag, because of the follower may have been catchup
		if (( flag = SendCatchupEndflag(catch_buf, catchup_sequence, ce))){
			logstore.CatchUpEnd(ce->fd_);
			LOG(HCK,("leader send catchup endflag ok\n"));
			close(ce->fd_);
			event_del(&ce->ev_read_);
			delete ce;
			return true;
		}else if (flag == false && ce->active_ == false) {
			//ce->lock(); //there is no race condition here
			close(ce->fd_);
			event_del(&ce->ev_read_);
			delete ce;
			return false;
		}else{
			// do noting , there have new proposals,  go on catcheup
			LOG(HCK,("go on catch up\n"));
		}	
	}	
	//fixme, if the LEADER_WAIT_NUM is too small or to bigger
	if (1){
		bool stopflag_sended = false;
		for (int i =  0; remain_size > 0 ; i++) {	
			if (stopflag_sended == false) {
				if (Global_P12Info.next_unused_sequence() < LEADER_WAIT_NUM \
					||	catchup_sequence  >=  (Global_P12Info.next_unused_sequence() - LEADER_WAIT_NUM))
				{
					//send a msg to the proposal ,ask it to stop,then proposal  will wait some time before another 
					//kv choosed as an new proposal
					pcmsger.SetStopFlag();	
					//pcsockpair.SendMessage();
					stopflag_sended = true;
				}		
			}
			if ( GetProposalBySequence(catchup_sequence, citem_,ce->fd_)) {
				if ( EncodeCatchupAckBody(buf, citem_, remain_size, use_size) ){
					buf += use_size;
					item_num++;
					++catchup_sequence;
				}else{
					LOG(HCK,("item_num = %d\n",item_num));
					break;
				}
			} else {
				LOG(HCK,("GetProposalBySequence return false\n"))
				break;
			}
		}
		EncodeCatchupAckHeader(catch_buf,  len -remain_size, item_num, 0); //encode the header
		if ( TcpSend(ce->fd_, catch_buf,  len -remain_size) ){
			LOG(HCK,("send to catch up proposals to  follower ok\n"));
			return true;
		}else{
			LOG(HCK,("catch up send error,close connection\n "));
			close(ce->fd_);
			event_del(&ce->ev_read_);
			ce->active_ = false;	
			delete ce;
			return false;
		}	
	}
	
}
/*
*notify the follower that it have been catchup, add it to the followerlist
*
*to ensure that the the next proposal is send after the  
*the   follower is   acctualy add to the the followerlist(and the follower is ready for receive new proposals)
*/
bool 
LeaderCatchup::SendCatchupEndflag(char *catch_buf, U64 catchup_sequence, ClientEvent *ce){
	U32 total_len = EncodeHeaderSize();
	char ip[64];
	flws.LockCatchupMutex();
	if (Global_P12Info.next_unused_sequence() ==  catchup_sequence){
		EncodeCatchupAckHeader(catch_buf, total_len, 0, 1);
		if ( TcpSend(ce->fd_, catch_buf, total_len) ){
			//send to the follower notify it have catched up
			class FollowerInfo *finfo = new  class FollowerInfo();		
			//snprintf(ip, sizeof(ip), "%s", inet_ntop(AF_INET, &ce->client_addr.sin_addr, ip, sizeof(ip)) );
			if (inet_ntop(AF_INET, &ce->client_addr_.sin_addr, ip, sizeof(ip)) == NULL){
				LOG(ERR,("inet_ntop error\n"));
				delete finfo;
				flws.UnlockCatchupMutex();
				return false;
			}
			LOG(HCK,("the new follower ip is %s\n",ip));
			//give the follower time to receive the proposal -->so to wait
			sleep(LEADR_WAIT_CATCHUP_SUCCESS);
			finfo->Init(0, ip); 	//fixme follower id is not used here
			flws.LockFlist();
			flws.NewFollowerEnqueue(finfo);  //add the catchuped follower to the followerlist
			flws.UnlockFlist();
			flws.UnlockCatchupMutex();
			
			ifsendfollowerlist = true;

			return true;
		}else{
			ce->active_ = false;
			flws.UnlockCatchupMutex();
			LOG(HCK,("catchup follower disconnect\n"));	
			return false;
		}
	}else {
		LOG(HCK, ("the proposal have send more\n"));
		flws.UnlockCatchupMutex();
		return false;
	}
}

bool LeaderCatchup::GetProposalBySequence(U64 seq,  class  CatchupItem *citem,int fd) {
	U64 epoch;
	bool flag;
	epoch = GetEpochFromMetafile(seq);
	ProposalNum pn;
	pn.epoch = epoch;
	pn.sequence = seq;
	citem->pn = pn;
	if (seq  <= Global_P12Info.lsn_sequence()){	//get from the log_store;	
		citem->ifsynced = 1;	
		//flag = logstore.LogGetProposal(citem);    
		flag = logstore.CatchUp(citem, fd);
		if(!flag) {
			LOG(HCK,("catch up epoch:%llu,sequence:%llu error.\n", epoch, seq));
			assert(0);
		}
		return flag;
	} else {									//get it from pqueue
		citem->ifsynced = 0;
		flag  = Global_ProposalQueue.GetQproposal(citem);
		return flag;
	}
}

//fix me the logical turncated metafile shoud be get here to return the correct epoch
U64 LeaderCatchup::GetEpochFromMetafile(U64 seq) {
	U64 epoch;
	if (!logical_table->GetEpochOfTheSeq(seq, epoch)){
		LOG(HCK,("GetEpochOfTheSeq error\n"));
		assert(0);
	}
	return epoch;
}

void LeaderCatchupOnAccept(int fd, short ev, void * arg){
	LeaderCatchup *lc = (class LeaderCatchup *)arg;
	int follower_fd;
	int flags;
	struct sockaddr_in follower_addr;
	socklen_t follower_len = sizeof(follower_addr);
	follower_fd = accept(fd, (struct sockaddr*)&follower_addr, &follower_len);
	if (follower_fd == -1) {
		LOG(HCK,("Error:accept client failed\n"));
		return;
	}
	
	if ( (flags = fcntl(follower_fd, F_GETFL, 0) < 0) ||
		fcntl(follower_fd, F_SETFL, flags | O_NONBLOCK) < 0 ){
		LOG(HCK,( "Error:: set follower socket  O_NONBLOCK error\n"));
		close(follower_fd);
		return;
	}
	ClientEvent* ce = new class ClientEvent(follower_fd, &follower_addr);  //c++ fromat
	LOG(HCK,( "new ce\n")); 
	PassArg * pa = new PassArg((void*)ce, (void*)lc);
	// logstore.CatchUpEnd(follower_fd);
	event_set(&ce->ev_read_, follower_fd, EV_READ | EV_PERSIST, LeaderCatchupOnRead, (void *)pa);
	event_base_set(lc->base_, &ce->ev_read_);
	event_add(&ce->ev_read_, NULL);	//if receive a request before event register
}

void LeaderCatchupOnRead(int fd, short ev, void * arg){
	int errcode = 0;
	PassArg * pa = (PassArg *)arg;
	ClientEvent *ce = (ClientEvent *)pa->first();
	LeaderCatchup *lc = (LeaderCatchup *)pa->second();
	char buf[CATCHUP_MESSAGE_SIZE];
	ssize_t nsize;
	nsize = ReadvRec(fd,  buf, CATCHUP_MESSAGE_SIZE, errcode);
	if ( errcode == EMSGSIZE ){
		LOG(ERR,("follower elegantly disconnect\n"));
		return;
	}	
	if (0 == nsize){
		LOG(HCK,("follower elegantly disconnect\n"));
		close(ce->fd_);
		event_del(&ce->ev_read_);
		delete ce;
		delete pa;
		return;
	}else if (nsize < 0) {
		LOG(HCK,("follower failed disconnect\n"));	
		close(ce->fd_);
		event_del(&ce->ev_read_);
		delete ce;
		delete pa;
		return;
	}else{
		U64 catchup_sequence;
		if (!lc ->DecodeCatchupMessage(buf, catchup_sequence)){
			LOG(ERR, ("non catchup message recivered\n"));
			return;
		}		
		logstore.CatchUpEnd(ce->fd_);
		lc ->SendCatchupMessageAck(catchup_sequence,lc->catch_up_ack_buf_, CATCHUP_RETURN_ACK_SIZE, ce);
		return;
	}
}
