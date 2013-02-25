/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#include "commitsyncworker.h"

#include "proposer.h"
#include "metalogstore.h"

CommitSyncer::CommitSyncer(){
	base_ = event_base_new();
	if (NULL == base_){
		assert(0);
	}
}

CommitSyncer::~CommitSyncer(){
	if (base_){
		event_base_free(base_);
	}
}

void CommitSyncer::Start(void){
	struct timeval tv;
	tv.tv_sec = COMMIT_TIMEOUT_SECS;
	tv.tv_usec = 0;
	event_set(&commit_num_event_, cmtprosockpair.GetReadSock(),  EV_READ|EV_PERSIST,
			 CommitSyncerCb, this);
	event_base_set(base_, &commit_num_event_);
	event_add(&commit_num_event_, NULL);

	evtimer_set(&timeout_event_, TimeoutCb, this);		// trigger the commit execute when timeout
	event_base_set(base_, &timeout_event_);
	event_add(&timeout_event_,&tv);
	event_base_dispatch(base_);
}

/*
*encode the commite message
*	total_len:msg_type:cmt_epoch, cmt_sequence
*	  U32	     U32         U64		U64
*
*/
bool 
CommitSyncer::EncodeCommitSyncMessage(char* buf, ProposalNum cmtpn, U32 &size) {
	U32 netformat32;
	U64 netformat64;
	U32 total_len;
	U32 msg_type = COMMIT_MESSAGE;
	total_len = sizeof(msg_type) + sizeof(cmtpn.epoch) + sizeof(cmtpn.sequence) ;
	size = total_len + sizeof(total_len); 
	if ( size  >  COMMIT_MESSAGE_SIZE){
		LOG(ERR,("buf is too small\n"));
		return false;
	}
	netformat32 = htonl(total_len);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);
	
	netformat32 = htonl(msg_type);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);

	netformat64 = Htonl64(cmtpn.epoch);
	memcpy(buf, &netformat64,  sizeof(netformat64));
	buf += sizeof(netformat64);

	netformat64 = Htonl64(cmtpn.sequence);
	memcpy(buf, &netformat64,  sizeof(netformat64));
	buf += sizeof(netformat64);
	return true;	
}

void CommitSyncer::UpdateLocalCmtSequence(U64 cmt_sequence){
	ProposalNum cmt_pn;
	cmt_pn.sequence = cmt_sequence;
	cmt_pn.epoch	   =  Global_P12Info.epoch();
	mlstore.StoreMetaInfo(Global_P12Info.range_num(), cmt_pn);
	
}
inline bool
CommitSyncer::_SendCmtSequence(class FollowerInfo *fi,   U32 size){
	return TcpSend(fi->new_fd_,  sbuf_, size);
}

bool 
CommitSyncer::SendCmtSequence(U64 cmt_sequence) {
	U32 size;
	bool sended = false;
	ProposalNum cmtpn;
	cmtpn.sequence = cmt_sequence;
	cmtpn.epoch	=  Global_P12Info.epoch();
	
	if (!EncodeCommitSyncMessage(sbuf_, cmtpn, size)){
		LOG(HCK,("encode cmt_sync_msg error\n"));
		return false;
	}
	//then send the msg to the followers in the followerlist
	// we shoud use the tcp write sock exclusion( otherwise the payload will be mixed) 
	flws.LockFlist();
	//here will just like SendProposals
	if(flws.FollowerLiveNum() +1 < QUORUM_NUM ){
		LOG(HCK,("the number of server can't form a quorum\n"));	
		flws.UnlockFlist();
		return false;
	}
	for( FollowerList::size_type i = 0; i != flws.FollowerNum(); i++){
		switch ( flws.TryConnectButNotNewConnection(i)){
				case	 DELETING:					//zookeeper client set it
					/*LOG(HCK,("delete follower:%s.\n",flws[i]->ip_));
					close(flws[i]->new_fd);		
					event_del(&(flws[i]->fevent));
					flws.erase(i);
					i--;		// it is important*/
					break;
				case FOLLOWER_CLOSE:
					/*LOG(HCK,("delete follower:%s.\n",flws[i]->ip_));
					close(flws[i]->new_fd);		
					event_del(&(flws[i]->fevent));
					flws.erase(i);
					i--;		// it is important*/
					break;
				case CONNECT_FAIL:
					//do nothing wait for next connect try if we send fail
					LOG(HCK,( "wait for next connect\n"));
					break;
				case CONNECT_OK:
					//send the cmt msg
					//connect and send the cmt msg
					if ( _SendCmtSequence(flws.GetFollowerInfo(i), size) ){
						sended = true;
						LOG(HCK, ("send cmt sequence ok\n"));
					}else{
						//here we not set the  FollowerInfo states , the proposor will do int the ProposalMessageHandle
						LOG(HCK, ("send cmt sequence error\n"));
					}
					break;

				case NEW_CONNECT:
					LOG(HCK,("The connection have not been connected, syn cmt the next time\n"));
					break;
				default:
					LOG(HCK,("TryConnect fault\n"));
					break;				
		}
	}
	flws.UnlockFlist();	
	return sended;
}
void CommitSyncerCb(int sfd, short event, void* args){
	class CommitSyncer  *cmter = (class CommitSyncer  *)args;
	bool sended = false;
	//U64 cmt_sequence =  cmtprosockpair.GetValU64();	 //don't make the problem more complex
	//here we can also test if actually have data in the sock
	cmtprosockpair.GetMessage();
	U64  cmt_sequence = Global_P12Info.get_commited_sequence_locked();
	U64 lsc_sequence = Global_P12Info.get_last_synced_cmt_sequence_unlocked();
	if(cmt_sequence < lsc_sequence){
		LOG(ERR,("this shouded be happend\n"));
		assert(0);
	}
	 if ( (cmt_sequence - lsc_sequence) < COMMIT_UNSYNED_NUM){
		LOG(HCK,("the uncommited num is not enough,wait util enough\n"));
		//here we shoud not update the last sync time
		cmtprosockpair.ClearCmtShouldSync();
		return;
	 }
	cmter->UpdateLocalCmtSequence(cmt_sequence);	
	sended  = cmter->SendCmtSequence(cmt_sequence);
	if (sended){
		Global_P12Info.update_last_synced_cmt_sequence(cmt_sequence);		
	}
	//send ok or not ,we will update the timeval
	cmter->SetLastSyncTime();
	cmtprosockpair.ClearCmtShouldSync();
}

void TimeoutCb(int sfd, short event, void* args){
	class CommitSyncer  *cmter = (class CommitSyncer  *)args;
	bool sended;
	U64  cmt_sequence = Global_P12Info.get_commited_sequence_locked();
	U64  lsc_sequence = Global_P12Info.get_last_synced_cmt_sequence_unlocked();
	if(cmt_sequence < lsc_sequence){
		LOG(ERR,("this shouded be happend\n"));
		assert(0);
	}
	if ( !cmter->IfTimeOuted()){
		LOG(HCK,("haven't timeout since last time\n"));
		cmter->ResetTimeOutEvent();
	}else{
		LOG(HCK,("have timeouted since last time\n"));
		if ( cmt_sequence == lsc_sequence){
			//is the cmt have been updated, not need to sync
			LOG(HCK,("cmt_sequence =  lsc_sequence do nothing\n"));	
		}else{	
			cmter->UpdateLocalCmtSequence(cmt_sequence);
			sended = cmter->SendCmtSequence(cmt_sequence);
			if (sended){
				Global_P12Info.update_last_synced_cmt_sequence(cmt_sequence);
			}
			//send ok or not ,we will update the timeval
		}
		cmter->SetLastSyncTime();
		// reset the timeoutevent
		cmter->ResetTimeOutEvent();
	}
}
