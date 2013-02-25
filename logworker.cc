/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#include "logworker.h"

// the sock pair between the log worker and  proposal worker, used for the 
// logworker thread to wakeup the proposal worker
class SocketPair     ltpsockpair;    
						 
class LogManager logstore;
//class Storage logstore;

log_worker::log_worker():base(NULL){
	base = event_base_new();
	if (base == NULL){
		assert(0);
	}
	event_set(&log_msg_event, pwsockpair.GetReadSock(), EV_READ|EV_PERSIST, 
				log_msg_handle,&pwsockpair);
	event_base_set(base, &log_msg_event);
	event_add(&log_msg_event,NULL);
	//the lsn shoud be set to the last commited value or other's if i am the recover
	//fix me here
}

log_worker::~log_worker(){
	if(base){
		event_base_free(base);
	}	
}

void 
log_worker::start(void){
	event_base_dispatch(base);
}

void log_msg_handle(int sfd, short event, void* args){
	class SocketPair *sp = (class SocketPair *)args;
	assert(sfd == sp->GetReadSock());
	pwsockpair.GetMessage();
	//got the message and get out of the next sequence ,and log into db
	U64 epoch = Global_P12Info.epoch();
	U64 sequence = Global_P12Info.lsn_sequence();
	LOG(VRB,("sequence%llu\n",sequence + 1));
	//logstore.LogAppendProposal(Global_ProposalQueue[\
	//	Global_ProposalQueue.GetQproposalOffset(sequence + 1)].proposal());
	//logstore.LogAppendProposal(Global_ProposalQueue.GetQproposal(sequence + 1)->pinstance());	
	if (!logstore.AddRecord(Global_ProposalQueue.GetQproposal(sequence + 1)->pinstance())) {
		assert (0);
	}

	Global_P12Info.update_lsn_sequence(epoch, sequence +1);
	if( sequence + 1 <=  Global_P12Info.get_highest_promise_sequence() ){
		ltpsockpair.SendMessage();
	}		
}
