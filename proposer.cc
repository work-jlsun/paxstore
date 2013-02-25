/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: proposal thread
*/

#include "proposer.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

#include "pqueue.h"
#include "followerlist.h"
#include "cfg.h"
#include "logworker.h"
#include "libevent_tcp_server.h"

/***************************************************************/

/*
* SocketPair is used for the client recive thread to wake up 
*'message' the ProposerWorker thread
*/
class SocketPair     pwsockpair;  //the sock pair between  proposer and logwriter
class SocketPair     cmtprosockpair;  // the sock pair between proposer and CommitSyncer
class ProposerAndCatchuperMsger pcmsger;

ProposerWorker::ProposerWorker():base_(NULL){
	//do nothing trival workers
}

ProposerWorker::~ProposerWorker(){
	if(base_){
		event_base_free(base_);
	}	
}

void ProposerWorker::Start(void){
	//new the event base
	base_ = event_base_new();
	if (base_ == NULL){
		assert(0);
	}				
	//set the libevent_tcp_server message handle
	event_set(&client_msg_event_, sockpair.GetReadSock(), EV_READ|EV_PERSIST, 
				ClientMsgHandle, this);
	event_base_set(base_, &client_msg_event_);
	event_add(&client_msg_event_,NULL);	
	// set the log thread message handle
	event_set(&logwriter_msg_event_, ltpsockpair.GetReadSock(), EV_READ|EV_PERSIST, 
				LogworkerMessageHandle, this);
	event_base_set(base_, &logwriter_msg_event_);
	event_add(&logwriter_msg_event_,NULL);
	
	event_base_dispatch(base_);	
}

void ClientMsgHandle(int sfd, short event, void *arg) {
	class ClientKeyValue * cvalue;
	bool flag;
	ProposerWorker *pw = (ProposerWorker *)arg;
	// get the message from the keyvalue list, and the if we can lanch more proposal , get
	//one entry from the value list and constructor a proposal and put it in the pqueue
	sockpair.GetMessage();
	if( Global_P12Info.open_cnt() >= MAX_PARALLEL ){
			LOG(HCK,("the proposal queue is full , wait \n"));
			//sockpair.SendMessage(); // wait for next try
			sleep(2);
			return;
	}
	// then   construct a proposal and send it
	if( (flag  = GlobleValueList.PopNextValue(cvalue)) == false){
		LOG(ERR,("fuck, what is wrong with the value queue\n"));
		assert(0);
	}
	U64 sn = Global_P12Info.next_unused_sequence();
	QProposal *qp = Global_ProposalQueue.GetQproposal(sn);		// initialize the leader proposal queue
	qp->Init(cvalue, sn);
	Global_P12Info.inc_next_unused_sequence();	//increase the sequence after init it
	Global_P12Info.inc_open_cnt();
	pwsockpair.SendMessage();	  //message the log writers	
	//then send this message to the followers	, here we will use the  async tcp 	
	if ( flws.SendProposals(qp,pw) ) {
		LOG(VRB,("send proposal ok\n"));
	}
	return;
}
