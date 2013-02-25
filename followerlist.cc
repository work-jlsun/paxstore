/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: manage leader state and send proposal to followers and send ack to client
*/

#include "followerlist.h"

#include <netdb.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "commitsyncworker.h"
#include "pqueue.h"
#include "log_manager.h"
#include "tools.h"
#include "cfg.h"
#include "proposer.h"
#include "logworker.h"
#include "leaderfailover.h"
#include "data_store.h"

char*  flist[2] = {"192.168.3.26","192.168.3.27"};
class FollowerList	flws;
char  proposal_send_buf[MAX_TCP_MSG_SIZE];
char  proposal_receive_buf[MAX_TCP_MSG_SIZE];

FollowerInfo::~FollowerInfo(){
	//LOG(HCK,("-->"));
	if(ip_){ 
		free(ip_);
	}
	//LOG(HCK,("<--"));
	// fixme  do i need to del event here?
}

//fixme,here we just use the globle follwer's info,later we will get the info 
//from the configure file
FollowerList::FollowerList(){			
	int i  = 0;
	class FollowerInfo *p;
	num_ = i;
	if ( pthread_mutex_init(&list_mutex_, NULL)  != 0){
		assert(0);
	}
	if ( pthread_mutex_init(&catchup_mutex_, NULL)  != 0){
		assert(0);
	}
	while (i--){
		 if ( !(p = new class FollowerInfo()) ){
			LOG(HCK,("malloc flower_info error.\n"));
			assert(0);
		 }
		 p->Init(0, flist[i]);  	//the follower id is not used here
		 fvec_.push_back(p);
	}
}

FollowerList::~FollowerList(){
	std::vector<class FollowerInfo*>::iterator iter;
	
	pthread_mutex_lock(&list_mutex_);
	int32_t i = fvec_.size();
	while (i--){
		delete fvec_[i];
	}
	
	fvec_.clear();	//delete the pointer
	pthread_mutex_unlock(&list_mutex_);
	if ( pthread_mutex_destroy(&list_mutex_)  != 0){
		assert(0);
	}
	if ( pthread_mutex_destroy(&catchup_mutex_)  != 0){
		assert(0);
	}
}

/*
*encode format: "total_len:msg_type:epoch:sequence:key_len:value_len:key:value"
*/
bool 
FollowerList::EncodeProposal(QProposal * qp, U32 & size){
	U32	 netformat32;
	U64	 netformat64;	
	char* buf;
	U32  key_len;
	U32  value_len;
	U32 msg_type =  PROPOSAL_MESSAGE;
	U32 total_len = 0;
	buf = proposal_send_buf;
	LOG( VRB,("-->\n"));
	key_len = strlen(qp ->pinstance().kv.kv.key) + 1;
	total_len += key_len;
	value_len = strlen(qp ->pinstance().kv.kv.value) + 1;
	total_len += value_len;
	total_len += 2 * sizeof(qp->pinstance().pn.sequence);
	total_len += 2 *sizeof(U32); // key_len + value_len
	total_len +=  sizeof(msg_type);
	
	if (total_len > MAX_TCP_MSG_SIZE){
		return false;
	}
	netformat32 = htonl(total_len);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);

	netformat32 = htonl(msg_type);
	memcpy(buf, &netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);
	 
	 netformat64 = Htonl64(qp ->pinstance().pn.epoch);
	 memcpy(buf, &netformat64,  sizeof(netformat64));
	 buf += sizeof(netformat64);

	 netformat64 = Htonl64(qp ->pinstance().pn.sequence);
	 memcpy(buf, &netformat64,  sizeof(netformat64));
	 buf += sizeof (netformat64);
	 
	 netformat32 = htonl(key_len);
	 memcpy(buf, &netformat32,  sizeof(netformat32));
	 buf += sizeof (netformat32);

	 netformat32 = htonl(value_len);
	 memcpy(buf, &netformat32,  sizeof(netformat32));
	 buf += sizeof (netformat32);
	 
	  memcpy(buf, qp ->pinstance().kv.kv.key, key_len);
	  buf += key_len;
	  
	  memcpy(buf, qp ->pinstance().kv.kv.value, value_len);
	  size = total_len + sizeof(total_len);	//size if the total message size;
	  LOG( VRB,("<--\n"));
	  return true;
}
/*
*encode format: "total_len:msg_type:epoch:sequence:key_len:value_len:key:value"
*/
bool 
FollowerList::EncodeProposal(FailOverItem *failover_item, U32 & size){
	U32	 netformat32;
	U64	 netformat64;	
	char* buf;
	U32  key_len;
	U32  value_len;
	U32 msg_type =  PROPOSAL_MESSAGE;
	U32 total_len = 0;
	buf = proposal_send_buf;
	LOG( VRB,("-->\n"));
	key_len = failover_item->key_len();
	total_len += key_len;
	value_len = failover_item->value_len();
	total_len += value_len;
	total_len += 2 * sizeof(U64);   //epoch +sequence
	total_len += 2 *sizeof(U32); // key_len + value_len
	total_len +=  sizeof(msg_type);
	
	if (total_len > MAX_TCP_MSG_SIZE){
		return false;
	}
	netformat32 = htonl(total_len);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);

	netformat32 = htonl(msg_type);
	memcpy(buf, &netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);
	 
	 netformat64 = Htonl64(failover_item->pn().epoch);
	 memcpy(buf, &netformat64,  sizeof(netformat64));
	 buf += sizeof(netformat64);

	 netformat64 = Htonl64(failover_item->pn().sequence);
	 memcpy(buf, &netformat64,  sizeof(netformat64));
	 buf += sizeof (netformat64);
	 
	 netformat32 = htonl(key_len);
	 memcpy(buf, &netformat32,  sizeof(netformat32));
	 buf += sizeof (netformat32);

	 netformat32 = htonl(value_len);
	 memcpy(buf, &netformat32,  sizeof(netformat32));
	 buf += sizeof (netformat32);
	 
	  memcpy(buf, failover_item->kv(), key_len);
	  buf += key_len;
	  
	  memcpy(buf, failover_item->kv() + key_len, value_len);
	  size = total_len + sizeof(total_len);	//size if the total message size;
	  LOG( VRB,("<--\n"));
	  return true;
}

/*
*decode proposal response:
*
*proposal response format:"total_len + msg_type+ follwerid + epoch + sequence"
*/
bool 
DecodeProposalResponse(char* buf, ProposalResponseInfo & pri){
	U32 msg_type;
	memcpy((void*)&msg_type, buf, sizeof(msg_type));
	msg_type = ntohl(msg_type);
	buf += sizeof(msg_type);
	if (msg_type != PROPOSAL_MESSAGE_ACK){
		LOG(ERR,("the msg is not PROPOSAL_MESSAGE_ACK\n"));
		return false;
	}
	memcpy((void *)&(pri.followerid_),  buf, sizeof(pri.followerid_));
	pri.followerid_ = ntohl(pri.followerid_);
	buf += sizeof(pri.followerid_);
	memcpy((void *)&(pri.epoch_), buf, sizeof(pri.epoch_));
	pri.epoch_  = Ntohl64(pri.epoch_);
	buf += sizeof(pri.epoch_);
	LOG(VRB, (" epoch %llu\n", pri.epoch_));
	memcpy((void *)&(pri.sequence_), buf, sizeof(pri.sequence_));
	pri.sequence_= Ntohl64(pri.sequence_);
	LOG(VRB, (" sequece %llu\n", pri.sequence_));
	return true;
}


bool 
FollowerList::SendProposal(class FollowerInfo * fi , U32 size){
	return TcpSend(fi->new_fd_,  proposal_send_buf, size);
}

bool 
FollowerList::SendProposals(class QProposal * qp, class ProposerWorker *pw ){   
		U32  msize;
		U32 sendoknum = 0;
		//here is the the message from the catch up thread, let the send task to stop for
		//a while to wait the follower to catchup
		while ( pcmsger.TestStopFlag()){
			sleep(PROPOSER_SLEEP_TIME);
		}
		//encode the proposal to message buffer
		LOG( VRB,("-->SendProposals\n"));
		if ( !EncodeProposal(qp,msize) ){
			LOG(HCK,("message is too bigger.\n"));
			assert(0);	//the big message have been filted, so we just assert here
		}
		//we must send success number must >= the QUORUM_NUM
		while (  (sendoknum + 1) <  QUORUM_NUM ){
			LockCatchupMutex();
			LockFlist();	
			//sendoknum = 0;
			if( FollowerLiveNum() +1 < QUORUM_NUM ){
				LOG(HCK,("the number of server can't form a quorum.\n"));
			}else{
				for(size_type  i = 0; i != FollowerNum(); i++){
					switch ( TryConnect(i)){
						case	 DELETING:					//zookeeper client set it
								/*LOG(HCK,("delete follower:%s.\n",fvec_[i]->ip_));
								close(fvec_[i]->new_fd_);
								// fix me ,if this fevent didn't add to base_,we can't delete it
								event_del(&(fvec_[i]->fevent_));
								delete fvec_[i];
								fvec_.erase(fvec_.begin()+i);
								i--;		// it is important*/
								break;
						case FOLLOWER_CLOSE:
								/*LOG(HCK,("delete follower:%s.\n",fvec_[i]->ip_));
								close(fvec_[i]->new_fd_);		
								event_del(&(fvec_[i]->fevent_));
								delete fvec_[i];								
								fvec_.erase(fvec_.begin()+i);
								i--;*/
								break;
						case CONNECT_FAIL:
								//do nothing wait for next connect try if we send fail
								LOG(HCK,("wait for next connect.\n"));
								break;
						case CONNECT_OK:
								if( SendProposal(fvec_[i], msize)){ //send  the proposal
									sendoknum++;
								}
								break;
						case NEW_CONNECT:  //set the read event  send the proposal
								event_set(&fvec_[i]->fevent_, fvec_[i]->new_fd_  , EV_READ | EV_PERSIST,
										ProposalMessageHandle, fvec_[i] );	
								event_base_set(pw->base_, &fvec_[i]->fevent_);
								event_add(&fvec_[i]->fevent_, NULL);	//if receive a request before event register,
								if (SendProposal(fvec_[i],msize)){ 		
									sendoknum++;
								}
								break;
						default:
								LOG(HCK,("TryConnect fault.\n"));
								break;
					}
				}
			}
			UnlockFlist();
			UnlockCatchupMutex();
			// give the chance for the follower catchup and then add to  the followerlist 
			if ( (sendoknum + 1) <  QUORUM_NUM ){ 
				sendoknum = 0; 
				sleep(2); 
			} 
		}
		//UnlockCatchupMutex();
		LOG( VRB,("<--SendProposals\n"));	
		return true;
}

void 
ProposalMessageHandle(int sfd, short event, void *arg){
	FollowerInfo * fi = (FollowerInfo *)arg;
	char* buf = proposal_receive_buf;
	int errcode = 0;
	ProposalResponseInfo pinfo;
	assert(fi->new_fd_ == sfd);
	ssize_t nsize = ReadvRec(sfd, buf, MAX_TCP_MSG_SIZE, errcode);
	if ( errcode == EMSGSIZE ){
		LOG(HCK,("message bigger than buffer.\n"));
		return;
	}
	if (nsize == 0){	// the follower close elegantly
		LOG(HCK,("the follower disconnected elegantly.\n"));
		close(fi->new_fd_);		
		event_del(&(fi->fevent_));
		fi->state_ = CLOSED;
		ifsendfollowerlist = true;
		return;		
	}else if (nsize  < 0){	// the follower close force?
		LOG(HCK,("the follower disconnected forcely.\n"));
		close(fi->new_fd_);		
		event_del(&(fi->fevent_));
		fi->state_ = CLOSED;
		ifsendfollowerlist = true;
		return;
	}else {
		//decode the proposal result and  do something
		if (! DecodeProposalResponse(buf, pinfo)){
			return;
		}
		//decode ok
		if(pinfo.epoch_  >  Global_P12Info.epoch()){
			assert(0);
		}
		if(pinfo.sequence_  <= Global_P12Info.get_commited_sequence_unlocked()){
			LOG(VRB, ("pinfo.sequence:%llu,commited.sequence:%llu\n",\
				pinfo.sequence_, Global_P12Info.get_commited_sequence_unlocked()));
			LOG(VRB,("the proposal have commited!do nothing.\n"));
		}else if (pinfo.sequence_  == Global_P12Info.get_commited_sequence_unlocked() + 1){		
			QProposal *qp = Global_ProposalQueue.GetQproposal(pinfo.sequence_);
			qp->SetPromise(pinfo.followerid_);
			CmtRange cr;
			Global_P12Info.update_highest_promise_sequence(pinfo.sequence_);
			cr = Global_P12Info.update_commited_sequence(pinfo.epoch_, pinfo.sequence_);
			
			/*here we will deal with the proposals that have just been  set committed*/
			// test here;
			proposal * pr;
			std::string key;
			std::string value;
			U64 seq;

			assert(cr.from <= cr.to);
			for (U64 i = cr.from + 1; i != cr.to +1; i++){
				qp = Global_ProposalQueue.GetQproposal(i);				
				pr = qp->ppinstance();

				key = pr->kv.kv.key;
				value = pr->kv.kv.value;
				seq = pr->pn.sequence;
				datastore.WriteData(key, value, seq);		// insert record into leveldb
				
				//if(pr->pn.sequence%9999 ==0)
					pr->notifyclient();
				qp->SetState(commmited);
				Global_P12Info.dec_open_cnt();
			}
			//send commit thread a message ,ask it to send cmt message to follower
			if (cr.from != cr.to ){
				if (  cmtprosockpair.IfCmtShouldSync() ) {
					U64 cmt_sequence = Global_P12Info.get_commited_sequence_unlocked();
					U64 lsc_sequence = Global_P12Info.get_last_synced_cmt_sequence_locked();
					if (cmt_sequence < lsc_sequence){
						assert(0);
					}
					if( (cmt_sequence - lsc_sequence) >= COMMIT_UNSYNED_NUM ){
						cmtprosockpair.SetCmtShouldSync();
						//cmtprosockpair.SendValU64(cmt_sequence);  //don't make the problem more complex
						cmtprosockpair.SendMessage();
					}
				}else{
					//last cmt syn worker is in progress,do nothing
				}
			}
				
			//qp->clear();	  //fix me if the log writer is slow than the propoal_worker
			//Global_P12Info.dec_open_cnt();	
		}
		else {
			QProposal *qp = Global_ProposalQueue.GetQproposal(pinfo.sequence_);
			qp->SetPromise(pinfo.followerid_);
			Global_P12Info.update_highest_promise_sequence(pinfo.sequence_);  // important
			LOG(HCK,("happend in multi clients\n"));   //exactly in multi connection
		}
	}
}


void LogworkerMessageHandle(int sfd, short event, void *arg){
	CmtRange cr;
	QProposal *qp;
	proposal * pr;
	std::string key;
	std::string value;
	U64 seq;

	LOG(VRB, ("-->"));
	ltpsockpair.GetMessage();
	cr = Global_P12Info.update_localdelay_commited_sequence();
	assert(cr.from <= cr.to);
	for (U64 i = cr.from + 1; i != cr.to + 1; i++){
		qp = Global_ProposalQueue.GetQproposal(i);
		pr = qp->ppinstance();

		key = pr->kv.kv.key;
		value = pr->kv.kv.value;
		seq = pr->pn.sequence;
		datastore.WriteData(key, value, seq);		// insert record into leveldb
		
		//if(pr->pn.sequence%9999 ==0)
			pr->notifyclient();
		qp->SetState(commmited);
		Global_P12Info.dec_open_cnt();
	}
	LOG(VRB, ("<--"));
	return;
}

short 
FollowerList::TryConnect(U32 index){
	int sock;
	int flags; 
	struct sockaddr_in follower_addr;  
	LOG( VRB,("-->\n"));
	if  ( fvec_[index]->state_ == NEWITEM ){
		if (( sock  = socket(AF_INET,SOCK_STREAM , 0)) < 0){
			return CONNECT_FAIL;
		}
		//set   tcp   async;
		if ( ( flags = fcntl(sock, F_GETFL, 0)) < 0 ||
			fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0 ){
			LOG(HCK,("Error:: server listensock set O_NONBLOCK error\n"));
			close(sock);
			return CONNECT_FAIL;
		}
		 int send_buf_size = MAX_PARALLEL* MAX_TCP_MSG_SIZE/10; 
		if( (flags = setsockopt(sock, SOL_SOCKET, SO_SNDBUFFORCE, (void *)&send_buf_size, 
						sizeof(send_buf_size) ) ) < 0 ){
			LOG(HCK,("setsockopt send buf size error\n"));
			close(sock);
			return CONNECT_FAIL;
		}				

		int rcvbuf_len;
		socklen_t len = sizeof(rcvbuf_len);
		if( getsockopt( sock, SOL_SOCKET, SO_SNDBUF, (void *)&rcvbuf_len, &len ) < 0 ){
			  perror("getsockopt: ");
			  return -1;
		}
		follower_addr.sin_family = AF_INET;
		follower_addr.sin_port = htons(FOLLOWERPORT);
		follower_addr.sin_addr.s_addr = inet_addr(fvec_[index]->ip_);
		LOG( VRB,("before ConnectRetry\n"));
		if( ConnectRetry(sock, (struct sockaddr*)&follower_addr, sizeof(follower_addr)) == -1){
			close(sock);
			++fvec_[index]->tryconnecttime;
			LOG( VRB,("CONNECT_FAIL\n"));
			LOG( VRB,("<--\n"));
			if(fvec_[index]->tryconnecttime <MAXTRYCONNECTTIME)
				return CONNECT_FAIL;
			else
				return DELETING;
		}
		fvec_[index]->tryconnecttime =0;
		// update some info
		fvec_[index]->new_fd_ = sock;
		fvec_[index]->new_follower_addr_ = follower_addr;
		fvec_[index]->state_ = CONNECTED;
		LOG( VRB,("NEW_CONNECT\n"));
		LOG( VRB,("<--\n"));
		return NEW_CONNECT;
	}else if (fvec_[index]->state_ == CLOSED){
		// return info for proposal to delete event
		LOG( VRB,("FOLLOWER_CLOSE\n"));
		LOG( VRB,("<--\n"));
		return FOLLOWER_CLOSE;	
	}else if (fvec_[index]->state_ == CONNECTED){
		LOG( VRB,("CONNECT_OK\n"));
		LOG( VRB,("<--\n"));
		return CONNECT_OK; 
	}else if (fvec_[index]->state_ == ZOOKEEPER_DELETED){
		//the zookeeper deconnect with the follower,so set the deleted flag
		LOG( VRB,("DELETING\n"));
		LOG( VRB,("<--\n"));
		return DELETING;
	}else{
		LOG( VRB,("follower state unkown\n"));
		LOG( VRB,("<--\n"));
		assert(0);
		return  DELETING;  //just let out of warning 
	}
}

short 
FollowerList::TryConnectButNotNewConnection(U32 index){
	int sock;
	int flags; 
	struct sockaddr_in follower_addr;  
	LOG( VRB,("-->\n"));
	if  ( fvec_[index]->state_ == NEWITEM ){
		return NEW_CONNECT;
	}else if (fvec_[index]->state_ == CLOSED){
		// return info for proposal to delete event
		LOG( VRB,("FOLLOWER_CLOSE\n"));
		LOG( VRB,("<--\n"));
		return FOLLOWER_CLOSE;	
	}else if (fvec_[index]->state_ == CONNECTED){
		LOG( VRB,("CONNECT_OK\n"));
		LOG( VRB,("<--\n"));
		return CONNECT_OK; 
	}else if (fvec_[index]->state_ == ZOOKEEPER_DELETED){
		//the zookeeper deconnect with the follower,so set the deleted flag
		LOG( VRB,("DELETING\n"));
		LOG( VRB,("<--\n"));
		return DELETING;
	}else{
		LOG( VRB,("follower state unkown\n"));
		LOG( VRB,("<--\n"));
		assert(0);
		return  DELETING;  //just let out of warning 
	}
}
