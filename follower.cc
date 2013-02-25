/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: follower receive proposal messages from leader and send back ack
  *            : receive cmt message and write data into storage engine
  *		   : receive read request and read data from storage and send it to client
*/

#include "follower.h"

#include <stdlib.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <string.h>

#include "commitsyncworker.h"
#include "pqueue.h"
#include "proposer.h"
#include "metalogstore.h"
#include "frecover.h"
#include "zkcontrol.h"
#include "data_store.h"
#include "serverread.h"

class FState flstates;
class FProposalQueue GlobalFproposal;

char FollowerReceiveBuffer[MAX_TCP_MSG_SIZE];
char FollowerPresponseBuffer[MAX_PRESPONSE_MSG_SIZE];
int do_follower_recovery = 1;

FProposalQueue::FProposalQueue(){
	if(!pthread_rwlock_init(&plock_, NULL)){
		//noop

	}else{
		assert(0);
	}
}

FProposalQueue::~FProposalQueue(){
	if(!pthread_rwlock_destroy(&plock_)){
		//noop
	}else{
			assert(0);
	}
}

bool Follower::WriteFpr(U64 begin_seq,U64 end_seq){
	std::string key,value;
	U64 seq;
	fproposal * fpr;
	LOG(HCK,("begin_seq=%llu\t\tend_seq=%llu.\n",begin_seq,end_seq));
	for(U64 i=begin_seq;i<=end_seq;i++){
		fpr = GlobalFproposal.GetFproposal(i);
		key = fpr->kv.key;
		value = fpr->kv.value;
		seq = fpr->pn.sequence;
		//LOG(HCK,("i=%llu\t fpr->pn.sequence=%llu\n",i,fpr->pn.sequence));
		if(datastore.WriteData(key, value, seq)==false){
			LOG(HCK,("insert data into leveldb error\n"));
			assert(0);
		}
	}
	LOG(HCK,("follower insert data into leveldb ok\n"));
	return true;
}

Follower::Follower(U32 range_num):range_num_(range_num),base_(NULL),
								port_(FOLLOWERPORT),logical_table(NULL){
	base_ = event_base_new();
	if (!base_){
		LOG(HCK,("event_base_new error\n"));
		exit(0);
	}	
}

Follower::Follower():base_(NULL),port_(FOLLOWERPORT),logical_table(NULL){
	base_ = event_base_new();
	if (!base_){
		LOG(HCK,("event_base_new error\n"));
		exit(0);
	}	
}

Follower::~Follower(){
	if (logical_table != NULL){
		delete logical_table;
	}
	if(base_){
		event_base_free(base_);
	}	
}

bool Follower::FListenInit(){
	if (base_){
		listenfd_ = socket(AF_INET, SOCK_STREAM, 0);
		if ( listenfd_ < 0 ){
			return false;
		}
		int flags; 
		if ( ( flags = fcntl(listenfd_, F_GETFL, 0)) < 0 ||
			fcntl(listenfd_, F_SETFL, flags | O_NONBLOCK) < 0 ){
			LOG(HCK,( "Error:: server listensock set O_NONBLOCK error\n"));
			return false;
		}
		struct sockaddr_in listen_addr;
		//memset(&listen_addr, 0, sizeof(listen_addr));
		bzero((void *)&listen_addr, sizeof(listen_addr));
		listen_addr.sin_family = AF_INET;
		listen_addr.sin_addr.s_addr = INADDR_ANY;
		listen_addr.sin_port = htons(port_);

		// Set to reuse address   
    		int activate = 1;
    		if (setsockopt(listenfd_, SOL_SOCKET, SO_REUSEADDR, &activate, sizeof(int)) != 0) {
			LOG(HCK,("setsockopt, setting SO_REUSEADDR error\n"));
			return false;
    		}
		int bindret;
		if (bindret = (bind(listenfd_, (struct sockaddr*)&listen_addr, sizeof(listen_addr))) < 0){
			LOG(HCK,("Error: server bind error. errorcode:%d\n",bindret));
			return false;
		}

		if ( listen(listenfd_, BACKLOG) < 0){
			LOG(HCK,("Error: server listen error\n"));
			return false;
		}
		return true;
	}else{
		return false;
	}
}


bool Follower::Start(){
	event_set(&ev_accept_, listenfd_, EV_READ | EV_PERSIST, FollowerOnAccept, this);
	event_base_set(base_,&ev_accept_);
	event_add(&ev_accept_, NULL);
	event_base_dispatch(base_);	
	return true;
}

bool Follower::Init(void){
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
	if( !FListenInit()) {
		 LOG(HCK,("Error: follower  start  listen error\n"));
		return false;
	}
	return true;
}

U32 GetMsgType(char * buf){
	U32 msg_type;
	memcpy((void*)&msg_type, (void*)buf, sizeof(msg_type));
	msg_type = ntohl(msg_type);
	return msg_type;
}

/*
*decode format: "total_len:msg_type:epoch:sequence:key_len:value_len:key:value"
*			attention: the total_len have been readed,
					here we just decode the others
*/
bool  DecodeProposal(char* buf ,fproposal & pr){
	//proposal  pr;
	U32 len = 0;
	U32 key_len;
	U32 value_len;
	U32 msg_type;
	len = sizeof(msg_type);
	memcpy((void*)&msg_type, (void*)buf, len);
	msg_type = ntohl(msg_type);
	if ( msg_type  != PROPOSAL_MESSAGE){
		LOG(ERR,("is not PROPOSAL_MESSAGE\n"));
		return false;
	}
	buf += len;
	
	len = sizeof(pr.pn.epoch);
	memcpy((void *)&(pr.pn.epoch), (void *)buf, len);
	pr.pn.epoch =Ntohl64(pr.pn.epoch);
	buf  += len;

	len = sizeof(pr.pn.sequence);
	memcpy((void *)&(pr.pn.sequence), (void *)buf, len);
	pr.pn.sequence = Ntohl64(pr.pn.sequence);
	buf += len;
	
	len = sizeof(key_len);
	memcpy((void *)&(key_len), (void *)buf, len);
	key_len = ntohl(key_len);
	buf += len;

	len = sizeof(value_len);
	memcpy((void *)&(value_len), (void *)buf, len);
	value_len = ntohl(value_len);
	buf += len;

	//not copy the data, but len the pointer point to  global data buffer
	pr.kv.key = buf;
	pr.kv.value = buf + key_len;
	return true;
}

/*
*	total_len:msg_type:cmt_epoch:cmt_sequence
*	  U32	     U32         U64		
*
*/
void DecodeCmtMsg(char * buf, ProposalNum& pn){
	U32 msg_type;
	buf +=  sizeof(msg_type);
	memcpy((void *)&pn.epoch, (void *)buf, sizeof(pn.epoch));
	pn.epoch = Htonl64(pn.epoch);
	buf += sizeof(pn.epoch);
	memcpy((void *)&pn.sequence, (void *)buf, sizeof(pn.sequence));
	pn.sequence = Htonl64(pn.sequence);
	buf += sizeof(pn.sequence);
}

/*
*	total_len:msg_type:cmt_epoch:cmt_sequence
*	  U32	     U32         U64		
*
*/
void DecodeLeaderFailOverSuccessMsg(char * buf, ProposalNum& pn){
	U32 msg_type;
	buf +=  sizeof(msg_type);
	memcpy((void *)&pn.epoch, (void *)buf, sizeof(pn.epoch));
	pn.epoch = Htonl64(pn.epoch);
	buf += sizeof(pn.epoch);
	memcpy((void *)&pn.sequence, (void *)buf, sizeof(pn.sequence));
	pn.sequence = Htonl64(pn.sequence);
	buf += sizeof(pn.sequence);
}

/*
*encode proposal response format :  "total_len:msg_type:followerid:epoch:sequence"
*
*/
bool EncodeProposalResponse(ProposalResponseInfo &pr, U32 & size ){
	U32 total_len;
	U32 netformat32;
	U64 netformat64;
	U32 msg_type  = PROPOSAL_MESSAGE_ACK;
	char *buf  =  FollowerPresponseBuffer;
	
	//total_len = sizeof(pr);
	total_len  = sizeof(msg_type) + sizeof(pr.followerid_) 
			  + sizeof(pr.epoch_) + sizeof(pr.sequence_);
	size = total_len  + sizeof(total_len);	
	if ( size > MAX_PRESPONSE_MSG_SIZE ){
		LOG(HCK,("presponse too bigger\n"));
		return false;
	}
	netformat32 = htonl(total_len);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);

	netformat32 = htonl(msg_type);
	memcpy(buf, &netformat32, sizeof(netformat32));
	buf += sizeof(netformat32);

	
	netformat32 = htonl(pr.followerid_);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);
	
	netformat64 = Htonl64(pr.epoch_);
	memcpy(buf, &netformat64,  sizeof(netformat64));
	buf += sizeof(netformat64);	

	netformat64 = Htonl64(pr.sequence_);
	memcpy(buf, &netformat64,  sizeof(netformat64));
	buf += sizeof(netformat64);	
	return true;
	
}


void FollowerOnAccept(int fd, short ev, void* arg){
	Follower *fl = (Follower *)arg;
	int proposal_fd;
	int flags;
	struct sockaddr_in proposal_addr;
	socklen_t addr_len = sizeof(proposal_addr);
	assert(fd == fl->listenfd_);
	proposal_fd = accept(fd, (struct sockaddr*)&proposal_addr, &addr_len);
	/*
	if ( errno == EINTR ){
		return;  //it is just happed in multi (½ø³Ì)£¬ here we  can  do not handle it
	}
	*/
	if (proposal_fd == -1){
		LOG(HCK, ("Error:accept proposer failed"));
		return;
	}
	if ( (flags = fcntl(proposal_fd, F_GETFL, 0) < 0) ||
		fcntl(proposal_fd, F_SETFL, flags | O_NONBLOCK) < 0 ){
		close(proposal_fd);
		LOG(HCK,("Error:: set proposer socket  O_NONBLOCK error\n"));
	}
	//set the follower receive buffer, it is important to bigger than the MAX_PARALLEL 
	 int recv_buf_size = MAX_PARALLEL* MAX_TCP_MSG_SIZE/10; 
	if( (flags = setsockopt(proposal_fd, SOL_SOCKET, SO_RCVBUFFORCE, (void *)&recv_buf_size, 
					sizeof(recv_buf_size) ) ) < 0 ) {
		LOG(HCK,("setsockopt recv buf size error\n"));
		close(proposal_fd);
		return;
	}
	ProposalEvent* pe = new ProposalEvent(proposal_fd, &proposal_addr);  //c++ fromat
	PassArg * pa = new PassArg((void*)pe,(void*)fl);
	event_set(&pe->ev_read_, proposal_fd, EV_READ | EV_PERSIST, FollowerOnRead, pa);
	event_base_set(fl->base_, &pe->ev_read_);
	event_add(&pe->ev_read_, NULL);	//if receive a request before event register,
								// will it be triggered?
}

void FproposalMsgHandle(int sockfd, Follower *fl){
	fproposal  fpr;
	//decode the data and form a log format key:value
	bool flag ;
	if ( !(flag = DecodeProposal(FollowerReceiveBuffer,  fpr)) ){
		LOG(HCK,("DecodeProposal  error\n"));
		return;
	}		
	if( fpr.pn.epoch > flstates.epoch() ){
			if (fpr.pn.sequence <= flstates.lsn_sequence()) {
				LOG(HCK,("have received this  proposal before\n"));
				SendPreposalAck(fpr, sockfd);
				LOG(HCK,("have reciver this proposal ,but flstate epoch have not update, error\n"));
				assert(0);
				return;
			}
			else if(fpr.pn.sequence == (flstates.lsn_sequence() + 1 )) {
				//update former cmt
				ProposalNum pn_;
				pn_.epoch = flstates.epoch();
				pn_.sequence = flstates.lsn_sequence();				
				mlstore.StoreMetaInfo(flstates.rangenum(),pn_);	
				
				//update the logical table
				for(int i = flstates.epoch(); i !=  fpr.pn.epoch; i++) {
					if ( !fl->UpdateLogicalTable( i, flstates.lsn_sequence())){
						LOG(HCK,("follower UpdateLogicalTable  error\n"));		
						assert(0);
					}else{
						LOG(HCK,("follower UpdateLogicalTable  ok\n"));									
					}					
				}
				
				//log the entry
				if(!logstore.AddRecord(fpr)) {
					assert(0);
				}
				GlobalFproposal.SetFproposal(fpr.pn.sequence, fpr);
				flstates.set_epoch(fpr.pn.epoch);
				flstates.inc_lsn_sequence();
				SendPreposalAck(fpr, sockfd);
				return;
			}else{								//this is also just happend in downs  because we use tcp
				LOG(HCK,( "the proposal sequence number is too bigger, need catch up the delayed items\n"));
				assert(0);  //will not happend 
			}			
			LOG(HCK,( "it is happend in the  leader down circumstance\n"));				
	}
	else if ( fpr.pn.epoch ==  flstates.epoch() ){
		if (fpr.pn.sequence <= flstates.lsn_sequence()){
			LOG(HCK,("have received this  proposal before\n"));
			SendPreposalAck(fpr, sockfd);
			return;
		}
		else if(fpr.pn.sequence == (flstates.lsn_sequence() + 1 )){
			if (!logstore.AddRecord(fpr)) {
				assert (0);
			}
			GlobalFproposal.SetFproposal(fpr.pn.sequence, fpr);    // put the fproposal records into  commit queue
			flstates.inc_lsn_sequence();
			SendPreposalAck(fpr, sockfd);
			return;
		}else{								//this is also just happend in downs  because we use tcp
			LOG(HCK,( "the proposal sequence number is too bigger, need catch up the delayed items\n"));
			assert(0);  //will not happend 
		}	
	}else if (fpr.pn.epoch < flstates.epoch() ) {
		LOG(HCK,("the proposal's epoch is small , proposer are you ok\n"));
		assert(0);
	}	
}

void FcmtMsgHandle(Follower * fl){
	ProposalNum pn;
	DecodeCmtMsg(FollowerReceiveBuffer, pn);
	if (pn.epoch< flstates.epoch()){
		LOG(ERR,("epoch too small proposer are you ok?\n"));
		assert(0);	
	}else if (pn.epoch> ( flstates.epoch() +1)){
		LOG(ERR,("epoch too big proposer are you ok?\n"));
		assert(0);	
	}
	else if(pn.epoch== ( flstates.epoch() +1 ) ){
		//fix me update the logical turnacted file
		LOG(HCK,("here happed in leader failover condition\n"));
		LOG(HCK,("And the follower threads will restart, follower will redo freover\n"));
		LOG(HCK,("so this can not be happed here,assert\n"));
		assert(0);
	}
	// ok the "pn.eoch ==flstates.epoch() "
	if (pn.sequence>= flstates.cmt_sequece()){
	// follower write data into leveldb only when  follower
	//	LOG(HCK,("begin_seq=%llu\t\tend_seq=%llu\n",flstates.cmt_sequece()+1,pn.sequence));
		fl->WriteFpr(flstates.cmt_sequece()+1,pn.sequence);
		mlstore.StoreMetaInfo(flstates.rangenum(), pn);
		flstates.update_cmt_sequence(pn.sequence);		
	}else{
		
		LOG(HCK,("received cmt sequence  %llu is small than my %llu , there is an leader down before\n", pn.sequence, flstates.cmt_sequece()));
		// fixme , i shoud thing more here
	}
}

// this function will never be called ,so we don't write data into leveldb
void FLeaderFailoverSuccessMsgHandle(void){
	ProposalNum pn;
	DecodeLeaderFailOverSuccessMsg(FollowerReceiveBuffer, pn);	
	if (pn.epoch< flstates.epoch()){
		LOG(ERR,("epoch too small proposer are you ok?\n"));
		assert(0);	
	}else if (pn.epoch>   flstates.epoch()  ){
		LOG(ERR,("epoch too big proposer are you ok?\n"));
		assert(0);	
	}else { //pn.epoch == flstates.epoch()
		// ok the "pn.eoch ==flstates.epoch() " or "pn.eoch ==flstates.epoch() +1 " ,do it
		if (pn.sequence > flstates.cmt_sequece()){
			flstates.update_cmt_sequence(pn.sequence);
			mlstore.StoreMetaInfo(flstates.rangenum(), pn);
		}else{
			LOG(HCK,("received FailoverSuccess cmt sequence is small than my , it is not ture\n"));
			assert(0);
		}
	}
}


void FollowerOnRead(int fd, short ev, void* arg){
	PassArg * pa= (PassArg *)arg;	
	ProposalEvent * pe = (ProposalEvent *)pa->first();
	Follower *fl = (Follower *)pa->second();
	int errcode = 0;
	char *buf = FollowerReceiveBuffer;
	fproposal  fpr;
	if(rfmsger.IfTriggered() ==  false){			//fixme, attention with that here shoud no two or more  leader clients.
		if(rfmsger.GetMessageOk()){	//wait for get msg
			rfmsger.SetTriggered();
			LOG(HCK,("follower remote catcheup ok\n"));
		}else{
			LOG(HCK,("follower remote catcheup error\n"));
			//fixeme here the follower shoud  exit;
			event_base_loopbreak(fl->base_);
		}
	}
	ssize_t nsize = ReadvRec(fd, buf, MAX_TCP_MSG_SIZE,  errcode);
	if (errcode == EMSGSIZE){
		LOG(HCK,("message bigger than buffer\n"));
		return;
	}
	if(nsize == 0){
		LOG(HCK,("proposal disconnected\n"));
		close(fd);
		event_del(&pe->ev_read_);
		pe->active_  = false;	//
		//delete pe;	//ok, if we shoud delete it;  fixme		
		return;
	}else if(nsize < 0){
		LOG(HCK,("proposal failed disconnected\n"));
		close(fd);
		event_del(&pe->ev_read_);
		pe->active_ = false;	//it is closed; //the ce struct will be closed by other	//fix me ,it need atomic set
		// delete ce;	//ok, if we shoud delete it;  fixme	
		return;
	}else{		
		switch(GetMsgType(FollowerReceiveBuffer)){
			case COMMIT_MESSAGE:
				FcmtMsgHandle(fl);
				break;
			case PROPOSAL_MESSAGE:
				FproposalMsgHandle(fd, fl);
				break;
			case LEADER_FAILOVER_SUCCESS_MSG:
				FLeaderFailoverSuccessMsgHandle();
			default:
				LOG(ERR,("unkone msg type\n"));
				break;
		}
	}
}

bool SendPreposalAck(struct fproposal & fpr, int fd){
	//bool flag;
	U32 size;
	ProposalResponseInfo pr;
			
	pr.Init(0, fpr.pn.epoch, fpr.pn.sequence);
	if ( !EncodeProposalResponse(pr, size) ){
		LOG(HCK,( "proposal response buffer is small\n"));
		return false;	
	}
	 if( TcpSend(fd, FollowerPresponseBuffer, size) ){
		LOG(VRB,("send back proposal ok \n"));
		return true;
	 }else {
	 	 LOG(HCK,("send back proposal error \n"));
		return false;
	}				 
}
/*
*the cleanup function will call by the follower 
*when the follower can pthread_exit or 
*ended by other threads throught pthread_cancel()
*/
void follower_cleanup_handler(void *p){
	int fd = *(int *)p;
	close(fd);
	LOG(HCK,("cleanup\n"));
}

void* follower_thread(void *arg){
	//U32 range_num = *(U32 *)arg;
	Follower myfollower;
	DisableSigPipe();
	if (!myfollower.Init()){
		LOG(HCK,("myfollower init error\n"));
		assert(0);
	}
	int listen_fd = myfollower.listenfd();
	pthread_cleanup_push(follower_cleanup_handler, (void *)&listen_fd);
	LOG(HCK,("start follower thread\n"));
	myfollower.Start();
	LOG(HCK,("exit follower thread\n"));
	pthread_cleanup_pop(0);  
	return NULL;
}

void* recovery_thread(void *arg){
	class PassArgsToFollower * passargs = (class PassArgsToFollower *)arg;
	class FRecovery myfrecover;
	myfrecover.InitArgsFromZooKeeper(passargs);
	myfrecover.Start();
	return NULL;
}

void follower_read_cleanup_handler(void *p){
	int fd = *(int *)p;
	close(fd);
	LOG(HCK,("cleanup\n"));
}

void* follower_read_thread(void *arg) {
	 ServerRead lr;
	 DisableSigPipe();
	 if (!lr.Init(false)){
		LOG(HCK,("ServerRead init error\n"));
		assert(0);
	}
	 int listen_fd = lr.listenfd();
	 pthread_cleanup_push(follower_read_cleanup_handler, (void *)&listen_fd);
	 LOG(HCK,("start read_thread  thread\n"));	 
	 lr.Start();
	 LOG(HCK,( "read_thread  event loop exit\n"));	 
 	 pthread_cleanup_pop(0);  
	 return NULL; 
}

void cleanup_handler(void *p){
	int fd = *(int *)p;
	close(fd);
	LOG(HCK,("cleanup\n"));
}

void * test_reuseaddr1(void *arg){
	Follower myfollower;
	DisableSigPipe();
	if ( !myfollower.FListenInit()){
		LOG(HCK,("FListenInit error\n"));
		return NULL;
	}else{
		LOG(HCK,("FListenInit ok\n"));
	}
	int fd = myfollower.listenfd();
	pthread_cleanup_push(cleanup_handler, (void *)&fd);
	int i ;
	scanf("%d",&i);
	pthread_cleanup_pop(0);  	
}

void * test_reuseaddr2(void *arg){
	Follower myfollower;
	DisableSigPipe();
	if ( !myfollower.FListenInit()){
		LOG(HCK,("FListenInit error\n"));
	}else{
		LOG(HCK,("FListenInit ok\n"));
	}

}
