/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: to recovery data when follower start
*/

#include "frecover.h"

#include <errno.h>

#include "tools.h"
#include "follower.h"
#include "metalogstore.h"
#include "leadercatchup.h"
#include "data_store.h"

class SocketPair rfmsger; //the messange between the frecover and the follower
char leaderip[16] = "192.168.3.25";

bool FRecovery::FRecoveryInsertData(U64 begin_seq,U64 end_seq){
	logical_table_->LogicalTableWarmUp();
	std::string key;	
	std::string value;
	U64 epoch;
	CatchupItem citem;
	for(U64 j=begin_seq;j<=end_seq;j++){
		if(logical_table_->GetEpochOfTheSeq(j, epoch)){
			citem.pn.epoch = epoch;
			citem.pn.sequence = j;
			if(logstore.CatchUp(& citem, -3)) {	//-2 represent fd
				key = citem.kv.key;
				value = citem.kv.value;
				if(datastore.WriteData(key, value, j)!=true){
					LOG(HCK,("insert leveldb error\n"));
					assert(0);
				}
			}
			else {
				LOG(HCK,("catch up epoch:%llu,sequence:%llu,key:%s error.\n",epoch,j,citem.kv.key));
				assert(0);
			}
		}
		else{
			LOG(HCK,("get epoch from logicaltruncated table error\n"));
			assert(0);
		}
	}
	logstore.CatchUpEnd(-3);
	LOG(HCK,("local recovery data ok\n"));
	return true;
	
}

void
FRecovery::Start(void){
	ProposalNum pn;
	if (!logical_table_->LogicalTableWarmUp()){
		LOG(HCK,("logical_table_   LogicalTableWarmUp error\n"));
		//send to the follower that i am remote recover is not ok
		rfmsger.SendMessageErr(); 
		assert(0);
	}
	int ret = mlstore.GetCommitPn(range_, pn);
	if (  ret == GETERROR ){
		assert(0);
	}else if (ret == GETEMPTY){
		pn.epoch = 0;
		pn.sequence = 0;	
		LOG(HCK,("no cmt store in the db before ---  cmt(epoch:sequence)=(0:0)\n"));
	}else{
		LOG(HCK,("got the cmt store cmt(epoch:sequence) = (%llu:%llu)\n",pn.epoch, pn.sequence));
	}
	set_catchup_sequence(pn.sequence + 1);
	if ( !flstates.Init(pn.epoch, pn.sequence, pn.sequence, range_) ){
		//send to the follower that i am remote recover is not ok
		rfmsger.SendMessageErr(); 		
		LOG(ERR,("mlstore infor  cause init flstate error\n"));	
		assert(0);
	}
	// fix me here there will be an local recovery use the log entry small the the cmt	
	//then remote recovery througth cacheup
	if (RemoteCatchUp()){
		LOG(HCK,("remote catchup ok\n"));		
	}else{				//if return false, the leader maybe been down ,elet leader
		//send to the follower that i am remote recover is not ok
		//rfmsger.SendMessageErr(); 
		LOG(HCK,("remote catchup error\n"));
	}	
}

/*
*catchup response message size
*decode format:msg_type:proposal numbers:endflag:[flag?:epoch:sequence:[ketlen:valuelen:key:value][.....][]] 
				u32		u32			u8        u8
*			//total_len, have been readed	
*/

inline U32
FRecovery::DecodeHeaderSize(struct catchup_header & ch){
	U32 size;
	//size = sizeof(struct catchup_header);  error
	size = sizeof(ch.endflag)  + sizeof(ch.proposal_numbers) + sizeof(ch.msg_type);
	return size;
}

bool 
FRecovery::DecodeCatchupAckHeader(char* buf, struct catchup_header  &ch){
	U32 msg_type;
	U32 proposalnum;
	U8 endflag;
	memcpy((void *)&(msg_type), buf, sizeof(msg_type));
	msg_type = ntohl(msg_type);
	buf += sizeof(msg_type);
	if( msg_type != CATCHUP_MESSAGE_ACK) {
		LOG(HCK,("non catchup ack recieved\n"));
		return false;
	}
	memcpy((void *)&(proposalnum), buf, sizeof(proposalnum));
	proposalnum = ntohl(proposalnum);
	buf += sizeof(proposalnum);

	memcpy((void*)&(endflag), buf, sizeof(endflag));
	//just a byte need not hton
	buf += sizeof(endflag);
	ch.init(msg_type, proposalnum, endflag);
	return true;
}


inline U32 
FRecovery::DecodeCatchupAckBody(char* buf,class FCatchupItem &fci){
	U32 size;
	memcpy((void*)&(fci.ifcmted), buf, sizeof(fci.ifcmted));
	//just a byte need not htonl
	buf += sizeof(fci.ifcmted);

	memcpy((void*)&(fci.pn.epoch), buf, sizeof(fci.pn.epoch));
	fci.pn.epoch = Ntohl64(fci.pn.epoch);
	buf += sizeof(fci.pn.epoch);
		
	memcpy((void*)&(fci.pn.sequence), buf, sizeof(fci.pn.sequence));
	fci.pn.sequence = Ntohl64(fci.pn.sequence);
	buf += sizeof(fci.pn.sequence);

	memcpy((void*)&(fci.key_len), buf, sizeof(fci.key_len));
	fci.key_len= ntohl(fci.key_len);
	buf += sizeof(fci.key_len);

	memcpy((void*)&(fci.value_len), buf, sizeof(fci.value_len));
	fci.value_len = ntohl(fci.value_len);
	buf += sizeof(fci.value_len);
		
	fci.kv.key = buf;
	fci.kv.value = buf + fci.key_len;	
	size =  sizeof(fci.ifcmted) + sizeof(fci.pn.epoch) + sizeof(fci.pn.sequence) 
			+ sizeof(fci.key_len) + sizeof(fci.value_len) + fci.key_len +  fci.value_len;
	return size;	
}

/*
*after have receivered the catchup entry , update the metainfos(include 
*logical truncated table the flstates, and so on)
*
*/
bool
FRecovery::LogAndUpdateMetainfo(class FCatchupItem &fci) {
	//store the log entry and update the metalog info	
	std::string key,value;
	U64 seq;
	if ( fci.pn.sequence ==  catch_sequence_){
		//update the loggical turncated tables
		if (fci.pn.epoch  == flstates.epoch()+1){				//update the follower's epoch
			//the former must have been cmtted, ensure 
			ProposalNum pn;
			pn.epoch = flstates.epoch();
			pn.sequence = catch_sequence_ -1;
			mlstore.StoreMetaInfo(flstates.rangenum(), pn);			
			// here we shoud update the loggical turncated tables (last-epoch + the last sequences)					
			logical_table_->UpdateLogicalTable(flstates.epoch(), catch_sequence_ -1);
			flstates.set_epoch(fci.pn.epoch);//update the epoch of follower  set the newer epoch				
		}
		//store the log entry	
		//flogstore.LogAppendProposal(fci);		
		//logstore.LogAppendProposal(fci);		
		if (fci.pn.epoch>logstore.get_current_epoch()) {
			LOG(HCK,("FUCKING DAY,come on baby,delete all of the record after cmt.\n"));
			//logstore.FollowerLogStart();
		}
		if (fci.pn.sequence>logstore.get_last_sequence()) {
			if (!logstore.AddRecord(fci)) {
				assert (0);
			}
		}
		else {
			LOG(HCK,("sequence:%llu has been added into log before.\n",fci.pn.sequence));
		}
		//update the cmt_sequence
		if (fci.ifcmted) {				
			if (fci.pn.sequence  == flstates.cmt_sequece() + 1){
				 flstates.update_cmt_sequence(fci.pn.sequence);

				 key = fci.kv.key;
				 value = fci.kv.value;
				 seq = fci.pn.sequence;
				 datastore.WriteData(key,value,seq);	// insert data into leveldb		
				 
				mlstore.StoreMetaInfo(flstates.rangenum(), fci.pn);
			}else{
				LOG(ERR,("the cmt is  not inc one by one, but i  is also ok(see point 27), set cmt sequnce\n"));
				// we should write the datas that from cmt+1 to fci.sequence into leveldb
				flstates.update_cmt_sequence(fci.pn.sequence);
				FRecoveryInsertData(flstates.cmt_sequece() + 1, fci.pn.sequence);
				mlstore.StoreMetaInfo(flstates.rangenum(), fci.pn);
			}							
		}	
		//update the lsn
		if (fci.pn.sequence == flstates.lsn_sequence() + 1) {
				flstates.inc_lsn_sequence();
		}
		catch_sequence_++;
		return true;
	}
	else{
			LOG(HCK,("this catchup item is not the next i wanted\n"));
			return false;
	}
}

/*
*	encode format : total_len:msg_type:sequence:
*					U32	  U32 	  U64
*/
inline bool  
FRecovery::EncodeCatchupMessage(char* buf, U32 &size, U64 sequence){	
	U32 netformat32;
	U64 netformat64;
	U32 total_len;
	U32 msg_type = CATCHUP_MESSAGE;
	total_len = sizeof(msg_type) + sizeof(sequence);
	size = total_len + sizeof(total_len); 
	if ( size  >  CATCHUP_MESSAGE_SIZE){
		LOG(ERR,("buf is too small\n"));
		return false;
	}
	netformat32 = htonl(total_len);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);
	
	netformat32 = htonl(msg_type);
	memcpy(buf, &netformat32,  sizeof(netformat32));
	buf += sizeof(netformat32);
	
	netformat64 = Htonl64(sequence);
	memcpy(buf, &netformat64,  sizeof(netformat64));
	buf += sizeof(netformat64);
	return true;	
}

/*
*send the catch up message to the leader ,
*will send error when the leader connection fail or others
*when error we the follwer  thread can just exit and try again(./followerstart)
*
*/

bool 
FRecovery::SendCatchupMessage(void){
	char buf[CATCHUP_MESSAGE_SIZE];
	U32 size;
	if ( !EncodeCatchupMessage(buf, size, catch_sequence_)){	
		return false;
	}
	if (TcpSend(catch_sock_, buf, size) ){
		LOG(HCK,("send ok\n"));
		return true;
	}else{
		LOG(ERR, ("send error\n"));
		close(catch_sock_);
		return false;
	}	
}

/*
*remotecatchup: 
*fixme attention:when return false , we shoud check if the leader have been down
*		if the follower have been down i shoud do the leader election					
*
*/
bool FRecovery::RemoteCatchUp(void){
	ssize_t ret;
	int errorcode;
	struct sockaddr_in leader_addr;
	//send message to the 
	if (( catch_sock_  = socket(AF_INET,SOCK_STREAM , 0)) < 0){
			assert(0);//assert and restart;
	}
	leader_addr.sin_family = AF_INET;
	leader_addr.sin_port = htons(LEADER_CATCHUP_PORT_NUM);
	leader_addr.sin_addr.s_addr = inet_addr(leader_ip_);
	LOG(HCK,("leader ip:%s,\tport:%d.\n",leader_ip_,LEADER_CATCHUP_PORT_NUM));
	if ( ConnectRetry(catch_sock_, (struct sockaddr*)&leader_addr, sizeof(leader_addr)) == -1){
		close(catch_sock_);
		LOG( VRB,("CONNECT_FAIL\n"));
		return false;	//return and elect leader
	}
	//set   tcp   async;
	/*
	int flags;		
	if ( ( flags = fcntl(catch_sock, F_GETFL, 0)) < 0 ||
		fcntl(catch_sock, F_SETFL, flags | O_NONBLOCK) < 0 ){
		cout << "Error::   set O_NONBLOCK error\n" << endl;
		close(catch_sock);
		assert(0);
	}
	*/
	
	//block or use the libevent?  //we can do other things after the catchup
	while (1) {
		if (SendCatchupMessage()) {
			ret = ReadvRec(catch_sock_, catch_up_ack_buf_,  CATCHUP_RETURN_ACK_SIZE,  errorcode);
			if (errorcode == EMSGSIZE){
				LOG(ERR,("catch ack too bigger\n"));
				continue;
			}else if (ret == 0){
				LOG(ERR,("leader catchup thread elegantly disconnect\n"));
				close(catch_sock_);
				return false;
			}else if (ret < 0){
				LOG(ERR,("leader catchup thread failed disconnected\n"));
				close(catch_sock_);
				return false;
			}else{
				LOG(HCK,("receive catchup ack ok\n"));
				/******************************/
				struct catchup_header ch;
				class FCatchupItem fci;
				if ( !DecodeCatchupAckHeader(catch_up_ack_buf_,ch)){
					LOG(ERR,("not catchup ack me\n"));
					continue;	//it shoud be fix?
				}
				if (ch.if_catchup_ended()) {
					//here waitup the follower thread for ok
					close(catch_sock_);
					//rfmsger.SendMessageOk();
					LOG(HCK,("LOG FOLLOWER METHOD START.\n"));
					//logstore.FollowerLogStart();	// here delete all of the records after cmt
					rfmsger.SendMessageOk();
					return true; 
				}
				char * buf = catch_up_ack_buf_ + DecodeHeaderSize(ch);
				U32 size;
				for(U32 i = 0; i != ch.proposal_numbers; i++) {
					size = DecodeCatchupAckBody(buf, fci);
					if (LogAndUpdateMetainfo(fci) ){
						LOG(HCK,("catch up sequence:%llu ok\n",fci.pn.sequence));	
					}else{
						LOG(HCK,("catch up sequence:%llu error\n",fci.pn.sequence));	
						break;  //go on catchup the correct next  proposal
					}
					buf += size;
				}			
			}
		}
		else{	
			LOG(HCK,(" error, the leader sock closed\n"));
			close(catch_sock_);
			//here waitup the follower thread for error,the thread shoud exist
			rfmsger.SendMessageErr(); 
			return false;
		}
	}
}
