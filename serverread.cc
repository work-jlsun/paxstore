/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: read data from server storage engine and send it to client
*/

#include "serverread.h"

#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstdlib>
#include <cstring>

#include "followerlist.h"
#include "leadercatchup.h"
#include "data_store.h"

char ReadResponseBuffer[MAX_TCP_MSG_SIZE];

bool ValidateReadMsg(char *buf, std::string &key, ClientEvent *ce) {
	//fix me ,now this may need crc check
	if (0) {	
		//crccheck
		return false;
	}
	U32 key_len;
	memcpy(&key_len, buf, sizeof(key_len));
	key_len = ntohl(key_len);
	key = (char *) (buf+ sizeof(U32));
	return true;
}

/*
*encode read response format :  "total_len:msg_type:value_len:value:msg_type:ip_num:ip_len:ip:ip_len:ip..."
*
*/
bool EncodeReadResponse(std::string value, U32 & size ,bool flag) {
	U32 total_len,value_len,tmp;
	U32 netformat32;
	U32 followernum,i,distance,iplen;
	char *buf  =  ReadResponseBuffer;
	total_len =  value.size() + 2 * sizeof(U32);
	value_len = value.size() + 1;
	
	tmp = htonl(SENDREAD_MESSAGE);
	memcpy(buf + sizeof(U32),&tmp,sizeof(U32));	// message type
	tmp = htonl(value_len);
	memcpy (buf+2*sizeof(U32), &tmp, sizeof(U32));	// value len
//	snprintf(buf +  2 * sizeof(U32), sizeof(buf), "%s",value.c_str());
//	snprintf(buf +  3 * sizeof(U32), sizeof(ReadResponseBuffer), "%s",value.c_str());	// value
	memcpy(buf+ 3*sizeof(U32),value.c_str(),value_len);

	if (flag) {
		followernum = flws.FollowerLiveNum();
		if (followernum != 0)
			distance = 3 * sizeof(U32) + value_len;
			tmp = htonl(SENDFOLLOWERIP_MESSAGE);
			memcpy(buf + distance,&tmp,sizeof(U32));		// send ip message type
			total_len += sizeof(U32);
			distance += sizeof(U32);
			tmp = htonl(followernum);
			memcpy(buf + distance,&tmp,sizeof(U32));
			total_len += sizeof(U32);
			distance += sizeof(U32);
			for( i = 0; i < flws.FollowerNum(); i++) {
				//iplen = strlen(flws.fvec_[i].ip_) +1;
				if ((flws.GetFollowerInfo(i)->state_ == NEWITEM ) || ( flws.GetFollowerInfo(i)->state_ ==  CONNECTED)) {
					iplen = strlen(flws.GetFollowerInfo(i)->ip_) + 1;
					tmp = htonl(iplen);
					memcpy(buf+distance,&tmp,sizeof(U32));			// follwer[i].iplen
					distance += sizeof(U32);
					//snprintf(buf +  distance, sizeof(ReadResponseBuffer), "%s",flws.fvec_[i].ip_);	// follower[i].ip
					memcpy(buf + distance ,flws.GetFollowerInfo(i)->ip_,iplen);
					distance += iplen;
					total_len += sizeof(U32);
					total_len += iplen;
				}
			}		
	}
	tmp = htonl(total_len);
	memcpy(buf, &tmp,  sizeof(tmp));		// total len
	size = total_len + sizeof(total_len);
	if ( size > MAX_TCP_MSG_SIZE ) {
		LOG(HCK,("read result too bigger\n"));
		return false;
	}
	return true;
}

bool SendReadData(std::string value, int fd,bool flag) {
	U32 size;
	if ( !EncodeReadResponse(value,size,flag) ) {
		LOG(HCK,( "read response buffer is small\n"));
		return false;	
	}
	 if ( TcpSend(fd, ReadResponseBuffer, size) ) {
		LOG(HCK,("send back read result ok \n"));
		return true;
	 } else {
	 	 LOG(HCK,("send back read result error \n"));
		return false;
	}	
}

void ServerRead_Accept(int fd, short ev, void* arg) {
	ServerRead *ts = (ServerRead *)arg;
	int client_fd;
	//int flags;
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);
	client_fd = accept(fd, (struct sockaddr*)&client_addr, &client_len);
	if (client_fd == -1) {
		LOG(ERR,("Error:accept client failed\n"));
		return;
	}
	std::map<int,bool>::iterator map_it;
	map_it = ts->mp.find(client_fd);
	if (map_it == ts->mp.end()) {
		ts->mp.insert(std::pair<int,bool>(client_fd,true));
	}
	else {
		// fix me delete the former client_fd and insert the new client_fd or we don't delete the former and only change the bool
		map_it->second = true;
		//assert(0);
	}
	ClientEvent* ce = new ClientEvent(client_fd, &client_addr);  //c++ fromat
	PassArg * pa = new PassArg((void*)ts,(void*)ce);
	event_set(&ce->ev_read_, client_fd, EV_READ | EV_PERSIST, ServerRead_Read, pa);
	event_base_set(ts->base_, &ce->ev_read_);
	event_add(&ce->ev_read_, NULL);	//if receive a request before event register,					
}

void ServerRead_Read(int fd, short ev, void* arg) {
	int errcode = 0,i;
	//static int number = 0;
	PassArg * pa = (PassArg *)arg;	
	ServerRead *lr = (ServerRead *)pa->first();
	ClientEvent *ce = (ClientEvent *)pa->second();
	char ret;
	char buf[MAX_TCP_MSG_SIZE];
	memset(buf,0x00, sizeof(buf));
	std::map<int,bool>::iterator map_it;
	map_it = lr->mp.find(fd);
	if(map_it == lr->mp.end()){
		assert(0);
	}
	ssize_t nsize = ReadvRec(fd, buf, MAX_TCP_MSG_SIZE, errcode);
	if ( errcode == EMSGSIZE ) {
		LOG(ERR,("message bigger than buffer\n"));
		return;
	}	
	if (nsize == 0) {		
		LOG(ERR,("client elegantly disconnect\n"));
		// delete ce;
		ce->lock();
		close(fd);
		event_del(&ce->ev_read_);		
		ce->active_ = false;	
		ce->unlock();
		lr->mp.erase(map_it);
		return;	
	} else if (nsize < 0) {
		LOG(ERR,("client failed disconnected\n"));
		ce->lock();
		close(fd);
		event_del(&ce->ev_read_);
		ce->active_ = false;	
		ce->unlock();
		lr->mp.erase(map_it);
		return;
	} else {	
		if (ifsendfollowerlist) {
			for (map_it = lr->mp.begin(); map_it != lr->mp.end(); ++map_it) {	
				// notify all of the client that leader's followerlist has changed
				map_it->second = true;
			}
			ifsendfollowerlist = false;
			map_it = lr->mp.find(fd);		// put the map_it to fd again
			if (map_it == lr->mp.end()) {
				assert(0);
			}
		}
		std::string key,value;
		if (ValidateReadMsg(buf,key,ce)) {
			datastore.ReadData(key,value);		// read the data from leveldb
			LOG(HCK,("key:%s\nvalue:%s\n",key.c_str(),value.c_str()));
			bool ifsendfollower = map_it->second;
			if (ifsendfollower)
				map_it->second = false;		//set the client's flag to false,then the next time,we needn't to send the followerlist ip again
			if (lr->IfIamLeader)
				SendReadData(value, fd,ifsendfollower);				//then send the data			
			else
				SendReadData(value, fd, false);		// if this node is not leader ,it will not send followerlist ip to client
		}
	}
}

ServerRead::ServerRead(int port_num):port_(port_num),base_(NULL) {
	base_ = event_base_new();
	if (!base_) {
		exit(0);
	}	
}

ServerRead::ServerRead():port_(READ_PORT_NUM),base_(NULL) {
	base_ = event_base_new();
	if (!base_) {
		exit(0);
	}	
}

ServerRead::~ServerRead() {
	if (base_) {
		event_base_free(base_);
	}	
}

bool ServerRead::Start() {		// to tell this node if i am leader,follower and leader will do different thing when reading values.
	event_set(&ev_accept_, listenfd_, EV_READ | EV_PERSIST, ServerRead_Accept, this);
	event_base_set(base_,&ev_accept_);
	event_add(&ev_accept_, NULL);
	event_base_dispatch(base_);	
	return true;
}

bool ServerRead::Init(bool flag){
	IfIamLeader = flag;
	if (base_ !=NULL) {
		if( !ListenerInit( listenfd_, port_)) {
			LOG(ERR,("Error: tcp server start error\n"));
			return false;
		}
	} else {
		return false;
	}
	return true;
}
