/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang 
  * email:  
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#include "libevent_tcp_server.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

#include <cstdlib>
#include <cstring>

class ValueList   GlobleValueList;
class SocketPair     sockpair;   //the  message method of the   "lib_tcp_server"  and "proposer worker"

TcpServer::TcpServer():port_(DEFAULT_PROT_NUM),base_(NULL){
	//in the constructor, we shoud do something trivial
}
		
TcpServer::TcpServer(int port_num):port_(port_num),base_(NULL){
	base_ = event_base_new();
	if (!base_){
		exit(0);
	}	
}		

bool TcpServer::Start(void){
	base_ = event_base_new();
	if (base_ !=NULL) {
		if( !ListenerInit( listenfd_, port_)) {
			LOG(ERR,("Error: tcp server start error\n"));
			return false;
		}
	}else{
		return false;
	}
	event_set(&ev_accept_, listenfd_, EV_READ | EV_PERSIST, OnAccept, this);
	event_base_set(base_,&ev_accept_);
	event_add(&ev_accept_, NULL);
	event_base_dispatch(base_);	
	return true;
}

TcpServer::~TcpServer(){
	if(base_){
		event_base_free(base_);
	}	
}

void OnAccept(int fd, short ev, void* arg){
	TcpServer *ts = (TcpServer *)arg;
	int client_fd;
	//int flags;
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);
	client_fd = accept(fd, (struct sockaddr*)&client_addr, &client_len);
	if (client_fd == -1){
		LOG(ERR,("Error:accept client failed\n"));
		return;
	}
	/*
	if ( (flags = fcntl(client_fd, F_GETFL, 0) < 0) ||
		fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) < 0 ){
		cout << "Frror:: set client socket  O_NONBLOCK error\n" << endl;
	}
	*/
	ClientEvent* ce = new ClientEvent(client_fd, &client_addr);  //c++ fromat
	event_set(&ce->ev_read_, client_fd, EV_READ | EV_PERSIST, OnRead, ce);
	event_base_set(ts->base_, &ce->ev_read_);
	event_add(&ce->ev_read_, NULL);	//if receive a request before event register,
					// will it be triggered?
}

void OnRead(int fd, short ev, void* arg){
	int errcode = 0;
	static int number = 0;
	ClientEvent *ce = (ClientEvent *)arg;
	char ret;
	char buf[MAX_TCP_MSG_SIZE];
	memset(buf,0x00, sizeof(buf));
	ssize_t nsize = ReadvRec(fd, buf, MAX_TCP_MSG_SIZE, errcode);
	if ( errcode == EMSGSIZE ){
		LOG(ERR,("message bigger than buffer\n"));
		return;
	}	
	if (nsize == 0){		
		LOG(ERR,("client elegantly disconnect\n"));
		// delete ce;
		ce->lock();
		close(fd);
		event_del(&ce->ev_read_);		
		ce->active_ = false;	
		ce->unlock();
		return;	
	}else if (nsize < 0){
		LOG(ERR,("client failed disconnected\n"));
		ce->lock();
		close(fd);
		event_del(&ce->ev_read_);
		ce->active_ = false;	
		ce->unlock();
		return;
	}else{	//have recive some data
	      //read the data and give it to the proposal queue;
	     //now just test the server side stub
	     //	cout << " package" << number << ":" << nsize 
	//	     << " datas:"	<< buf << endl;
		number++;
		class ClientKeyValue *cvalue  = new class ClientKeyValue();
		if (ValidateClientMsg(buf, cvalue, ce)){
			if ( !GlobleValueList.ValueEnqueue(cvalue) ){
				LOG(HCK,("clientkeyvalue enqueue error\n"));
				ret = BUSY;
				write(fd, &ret, sizeof(char));		// return the client that i am busy, i am not accept you request , the client may try later	
			} else{
				//cout << "clientkeyvalue enqueue ok, message the proposer\n";
				//message the proposal;
				//int wsfd = sockpair.GetWriteSock();
				sockpair.SendMessage(); // send message to is self ,wait for use the next time 
			}
		}else { 
			delete cvalue;
			ret = BUSY;	
			LOG(ERR,("ValidateClientMsg error\n"));
			write(fd, &ret, sizeof(char));	
		}		
	}
}

bool ValidateClientMsg(char * buf, class ClientKeyValue *cvalue , ClientEvent *ce){
	ClientMsg 	cmsg ;
	
	//fix me ,now this may need crc check
	if (0){	
		//crccheck
		return false;
	}
	
	U32 key_len;
	memcpy(&key_len, buf, sizeof(U32));
	key_len = ntohl(key_len);

	U32 value_len;
	memcpy(&value_len, buf + sizeof(U32), sizeof(U32));
	value_len = ntohl(value_len);

	cmsg.key_len = key_len;
	cmsg.value_len = value_len;
	cmsg.kv = (char *)(buf + 2 * sizeof(U32));

	cvalue->ce = ce;
	if ( (cvalue->kv.key = (char *)malloc(cmsg.key_len) )){
		memcpy(cvalue->kv.key, cmsg.kv, cmsg.key_len);
	}else{
		return false;
	}
	
	if ( (cvalue->kv.value = (char*)malloc(cmsg.value_len) )){
		memcpy(cvalue->kv.value, cmsg.kv + cmsg.key_len, cmsg.value_len);
	} else {
		return false;
	}
	return true;
}
