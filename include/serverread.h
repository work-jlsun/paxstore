/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: read data from server storage engine and send it to client
*/

#ifndef PAXSTORE_SERVERREAD_
#define PAXSTORE_SERVERREAD_
#include <string>
#include <map>

#include <event.h>

#include "valuelist.h"
#include "tools.h"
#include "cfg.h"

bool ValidateReadMsg(char *, std::string &key, ClientEvent *);
bool EncodeReadResponse(std::string value, int fd,bool flag);
bool SendReadData(std::string value,int fd,bool flag);

void ServerRead_Accept(int, short, void*);
void ServerRead_Read(int, short, void*);

class ServerRead{
friend void ServerRead_Accept(int, short, void*);
friend void ServerRead_Read(int, short, void*);
public:
	ServerRead();		
	explicit ServerRead(int port_num);
	virtual ~ServerRead();
	bool Start();		

	int listenfd(void){
		return listenfd_;
	}
	bool Init(bool flag);
private:
	bool IfIamLeader;
	std::map<int,bool> mp;
	int listenfd_;
	int port_; //server listening port
	struct event_base *base_;
	struct event ev_accept_;
	DISALLOW_COPY_AND_ASSIGN(ServerRead);
};
#endif
