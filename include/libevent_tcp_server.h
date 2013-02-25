/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang 
  * email:  
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#ifndef __LIBEVENT_TCP_SERVER__
#define __LIBEVENT_TCP_SERVER__
#include <event.h>

#include "valuelist.h"
#include "tools.h"
#include "cfg.h"

class  ClientKeyValue;

extern  class ValueList   GlobleValueList;
extern class SocketPair     sockpair;

typedef struct ClientMsg{
	//U32 total_len;
	U32 key_len;
	U32 value_len;
	char	*kv;
}ClientMsg ;

bool ValidateClientMsg(char *, class ClientKeyValue *, ClientEvent *);
void OnAccept(int, short, void*);
void OnRead(int, short, void*);

/*
*the  TcpServer is used for us to reciver the key/value pair from the client
*
*
*
*/
class TcpServer{
friend void OnAccept(int, short, void*);
friend void OnRead(int, short, void*);
public:
	TcpServer();		
	TcpServer(int port_num);
	bool Start(void);			
	virtual ~TcpServer();
private:
	int listenfd_;
	int port_; //server listening port
	struct event_base *base_;
	struct event ev_accept_;
	DISALLOW_COPY_AND_ASSIGN(TcpServer);
};
#endif
