/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#ifndef __LOGWORKER__
#define __LOGWORKER__
#include "log_manager.h"
#include "proposer.h"
#include "tools.h"
#include "pqueue.h"
#include "leadercatchup.h"

//the  log worker need the Storage ,but the cache up worker  also need the same storage
// the bdb is not the log_worker's attribute, it not contain the relationship just as  
//"bird"->"parrot"
extern class SocketPair     ltpsockpair;  
extern class LogManager logstore;
//extern class Storage logstore;


class log_worker
{
public:
	log_worker();
	~log_worker();
	void start(void);
private:
	
	//U64	lsn;		// it  is  my last  logged entry
						// if i am in recovery,it may shoud be set to the commit value
						// or other value's
	struct event_base *base;
	struct event log_msg_event;	
};
void log_msg_handle(int, short, void*);
#endif
