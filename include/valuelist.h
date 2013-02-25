/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#ifndef __VALUELIST__
#define __VALUELIST__
#include <deque>

#include <pthread.h>

#include "cfg.h"
	
class  ClientKeyValue;
/***************************************************************************
*here is the queue of fifo
*              ValueEnqueue  -->  | | | | | | | | | | | | | | -->PopNextValue
		GetBackValue							  -->get_next_value  "but not pop"	
***************************************************************************/
class ValueList{	 
#define LEADER_MAX_QUEUE_LENGTH 10000
public:
	ValueList();
	~ValueList();
	std::size_t GetCount(void);
	bool ValueEnqueue(class ClientKeyValue *);
	bool PopNextValue(class ClientKeyValue*&);
	bool GetBackValue(class ClientKeyValue* &) ;   //not pop the value,just get the pointer	
private:
	std::deque<class ClientKeyValue*>  vlist_;
	pthread_mutex_t   vlist_lock_;	
	DISALLOW_COPY_AND_ASSIGN(ValueList);
};
#endif
