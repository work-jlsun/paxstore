/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#include "valuelist.h"

#include <assert.h>

ValueList::ValueList(){
	if ( pthread_mutex_init(&vlist_lock_, NULL)  != 0){
		assert(0);
	}
	vlist_.clear();
}


ValueList::~ValueList(){
	if ( pthread_mutex_destroy(&vlist_lock_)  != 0){
		assert(0);
	}
	vlist_.clear();	//dequeu must have an destructor, it may be not needed here
}

 std::size_t ValueList::GetCount(void){
	std::size_t cnt;
	pthread_mutex_lock(&vlist_lock_);
	cnt = vlist_.size();
	pthread_mutex_unlock(&vlist_lock_);
	return cnt;
}

bool ValueList::ValueEnqueue(class ClientKeyValue*   cvalue) {
	std::size_t cnt;
	pthread_mutex_lock(&vlist_lock_);
	cnt = vlist_.size();
	LOG(VRB,("vlist_.size: %d", cnt)) ;
	if (cnt >= LEADER_MAX_QUEUE_LENGTH){
		pthread_mutex_unlock(&vlist_lock_);
		LOG(VRB,("enqueue error\n"));
		return false;
	}
	vlist_.push_front(cvalue);
	pthread_mutex_unlock(&vlist_lock_);	
	return true;
}

bool ValueList::PopNextValue(class ClientKeyValue*& cvalue) {	
	bool flag;
	pthread_mutex_lock(&vlist_lock_);
	if (vlist_.empty()){
		LOG(VRB,("PopNextValue empty\n")) ;
		flag = false;	
	}else{
		cvalue = vlist_.back(); 
		vlist_.pop_back();		//pop out , then we shoud 
		flag = true;
	}
	pthread_mutex_unlock(&vlist_lock_);	
	return flag; 
}

bool ValueList::GetBackValue(class ClientKeyValue* &cvalue){	
	bool flag;
	pthread_mutex_lock(&vlist_lock_);
	if (vlist_.empty()){
		LOG(VRB,("PopNextValue  not got the value\n")) ;
		flag = false;	
	}else{
		 cvalue = vlist_.front(); 
		flag = true;
	}
	pthread_mutex_unlock(&vlist_lock_);	
	return flag; 
}
