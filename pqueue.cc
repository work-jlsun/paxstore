/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#include "pqueue.h"

#include <sys/time.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "leadercatchup.h"

class P12Info  Global_P12Info;
class ProposalQueue  Global_ProposalQueue;

QProposal::QProposal(){
	pinstance_.pn.epoch = 0;
	pinstance_.pn.sequence = 0;
	pinstance_.kv.ce = NULL;
	pinstance_.kv.kv.key = NULL;
	pinstance_.kv.kv.value = NULL;
	promise_cnt_ = 0;
	promise_bitmap_ =0;
	state_ = initial;
	timeout_.tv_sec = 0;
	timeout_.tv_usec = 0;
}

QProposal::QProposal(class ClientKeyValue *cvalue, U64 sequence){
	pinstance_.pn.epoch = Global_P12Info.epoch();
	pinstance_.pn.sequence = sequence;
	pinstance_.kv.ce = cvalue->ce;
	pinstance_.kv.kv.key = cvalue->kv.key;
	pinstance_.kv.kv.value = cvalue->kv.value;
	promise_cnt_ = 0;
	promise_bitmap_ =0;
	state_ = initial;
	timeout_.tv_sec = 0;
	timeout_.tv_usec = 0;
	delete cvalue;  //it is new in the On_read , now it will enter the commit queue,delete it
}

void QProposal::Init(class ClientKeyValue *cvalue, U64 sequence){
	pinstance_.pn.epoch = Global_P12Info.epoch();
	pinstance_.pn.sequence = sequence;
	pinstance_.kv.ce = cvalue->ce;
	if(pinstance_.kv.kv.key != NULL){		// the proposal data is lazy deleted
		LOG(VRB,("QProposal init,p.kv.kv.key:%p\n",pinstance_.kv.kv.key));
		free(pinstance_.kv.kv.key);
	}
	pinstance_.kv.kv.key = cvalue->kv.key;
	if(pinstance_.kv.kv.value != NULL){
		LOG(VRB,("QProposal init,p.kv.kv.value:%p\n",pinstance_.kv.kv.value));
		free(pinstance_.kv.kv.value);
	}
	pinstance_.kv.kv.value = cvalue->kv.value;
	promise_cnt_ = 0;
	promise_bitmap_ =0;
	state_ = initial;
	timeout_.tv_sec = 0;
	timeout_.tv_usec = 0;
	delete cvalue;  //it is new in the On_read , now it will enter the commit queue,delete it
}

QProposal::~QProposal(){
	if (pinstance_.kv.kv.key){
		delete pinstance_.kv.kv.key;
		//p.kv.key = NULL;
	}
	if (pinstance_.kv.kv.value){
		delete pinstance_.kv.kv.value;
		//p.kv.value = NULL;
	}
}

void QProposal::Clear(void){
	pinstance_.pn.epoch = 0;
	pinstance_.pn.sequence = 0;	
	if (pinstance_.kv.kv.key){
		delete pinstance_.kv.kv.key;
		pinstance_.kv.kv.key = NULL;
	}
	if (pinstance_.kv.kv.value){
		delete pinstance_.kv.kv.value;
		pinstance_.kv.kv.value = NULL;
	}
	promise_cnt_ = 0;
	promise_bitmap_ =0;
	state_ = initial;
	timeout_.tv_sec = 0;
	timeout_.tv_usec = 0;
}

void QProposal::SetTimeOut(unsigned int usec){
	struct timeval current_time;
	gettimeofday(&current_time, NULL);
	timeout_.tv_sec = current_time.tv_sec + (usec / 1000000 );
	unsigned int usec_sum =  current_time.tv_usec + usec%1000000;
	if(usec_sum > 1000000) {
			timeout_.tv_sec += 1; 
	}
	
	timeout_.tv_usec = usec_sum % 1000000;
}

bool QProposal::IsTimeOut(void){
	struct timeval current_time;
	gettimeofday(&current_time, NULL);
	if( current_time.tv_sec > timeout_.tv_sec) {
		return true;				
	}else if(current_time.tv_sec == timeout_.tv_sec){
			if ( current_time.tv_usec >= timeout_.tv_usec){
				return true;
			}else {
				return false;
			}
	}else{
		return false;
	}
}

ProposalQueue::ProposalQueue(){
	if(!pthread_rwlock_init(&plock_, NULL)){
		//noop

	}else{
		assert(0);
	}
}

ProposalQueue::~ProposalQueue(){
	if(!pthread_rwlock_destroy(&plock_)){
		//noop
	}else{
			assert(0);
	}
}

bool ProposalQueue::GetQproposal(class CatchupItem *citem){
	QProposal * qpr = GetQproposal(citem->pn.sequence);
	proposal * pr =  qpr->ppinstance();

	if(citem->pn.sequence >= Global_P12Info.next_unused_sequence()){	// the proposal not ready here
		LOG(HCK,("The proposal have not been genarated completely\n"));
		return false;
	}
	//fixme acctualy i shoud get  a pthread lock of the proposal, because id maybe reinit and freed(but here the 
	//possobility is very very very small)  
	if ( (pr->pn.epoch != citem->pn.epoch)   || (pr->pn.sequence != citem->pn.sequence)){
		LOG(ERR, ("catchup sequence not in the pqueue\n"));
		return false;
	}
	citem->key_len  = strlen(pr->kv.kv.key);
	citem->value_len = strlen(pr->kv.kv.value);
	if ((citem->key_len + citem->value_len) > CATCHUP_ITEM_BUF_SIZE ){
		LOG(ERR, ("CATCHUP_ITEM_BUF_SIZE is too small?\n"));
		return false;
	}
	citem->kv.key = (char *)malloc(citem->key_len+1);
	citem->kv.value = (char *)malloc(citem->value_len+1);
	memset(citem->kv.key,0,citem->key_len+1);
	memset(citem->kv.value,0,citem->value_len+1);
	memcpy(citem->kv.key, pr->kv.kv.key, citem->key_len);
	memcpy(citem->kv.value,pr->kv.kv.value,citem->value_len);
	return true;	
}
