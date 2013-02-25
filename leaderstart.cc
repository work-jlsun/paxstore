/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: manage leader threads
*/

#include <pthread.h>

#include "leaderthreads.h"

int main(void){
	pthread_t tid[5];
	int err;
	err = pthread_create(&(tid[4]), NULL ,commitsync_thread, NULL);
	if (0 != err){
	        LOG(HCK,( "commitsync_thread error\n"));	 		
		return 0;
	}	
	
	err = pthread_create(&(tid[3]), NULL ,leadercatchup_thread, NULL);
	if (0 != err){
   		LOG(HCK,( "LeaderCatchup_thread error\n"));		
		return 0;
	}	
	
	err = pthread_create(&(tid[2]), NULL, logwriter_thread, NULL);
	if (0 != err){
   		LOG(HCK,( "logwriter_thread error\n"));				
		return 0;
	}
	err = pthread_create(&(tid[1]), NULL, proposer_thread, NULL);
	if (0 != err){
   		LOG(HCK,( "proposer_thread error\n"));					
		return 0;
	}

	err = pthread_create(&(tid[0]), NULL, tcpserver_thread, NULL);
	if (0 != err){
		LOG(HCK,( "tcpserver_thread error\n"));		
		return 0;
	}
	
	int i = 5;
	void *p;
	while (i--){
		err = pthread_join(tid[i], &p);
		if (err != 0){
			LOG(HCK,( "pthread_join error\n"));		
			
		}
	}
	return 0;
}
