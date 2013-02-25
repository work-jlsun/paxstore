/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: control the role change
*/

#include "zkcontrol.h"

#include <pthread.h>

#include "leaderthreads.h"
#include "follower.h"
#include "frecover.h"

bool ZKControl::StartLeader(pthread_t leadercatchupthread){
	int err;
	set_my_state(LEADER);
	//set the leadercatchupthread, this thread have been started at leaderfailover phase
	leaderthreads[5] = leadercatchupthread;

	err = pthread_create(&(leaderthreads[4]), NULL ,read_thread, NULL);
	if (0 != err){
	        LOG(HCK,( "read_thread error\n"));	 		
		goto out5;
	}	
	err = pthread_create(&(leaderthreads[3]), NULL ,commitsync_thread, NULL);
	if (0 != err){
	        LOG(HCK,( "commitsync_thread error\n"));	 		
		goto out4;
	}	
	
	err = pthread_create(&(leaderthreads[2]), NULL, logwriter_thread, NULL);
	if (0 != err){
   		LOG(HCK,( "logwriter_thread error\n"));				
		goto out3;
	}
	err = pthread_create(&(leaderthreads[1]), NULL, proposer_thread, NULL);
	if (0 != err){
   		LOG(HCK,( "proposer_thread error\n"));					
		goto out2;
	}

	err = pthread_create(&(leaderthreads[0]), NULL, tcpserver_thread, NULL);
	if (0 != err){
		LOG(HCK,( "tcpserver_thread error\n"));		
		goto out1;
	}
	return true;
out1:
	pthread_cancel(leaderthreads[1]);
out2:
	pthread_cancel(leaderthreads[2]);	
out3:
	pthread_cancel(leaderthreads[3]);
out4:	
	pthread_cancel(leaderthreads[4]);
out5:
	pthread_cancel(leaderthreads[5]);
	return false;
}

bool ZKControl::StopLeader(void){
	bool retflag = true;
	int ret;
	ret = pthread_cancel(leaderthreads[0]);
	if (ret != 0){
		LOG(HCK,("stop tcpserver_thread error\n"));
		retflag = false;
	}

	ret = pthread_cancel(leaderthreads[1]);
	if (ret != 0){
		LOG(HCK,("stop proposer_thread error\n"));
		retflag = false;
	}	

	ret = pthread_cancel(leaderthreads[2]);
	if (ret != 0){
		LOG(HCK,("stop logwriter_thread error\n"));
		retflag = false;
	}	

	ret = pthread_cancel(leaderthreads[3]);
	if (ret != 0){
		LOG(HCK,("stop commitsync_thread error\n"));
		retflag = false;
	}	

	ret = pthread_cancel(leaderthreads[4]);
	if (ret != 0){
		LOG(HCK,("stop read_thread error\n"));
		retflag = false;
	}		
	ret = pthread_cancel(leaderthreads[5]);
	if (ret != 0){
		LOG(HCK,("stop leadercatchupthread error\n"));
		retflag = false;
	}		

	set_my_state(INITIAl);
	return retflag;
}

bool ZKControl::StartFollower(class PassArgsToFollower & pass_args){
	int err;
	set_my_state(FOLLOWER);
	// logstore.FollowerLogStart(); //-----------------------------------------------------------
	PassArgsToFollower *thread_args = new PassArgsToFollower();
	thread_args->init(pass_args.range_num_, pass_args.leader_id_, pass_args.leader_ip_);
	rfmsger.ClearTrigger();
	err = pthread_create(&(followerthreads[0]), NULL, recovery_thread, (void *)thread_args);
	 if (0 != err){
		LOG(HCK,("start recovery_thread error\n"));			
		goto out1;		
	}
	err = pthread_create(&(followerthreads[1]), NULL,follower_thread, NULL);
	if (0 != err){
		LOG(HCK,("start follower_thread error\n"));					
		goto out2;
	}

	err = pthread_create(&(followerthreads[2]), NULL ,follower_read_thread, NULL);
	if (0 != err){
	        LOG(HCK,( "read_thread error\n"));	 		
		goto out3;
	}	

	return true;
out3:
	pthread_cancel(followerthreads[1]);
out2:
	pthread_cancel(followerthreads[0]);
out1:
	return false;
}

bool ZKControl::StopFollower(void){
	bool retflag = true;
	int ret;
	ret = pthread_cancel(followerthreads[2]);
	if (ret != 0){
		LOG(HCK,("stop follower_read_thread error\n"));
		retflag = false;
	}	

	ret = pthread_cancel(followerthreads[1]);
	if (ret != 0){
		LOG(HCK,("stop follower_thread error\n"));
		retflag = false;
	}	
	// the recovery_thread maybe have exit by itself normaly
	//in this condition pthread_cancel return 3
	ret = pthread_cancel(followerthreads[0]);
	if (ret != 0){
		LOG(HCK,("stop recovery_thread error\n"));
		retflag = false;
	}	
	set_my_state(INITIAl);
	usleep(2000); //wait the threads pthread_cleanup_pop function to be called
	return retflag;
}
