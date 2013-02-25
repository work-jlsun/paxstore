/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: control the role change
*/

#ifndef __ZKCONTROL__
#define __ZKCONTROL__
#include <string.h>

#include "kvproposal.h"

class PassArgsToFollower{
public:
	U32 range_num_;
	U32 leader_id_;
	char leader_ip_[20];
	void init(U32 range_num, U32 leader_id, char *ip){
		range_num_ = range_num;
		leader_id_  = leader_id;
		strcpy(leader_ip_, ip);
	}
private:
};


#define INITIAl 0
#define  LEADER   1
#define FOLLOWER 2
class ZKControl{
public:
	ZKControl(){
		my_state_  = INITIAl;
		if_normal_ = true;
	}
	~ZKControl(){
		
	}
	bool StartLeader(pthread_t leadercatchupthread);
	bool StopLeader(void);

	bool StartFollower(class PassArgsToFollower & pass_args);
	bool StopFollower(void);
	
	void set_my_state(U32 state){
		my_state_ = state;	
	}
	U32 my_state(void){
		return my_state_;
	}
	bool if_normal(void){
		if ( if_normal_){
			return true;
		}else{
			return false;
		}
	}
	void set_if_normal(bool flag){
		if_normal_ = flag;
	}	
private:
	U32 my_state_;
	bool if_normal_;  //if the state is ok(maybe have exit)
	/*
	*leaderthreads[4] : leadercatchupthread
	*leaderthreads[3] : commitsync_thread
	*leaderthreads[2] : logwriter_thread
	*leaderthreads[1] : proposer_thread
	*leaderthreads[0] : tcpserver_thread	
	*/
	pthread_t leaderthreads[6];

	/*
	*followerthreads[0]:recovery_thread
	*followerthreads[1]:follower_thread
	*followerthreads[2]:follower_read_thread
	*/
	pthread_t followerthreads[3];	
};
#endif
