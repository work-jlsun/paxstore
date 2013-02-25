/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: to read or write key value data from/to server(leader or followers)
*/

#ifndef PAXSTORE_CLIENT_
#define PAXSTORE_CLIENT_
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>

#include <pthread.h>
#include <zookeeper/zookeeper.h>

#include "cfg.h"
#include "kvproposal.h"
#include "rangetable.h"

class Server{
public:		
	Server(){};
	~Server(){
		if(ip_)
			free(ip_);
	};
	void Init(U32 fid, char *fip, int rangenum){
		id_  = fid;
		if ( !(ip_ = (char *)malloc(strlen(fip) + 1)) ){
			assert(0);
		}
		strcpy(ip_,fip);
		old_fd_ = new_fd_ = 0;
		writefd = 0;
		state_ = NEWITEM;
		ifisleader_ = false;
		ifwriteconnect = false;
		connecttime = 0;
		rangenum_ = rangenum;
	}
	void setleader(){
		ifisleader_ = true;
	}
	void setwriteconnect(){
		ifwriteconnect = true;
	}
	U32 id_;
	char *ip_;
	struct sockaddr_in  old_follower_addr_;
	struct sockaddr_in  new_follower_addr_;
	int connecttime;   	// tell us that client try to connet how many times and failed
	bool ifisleader_;
	bool ifwriteconnect;	// to tell us if client has been connected to leader for write only when this node is leader
	int old_fd_;
	int writefd;
	int new_fd_;
	U32 state_;
	int rangenum_;		// to tell us that this server manage which key range number records
};

class Client {
	friend bool IfIpExistInserver(char *,int rangenum);
	friend bool DecodeValue(char *str,std::string &value,U32 total_len,int rangenum);
public:
	Client();
	~Client() {}

	bool Start();

	bool ReadMessage(int index);
	bool ReadRecord(std::string key,bool consistent);
	bool WriteRecord(std::string key,std::string value);

	bool ConnectZoo();
	bool ZooGetChildren(const char* dir, watcher_fn wfunc,struct String_vector &childrens);

	bool WriteConnect(int rangenum);
	short ReadConnect(U32 index,int rangenum);

	bool GetLeader(int rangenum,char *str);		// to get the leader ip from zookeeper server 
	bool GetServer(int rangenum);	// to get all of the server ip that exist in the zookeeper server

	char* election_base_dir(void) {
		return (char *)election_base_dir_;
	}
	zhandle_t * zookeeper_handle(void) {
		return zk_;
	}
	class Server* serverinfo(U32 index) {
		return svec_[index];
	}

	void erase(U32 index) {
		  delete  svec_[index]; // delete the serverInfo
		  svec_.erase((svec_.begin() + index));	//delete the pointer in the vec
	}
	void clearserver() {
		std::vector<class Server*>::iterator iter;	
		for (iter = svec_.begin(); iter != svec_.end(); ) {
			if((*iter)->new_fd_ != 0)
				close((*iter)->new_fd_);		
			delete (*iter);
			iter = svec_.erase(iter);
		}
	}

	void LockEntry(void) {
		if (!pthread_mutex_lock(&mutex_)) {
			return;
		} else {
			assert(0);
		}
	}
	void UnLockEntry(void) {
		if (!pthread_mutex_unlock(&mutex_)) {
			return;
		} else {
			assert(0);
		}
	}
	int ServerNum() {
		return svec_.size();
	}

	void addtoliverange(int rangenum) {
		std::vector<int>::const_iterator iter;	
		for (iter = live_range_.begin(); iter != live_range_.end(); iter++) {
			if(rangenum== (*iter))
				return ;
		}
		live_range_.push_back(rangenum);
	}
	void erasefromliverange(int rangenum) {
		for (int i = 0 ; i< live_range_.size();i++) {
			if (live_range_[i] == rangenum) {
				live_range_.erase(live_range_.begin()+i);
				i--;
			}
		}
	}
	int get_range_count(){
		return range_count_;
	}
//	void set_range_num(int rangenum){
//		range_num_ = rangenum;
//	}
private:
	char	      election_base_dir_[50];
	//char	      epoch_base_dir_[50];
	//U32 		range_num_;
	pthread_mutex_t mutex_;   // the mutex protect the function LeaderElectstart( it is non reentrant function see point40-7 )
	struct sockaddr_in saddr;	
	int writefd;
	U32 num_;				 // the number of server(inclucde leader and followers)
	int leadersfd,followersfd[2];
	zhandle_t *zk_;
	char ip_[20];				// not use
	char myip[20];			// client's own ip ---not use

	std::vector<class Server*>  svec_;
	std::vector<int> live_range_;	// to tell us that currently, which key range server is live 
	std::map<int,int> read_port_;	// to tel us which key range is corresponded to which read port
	std::map<int,int> write_port_;	// to tel us which key range is corresponded to which write port
	int range_count_;		// get this data from /etc/paxstore.conf file the range num RANGE_NUM fileds, we need get all of the range servers from zookeeper server
	class RangeTable table; 
};
#endif
