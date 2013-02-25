/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: to read or write key value data from/to server(leader or followers)
*/

#include "client.h"

#include <time.h>		//Random number generator use the current time as seed
#include <netdb.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <iostream>

#include "tools.h"
#include "random.h"
#include "slice.h"

class Client cread;

U32 GetRandom(int max) {
	//srand(time(0));
	U32 i = rand() % max;
	return i;
}

int GetRangeNum(std::string key) {
	int rangenum = 0,pos = 0,multipler = 1;
	int len = strlen(key.c_str());

	pos = len -1;	
	while(key[pos] >= 48 && key[pos] <= 57) {
		rangenum += (key[pos] - 48) * multipler;
		multipler *= 10;
		pos--;
	}

/*	if(rangenum%cread.get_range_count()== 0)
		rangenum = 1;
	else
		rangenum = 2;
		*/
	// fix me
	//return rangenum%cread.get_range_count() +1;
	return 1;
}

void service_init(struct sockaddr_in *addr,int port,char ip[16]) {
	addr->sin_family = AF_INET;
	addr->sin_port = htons(port);
	addr->sin_addr.s_addr = inet_addr(ip);
}

int DecodeRangeNum(const char *path) {
	int rangenum = 0,pos = 0,multipler = 1;
	int len = strlen(path);
	pos = len -1;	
	while(path[pos] >= 48 && path[pos] <= 57){
		rangenum += (path[pos] - 48) * multipler;
		multipler *= 10;
		pos--;
	}
	return rangenum;
}

void wfun(zhandle_t *zk, int type, int state, const char *path, void *watcherCtx) {
	sleep(4);
	LOG(HCK,("DIR:%s.\n",path));
	int rangenum = DecodeRangeNum(path);
	U32 index;
	class Client * cl_watcher = (class Client *) watcherCtx;
	String_vector   childrens  = {0,NULL};
	PrintZKCallBackErrors(zk, type, state, path, watcherCtx);
	cl_watcher->LockEntry();
	LOG(HCK,("-->\n"));
	// fix me ,only delete the rangenum server
	// delete the former servers and get new nodes and start new servers 
	for(index = 0;index < cl_watcher->ServerNum(); index++) {
			if(cl_watcher->serverinfo(index)->rangenum_ == rangenum) {
				LOG(HCK,("delete server:%s. rangenum:%d\n",cl_watcher->serverinfo(index)->ip_,cl_watcher->serverinfo(index)->rangenum_));
				if(cl_watcher->serverinfo(index)->new_fd_ != 0)
					close(cl_watcher->serverinfo(index)->new_fd_);				
				cl_watcher->erase(index);				// delete the former servers
				index--;			
			}
	}

	cl_watcher->erasefromliverange(rangenum);
	// fix me, get the corresponding range server.
	if (cl_watcher->GetServer(rangenum))		// get the new servers
		cl_watcher->addtoliverange(rangenum);
	cl_watcher->UnLockEntry();
}

bool IfIpExistInserver(char *ip,int rangenum) {
	std::vector<class Server*>::const_iterator iter;	
	for (iter = cread.svec_.begin(); iter != cread.svec_.end(); iter++) {
		if(0 == strcmp(ip,(*iter)->ip_) && (*iter)->rangenum_ == rangenum)
			return true;
	}
	return false;
}

bool Exist(std::vector<std::string> ip_vec, char *ip){
	int i;
	for(i = 0;i < ip_vec.size(); i++) {
		if(0 == strcmp(ip,ip_vec[i].c_str()))
			return true;
	}
	return false;
}

/*
*encode read response format :  "total_len:msg_type:value_len:value:msg_type:ip_num:ip_len:ip:ip_len:ip..."
*							total_len has been read ,so here we don't decode total_len
*/
bool DecodeValue(char *str,std::string &value,U32 total_len,int rangenum) {
	U32 value_len,msg_type,distance = 0,ip_num,i,iplen;
	char ip[20];
	std::vector<std::string> ip_vec;

	memcpy(&msg_type,str,sizeof(msg_type));		// msg_type
	msg_type = ntohl(msg_type);
	distance += sizeof(U32);
	
	memcpy(&value_len, str+distance, sizeof(value_len));	// value_len
	value_len = ntohl(value_len);
	distance += sizeof(U32);
	value = (char *)(str + distance);				//value
	distance += value_len;

	if( total_len > distance) {
		// decode follower ip address
		LOG(HCK,("new follower come.\n"));
		memcpy(&msg_type,str+distance,sizeof(msg_type));		// msg_type
		msg_type = ntohl(msg_type);
		distance += sizeof(U32);
		if (msg_type == SENDFOLLOWERIP_MESSAGE) {
			memcpy(&ip_num,str+distance,sizeof(ip_num));
			ip_num = ntohl(ip_num);
			distance += sizeof(U32);
			
			for (i = 0;i < ip_num; i++) {
				memcpy(&iplen,str+distance,sizeof(iplen));		// iplen
				iplen = ntohl(iplen);
				distance += sizeof(U32);
				memcpy(ip,str+distance,iplen);		// ip
				distance += iplen;	
				std::string strip = ip;
				
				ip_vec.push_back(strip);
				
				if (!IfIpExistInserver(ip,rangenum)) {		// add the new come follower
					class Server *s;
					if ( !(s = new class Server()) ) {
						LOG(HCK,("malloc server_info error\n"));
						assert(0);
					 }
					s->Init(0, ip,rangenum);
					cread.svec_.push_back(s);
					LOG(HCK,("new follower ip:%s.\n",ip));
				}			
			}
			
			// delete the old followers
			// if the old followers exist in svec_,however,the coming key range followers don't have it,then ,we should delete it
			std::vector<class Server*>::iterator iter;	
			for (iter = cread.svec_.begin(); iter != cread.svec_.end(); iter++) {
				if (!Exist(ip_vec, (*iter)->ip_) && (*iter)->ifisleader_==false && (*iter)->rangenum_ == rangenum) {
					LOG(HCK,("delete server:%s.\n",(*iter)->ip_));
					if ((*iter)->new_fd_!=0)
						close((*iter)->new_fd_);
					delete (*iter);
					cread.svec_.erase(iter);
					iter--;
				}
			}
		}
	}
	return true;
}

Client::Client() {	
}

bool Client::Start() {
	writefd = 0;
	int i = 0,tmpport =0;
	//range_num_ = RANGE_NUM;
	if ( pthread_mutex_init(&mutex_, NULL)  != 0){
		assert(0);
	}	
	if(!ConnectZoo()) {				// connect to the zookeeper server
		LOG(HCK,("connect zookeeper server fail\n"));
		zookeeper_close(zk_);
		return false;
	}

	if (table.init()) {					// init the rangetable from /etc/paxstore.conf
		LOG(HCK, ("table init ok\n"));
	} else {
		LOG(HCK,("table init error\n"));
		assert(0);
	}
	range_count_ = table.get_range_num();		// init the range count from /etc/paxstore.conf
	// init the read port ; init from /etc/paxstore.conf	
	for (i=1; i <= range_count_; i++) {
		tmpport = table.GetRangeItem(i).leader_write_port_;
		write_port_.insert(std::pair<int,int>(i,tmpport));
		tmpport = table.GetRangeItem(i).read_port_;
		read_port_.insert(std::pair<int,int>(i,tmpport));
	}

	live_range_.clear();
	for(i = 1; i <= range_count_; i++) {		// get all of the Range servers
		if(GetServer(i))
			addtoliverange(i);
			//live_range_.push_back(i);		// it indicates that the i range server is live.
	}
	return true;
}

bool Client::ReadMessage(int index) {
	char rbuf[MAX_TCP_MSG_SIZE];
	memset(rbuf,0x00,sizeof(rbuf));
	int errcode = 0;
	std::string value;
	ssize_t nsize = ReadvRec(svec_[index]->new_fd_, rbuf, MAX_TCP_MSG_SIZE, errcode);
	if ( errcode == EMSGSIZE ) {
		LOG(ERR,("message bigger than buffer\n"));
		return false;
	}	
	if (nsize == 0) {		
		// fix me delete this server from svec
		LOG(HCK,("delete server:%s.\n",svec_[index]->ip_));
		LOG(ERR,("server elegantly disconnect\n"));
		if (svec_[index]->new_fd_ != 0)
			close(svec_[index]->new_fd_);
		delete svec_[index];
		svec_.erase(svec_.begin()+index);
		return false;	
	} else if (nsize < 0) {
		// fix me delete this server from svec
		LOG(ERR,("server failed disconnected\n"));
		LOG(HCK,("delete server:%s.\n",svec_[index]->ip_));
		if (svec_[index]->new_fd_ != 0)
			close(svec_[index]->new_fd_);
		delete svec_[index];
		svec_.erase(svec_.begin()+index);
		return false;
	} else {	
		if (DecodeValue(rbuf,value,nsize,svec_[index]->rangenum_)) {
			if (value.length() != 0) {
				LOG(VRB,("value:%s\n",value.c_str()));
			}
			else {
				LOG(HCK,("doesn't find the record\n"));
			}
		} else {
			LOG(HCK,("Decode value error\n"));
		}
	}
	return true;
}

bool Client::ReadRecord( std::string key,bool consistent) {
	int n,rangenum = 0;
	int errcode = 0;
	U32 tmp,key_len,total_len;
	char buf[PACKAGESIZE];
	memset(buf,0x00,sizeof(buf));
	//LOG(HCK,("memset buf[]\n"));
	snprintf(buf +  2 * sizeof(U32), sizeof(buf), "%s",key.c_str());
	// here, we must use string.c_str() other than string , if we use string directly then illigal instruction error will happen.
	key_len = strlen(buf + 2 *sizeof(U32)) + 1;
	tmp = htonl(key_len);
	memcpy (buf+sizeof(U32), &tmp, sizeof(U32));
	total_len = key_len + sizeof(U32);
	tmp = htonl(total_len);
	memcpy(buf,&tmp,sizeof(U32));
	bool flag = false;

	rangenum = GetRangeNum(key);

	U32 index;
REDO1:	
	if (consistent) {
		for (U32 i = 0;i < svec_.size(); i++) {
			if (svec_[i]->ifisleader_ && svec_[i]->rangenum_ == rangenum)
				index = i;
		}
	} else {		
		index = GetRandom(svec_.size());
		while(svec_[index]->rangenum_!=rangenum) {		// get the key range servers
			index = GetRandom(svec_.size());
		}
	}
	//LOG(HCK,("server ip : %s.\n",svec_[index]->ip_));
	switch ( ReadConnect(index,rangenum)) {
		case DELETING:					//zookeeper client set it
				LOG(HCK,("delete server:%s.\n",svec_[index]->ip_));
				if (svec_[index]->new_fd_ != 0)
					close(svec_[index]->new_fd_);				
				delete svec_[index];
				svec_.erase(svec_.begin()+index);
				LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
				goto REDO1;
				break;
		case FOLLOWER_CLOSE:
				LOG(HCK,("delete server:%s.\n",svec_[index]->ip_));
				if(svec_[index]->new_fd_ != 0)
					close(svec_[index]->new_fd_);
				delete svec_[index];				
				svec_.erase(svec_.begin()+index);
				LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
				goto REDO1;
				break;
		case CONNECT_FAIL:
				LOG(HCK,("wait for next connect\n"));
				// fix me ,if client doesn't connect to this server,it will destroy this server message,and connect to another server
				LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
				goto REDO1;
				break;
		case CONNECT_OK:
				if (TcpSend(svec_[index]->new_fd_, buf, sizeof(total_len) + total_len)) {
					LOG(HCK,("%s  send ok.\n",key.c_str()));
					flag = ReadMessage(index);
					if (!flag) {
						LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
						goto REDO1;		// try to read this data again
					}
				}
				else {
						LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
						goto REDO1;       // try to read this data again
				}
				break;
		case NEW_CONNECT:
				if (TcpSend(svec_[index]->new_fd_, buf, sizeof(total_len) + total_len)) {
					LOG(HCK,("%s  send ok.\n",key.c_str()));
					flag = ReadMessage(index);
					if (!flag) {
						LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
						goto REDO1;		// try to read this data again
					}
				}
				else {
						LOG(HCK,("read %s error,try to read this data again.\n",key.c_str()));
						goto REDO1;		// try to read this data again
				}
				break;
		default:
				LOG(HCK,("ReadConnect fault\n"));
				break;
	}
	return flag;	
}

bool Client::WriteRecord(std::string key,std::string value) {
	char buf[PACKAGESIZE];
	U32 total_len;
	U32 key_len;
	U32 value_len;
	U32 tmp;
	int rangenum;
	memset(buf,0x00,sizeof(buf));			
	snprintf(buf +  3 * sizeof(U32), sizeof(buf), "%s",key.c_str());
	key_len = key.size() + 1;
	LOG(VRB,( "key_len:%d:",key_len));
	tmp = htonl(key_len);
	memcpy (buf + sizeof(U32), &tmp, sizeof(U32));
	snprintf(buf +  3 * sizeof(U32) + key_len, sizeof(buf), "%s",value.c_str());
	value_len = value.size() + 1;
	LOG(VRB,( "value_len:%d:",value_len));
	tmp = htonl(value_len);
	memcpy (buf + 2 * sizeof(U32), &tmp, sizeof(U32));
	total_len = 2 * sizeof(U32) + key_len + value_len;
	tmp = htonl(total_len);
	memcpy (buf, &tmp, sizeof(U32));
	rangenum = GetRangeNum(key);
	if (!WriteConnect(rangenum)) {
		LOG(HCK,("doesn't connect to leader.\n"));
		assert(0);
		return false;
	}

	std::vector<class Server*>::iterator iter;	
	for (iter = svec_.begin(); iter != svec_.end(); iter++) {
		if((*iter)->ifisleader_ == true && (*iter)->rangenum_ == rangenum)		// find the leader
			break;
	}
	if (iter == svec_.end()) {
		//LOG(HCK,("Leader doesn't exist.\n"));
		assert(0);		
	}
	writefd = (*iter)->writefd;			// send the data to the keyrange leader.
	if (send(writefd,buf,sizeof(total_len) + total_len,0) > 0 ) {		
		LOG(HCK,("%s send ok.\n",key.c_str()));		
	} else {
		LOG(HCK,("send error\n"));
		return false;
	}
	unsigned char ret;
	int n;
	n = recv(writefd, (void *)&ret,sizeof(ret),0);
	if (n > 0 ) {
		//printf("state:%c\n",ret);
	} else if (n == 0) {
		LOG(HCK,("connection closed\n"));
		return false;
	} else {
		LOG(HCK,("errno:%d.\n",errno));
		LOG(HCK,("recv error\n"));
		//assert(0);
		return false;
	}
	return true;
}

bool Client::ConnectZoo() {
	zk_ = zookeeper_init("192.168.3.21:2180,192.168.3.21:2181,192.168.3.21:2182",
					global_watcher, RECV_CLIENT_TIME, 0, NULL,0);	
	sleep(2);
	//fixeme  the zookeeper_init is an async call, when it returns , it not represent ok
	if (zk_ == NULL) {
		LOG(HCK,("zookeeper init error\n"));
		return false;
	}
	return true;
}

bool Client::ZooGetChildren(const char* dir, watcher_fn wfunc,struct String_vector &childrens) {
	int rc;
	LOG(HCK,("-->\n"));
	memset(&childrens, 0, sizeof(childrens));
	rc = zoo_wget_children(zk_,dir, wfunc,this,  &childrens);  
	if (rc != (int)ZOK) {
		LOG(HCK,("the base is create error or disconnect? wget error\n"));
		PrintZooKeeperError(rc);
		assert(0);
		return false;
	}
	LOG(HCK,("<--\n"));
	return true;
}

bool Client::WriteConnect(int rangenum) {
	std::vector<class Server*>::iterator iter;	
	for (iter = svec_.begin(); iter != svec_.end(); iter++){
		if((*iter)->ifisleader_ == true && (*iter)->rangenum_ == rangenum)		// find the leader
			break;
	}
	if (iter == svec_.end()) {
		LOG(HCK,("Leader doesn't exist.\n"));
		assert(0);
		return false;
	}
	int sock;
	int flags; 
	int writeport;
	struct sockaddr_in follower_addr;  
	if  (  (*iter)->ifwriteconnect == false ) { 
		if (( sock  = socket(AF_INET,SOCK_STREAM , 0)) < 0){
			return false;
		}
		 int send_buf_size =  MAX_TCP_MSG_SIZE; 
		if ( (flags = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (void *)&send_buf_size, 
						sizeof(send_buf_size) ) ) < 0 ) {
			LOG(HCK,("setsockopt send buf size error\n"));
			close(sock);
			return false;
		}		
		std::map<int,int>::iterator map_it;
		map_it = write_port_.find(rangenum);
		if(map_it == write_port_.end()) {
			LOG(HCK,("ERROR RANGENUM.\n"));
			assert(0);
			return false;
		}
		writeport = map_it->second;

		follower_addr.sin_family = AF_INET;
		follower_addr.sin_port = htons(writeport);
		follower_addr.sin_addr.s_addr = inet_addr( (*iter)->ip_);
		if ( ConnectRetry(sock, (struct sockaddr*)&follower_addr, sizeof(follower_addr)) == -1) {
			close(sock);
			LOG( VRB,("CONNECT_FAIL\n"));
			LOG( VRB,("<--\n"));
			return false;
		}
		// update some info
		 (*iter)->setwriteconnect();	
		(*iter)->writefd = sock;
		LOG( VRB,("NEW_CONNECT\n"));
		LOG( VRB,("<--\n"));
		return true;
	}
	else {
		return  true;  //just let out of warning 
	}
}

short Client::ReadConnect(U32 index,int rangenum) {
	int sock;
	int flags; 
	int readport;
	struct sockaddr_in follower_addr;  
	LOG( VRB,("-->\n"));
	if  ( svec_[index]->state_ == NEWITEM ) {
		if (( sock  = socket(AF_INET,SOCK_STREAM , 0)) < 0) {
			return CONNECT_FAIL;
		}
		//set   tcp   async;
		if ( ( flags = fcntl(sock, F_GETFL, 0)) < 0 ||
			fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0 ){
			LOG(HCK,("Error:: server listensock set O_NONBLOCK error\n"));
			close(sock);
			return CONNECT_FAIL;
		}
		 int send_buf_size =  MAX_TCP_MSG_SIZE; 
		if( (flags = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (void *)&send_buf_size, 
						sizeof(send_buf_size) ) ) < 0 ) {
			LOG(HCK,("setsockopt send buf size error\n"));
			close(sock);
			return CONNECT_FAIL;
		}				
		std::map<int,int>::iterator map_it;
		map_it = read_port_.find(rangenum);
		if(map_it == write_port_.end()) {
			LOG(HCK,("ERROR RANGENUM.\n"));
			assert(0);
			return false;
		}
		readport = map_it->second;

		follower_addr.sin_family = AF_INET;
		follower_addr.sin_port = htons(readport);
		follower_addr.sin_addr.s_addr = inet_addr(svec_[index]->ip_);
		LOG( VRB,("before ConnectRetry\n"));
		if ( ConnectRetry(sock, (struct sockaddr*)&follower_addr, sizeof(follower_addr)) == -1) {
			close(sock);
			svec_[index]->connecttime++;
			LOG( VRB,("CONNECT_FAIL\n"));
			LOG( VRB,("<--\n"));
			if(svec_[index]->connecttime<MAXTRYCONNECTTIME)
				return CONNECT_FAIL;
			else
				return DELETING;
		}
		svec_[index]->connecttime = 0;
		// update some info
		svec_[index]->new_fd_ = sock;
		svec_[index]->new_follower_addr_ = follower_addr;
		svec_[index]->state_ = CONNECTED;
		LOG( VRB,("NEW_CONNECT\n"));
		LOG( VRB,("<--\n"));
		return NEW_CONNECT;
	}else if (svec_[index]->state_ == CLOSED) {
		// return info for proposal to delete event
		LOG( VRB,("FOLLOWER_CLOSE\n"));
		LOG( VRB,("<--\n"));
		return FOLLOWER_CLOSE;	
	} else if (svec_[index]->state_ == CONNECTED) {
		LOG( VRB,("CONNECT_OK\n"));
		LOG( VRB,("<--\n"));
		return CONNECT_OK; 
	} else if (svec_[index]->state_ == ZOOKEEPER_DELETED) {
		//the zookeeper deconnect with the follower,so set the deleted flag
		LOG( VRB,("DELETING\n"));
		LOG( VRB,("<--\n"));
		return DELETING;
	} else {
		LOG( VRB,("follower state unkown\n"));
		LOG( VRB,("<--\n"));
		assert(0);
		return  DELETING;  //just let out of warning 
	}
}

bool Client::GetLeader(int rangenum,char *str) {
	snprintf(election_base_dir_, sizeof(election_base_dir_), "%s/KeyRange%u", 
				ROOT_ELECTION_DIR, rangenum);
	LOG(HCK,("election_base_dir_:%s\n",election_base_dir_));
	int rc;
	char cbuf[100];
	struct Stat stat1;
	LeaderValue   leader_value;
	int len = sizeof(leader_value);
	snprintf(cbuf, sizeof(cbuf),"%s/%s",election_base_dir_, LEADER_PREFIX);   //fixme , the range num	
	rc = zoo_get(zk_, cbuf, 0, (char *)&leader_value, &len, &stat1);
	if ( (int)ZOK != rc ) {
		LOG(HCK,(" :zoo_get error\n"));
		PrintZooKeeperError(rc);
		return false;   //is the node is not exist,(may be happend,also,return false, then the  IfLeaderExist will also return false, it is also right);
	}
	LOG(HCK, ("leader_id:%u   leader_ip:%s\n",leader_value.leader_id, leader_value.leader_ip));
	strcpy(str, leader_value.leader_ip);
	return true;
}

bool Client::GetServer(int rangenum) {
	//snprintf(epoch_base_dir_, sizeof(epoch_base_dir_), "%s/KeyRange%u", 
	//			EPOCH_FOR_RANGE_BASE, rangenum);
	
	//LOG(HCK,("epoch_base_dir_:%s\n",epoch_base_dir_));
	char leaderip[20];
	if (!GetLeader(rangenum,leaderip)) {
		// fix me
		// how can we do when the leader doesn't exit
		LOG(HCK,("the %d Leader doesn't exist.\n",rangenum));
		return false;
		//assert(0);
	}		
	snprintf(election_base_dir_, sizeof(election_base_dir_), "%s/KeyRange%u", 
				ROOT_ELECTION_DIR, rangenum);
	LOG(HCK,("election_base_dir_:%s\n",election_base_dir_));

	struct String_vector childrens;	
	if (!ZooGetChildren(election_base_dir_, wfun, childrens)) {
		return false;
	}	
	// we can get all of the node ip through children ,
	U32 child_num=childrens.count;
	char ip[20];
	int len = sizeof(ip);
	int rc;
	struct Stat stat2;
	U32 j=0,k=0;
	char path[100];
	for (U32 i = 0; i != childrens.count; i++) {
		if (childrens.data[i]&&strcmp(childrens.data[i],"Leader") != 0) {
			snprintf(path, sizeof(path),"%s/%s",election_base_dir_, childrens.data[i]); 
			rc =  zoo_get(zk_, path, 0, ip, &len, &stat2);	
			if ( (int)ZOK != rc ) {
				LOG(HCK,(" :zoo_get error\n"));
				PrintZooKeeperError(rc);
				assert(0);
				return false;  
			}
			class Server *s;
			if ( !(s = new class Server()) ) {
				LOG(HCK,("malloc server_info error\n"));
				assert(0);
			 }
			s->Init(0, ip,rangenum);
			LOG(HCK,("server ip:%s.\tserver range:%d.\n",ip,rangenum));
			svec_.push_back(s);
			if (strcmp(leaderip,ip) == 0)
				s->setleader();
			memset(ip,0x00,sizeof(ip));
		}
		else
			continue;
	}
	return true;
}

int main(int argc, char* args[]) {
/*	if (argc != 3){
		LOG(HCK,("arg  num is error: %d %s\n", argc, args[0]));			
		return 0;
	}

	char* port = args[1];
	int portnum = atoi(port);
	DEFAULT_PROT_NUM = portnum;
	char *range = args[2];
	int rangenum = atoi(range);
	//cread.set_range_num(rangenum);
	*/
	DisableSigPipe();
	RandomGenerator gen;
	Stats stats;
	Slice slice;
	
      double start_=0,end_=0,time_=0,speed;
	if(!cread.Start())
		assert(0);
	std::string key,value;
	int choice,num,m = 0,addcount,valuelength;
	LOG(HCK,("please input if u wan't to read or write (1:read;2 write):"));	
	scanf("%d",&choice);
/*	while((cout<<"please input the record you wan't to read:")&&(cin>>key)){
		cread.ReadRecord(key, false);
	}	*/
	char buf[20];
	if (choice == 1) {
		while((printf("enter read number:\n")) && (scanf("%d",&num))) {
			if (0 == num) {
				break;
			}
			stats.cls();
			stats.Setdone(num);
			stats.Start();	
			for(int i = 1;i <= num; i++) {
				int p = rand() % num +1;	
                                snprintf(buf,sizeof(buf),"key%d",p);
				key.assign(buf);
				//std::cin>>key;
				if (!cread.ReadRecord(key, true)) {
					// fix me try to read this data again
					LOG(HCK,("read data error.\n"));
				}
			}
			stats.Stop();
			stats.Report();
		}
	}
	else if (choice == 2) {
		while ((printf("enter proposal number\n")) && (scanf("%d",&num)) && (printf("enter value length:")) && (scanf("%d",&valuelength))) {
			if (0 == num) {
				break;
			}
			stats.cls();
			stats.Setdone(num);
			stats.Start();	
			while (num--) {
				m++;
				snprintf(buf,sizeof(buf),"key%d",m);	
				key.assign(buf);
				slice = gen.Generate(valuelength);
				value.append(slice.data(),valuelength);
				while(!cread.WriteRecord(key, value)){
					sleep(1);
				}
				stats.AddBytes(valuelength);
				value = "";
			}
			stats.Stop();
			stats.Report();
		}
	}
	return 0;
}
