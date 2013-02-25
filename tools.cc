/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function: all kinds of tool functions 
*/

#include "tools.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <assert.h>
#include <iostream>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include "cfg.h"

U64 NowMicro(){
        struct timeval tv;
        gettimeofday(&tv,NULL);
        return static_cast<U64>(tv.tv_sec)*1000000 + tv.tv_usec;
}

SocketPair::SocketPair():is_cmt_synced_(0),triggered_(false){
	int listener = -1;
	int connector = -1;		//to write
	int acceptor = -1;			// to read ,or to wait on ths
	
	struct sockaddr_in listen_addr;
	struct sockaddr_in connect_addr;
	socklen_t size = sizeof(connect_addr);
	
	listener = socket(AF_INET,SOCK_STREAM , 0);
	if (listener < 0 ){
		assert(0);
	}
	memset(&listen_addr, 0, sizeof(listen_addr));
	listen_addr.sin_family = AF_INET;
	listen_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	listen_addr.sin_port = 0;		//let the kernel to choose a thread
	if (bind(listener, (struct sockaddr *) &listen_addr, sizeof (listen_addr))
		== -1){
		goto fail;
		
	}
	if (listen(listener, 1) == -1){
		goto fail;
	}

	//get the connector socket fd
	connector = socket(AF_INET, SOCK_STREAM, 0);
	if (connector < 0 ){
		goto fail;
	}
if (getsockname(listener, (struct sockaddr *) &connect_addr, &size) == -1){
		goto fail2;
	}
	if (size != sizeof (connect_addr)){
		goto fail2;
	}
	if (connect(connector, (struct sockaddr *) &connect_addr,
						sizeof(connect_addr)) == -1){
		goto fail2;
	}

	//get the accptor socket fd
	size = sizeof(listen_addr);
	acceptor = accept(listener, (struct sockaddr *) &listen_addr, &size);
	if (acceptor < 0){
		goto fail2;
	}
	if (size != sizeof(listen_addr)){
		goto fail2;
	}
	//now the listener fd is no longer to use
	close(listener);
	//now the last check
	if (getsockname(connector, (struct sockaddr *) &connect_addr, &size) == -1){
		goto fail2;
	}
	if (size != sizeof (connect_addr)
		|| listen_addr.sin_family != connect_addr.sin_family
		|| listen_addr.sin_addr.s_addr != connect_addr.sin_addr.s_addr
		|| listen_addr.sin_port != connect_addr.sin_port){
		goto fail2;
	}
	fd_[0] = connector;
	fd_[1] = acceptor;
	return;	
fail2:
	if( connector  !=  -1){
		close(connector);
	}	
fail:
	if( listener  !=  -1){
		close(listener);
	}
	assert(0);
}

SocketPair::~SocketPair(){
	int i = 2;
	while(i--){	
		if (fd_[i] > 0){
			close(fd_[i]);
		}
	}
}

bool ListenerInit( int &listenfd, int port){
		listenfd = socket(AF_INET, SOCK_STREAM, 0);
		if ( listenfd < 0 ){
			return false;
		}
		int flags; 
		if ( ( flags = fcntl(listenfd, F_GETFL, 0)) < 0 ||
			fcntl(listenfd, F_SETFL, flags | O_NONBLOCK) < 0 ){
			LOG(HCK,( "Error:: server listensock set O_NONBLOCK error\n" ));
			return false;
		}
		struct sockaddr_in listen_addr;
		bzero((void *)&listen_addr, sizeof(listen_addr));
		listen_addr.sin_family = AF_INET;
		listen_addr.sin_addr.s_addr = INADDR_ANY;
		listen_addr.sin_port = htons(port);

		// Set to reuse address   
    		int activate = 1;
    		if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &activate, sizeof(int)) != 0) {
			LOG(HCK,("setsockopt, setting SO_REUSEADDR error\n"));
			return false;
    		}
    		
		int bindret;
		if ( (bindret = bind(listenfd, (struct sockaddr*)&listen_addr, sizeof(listen_addr)) < 0)){
			LOG(HCK,("Error: server bind error. errorcode:%d\n",errno));
			return false;
		}
		//
		
		if ( listen(listenfd, BACKLOG) < 0){
			LOG(HCK,( "Error: server listen error\n" ));
			return false;
		}
		return true;
	
}


int ConnectRetry(int sockfd,  struct sockaddr *addr, socklen_t alen) {
	int nsec;
	LOG( VRB,("-->\n"));
	for (nsec = 1; nsec <= MAXSLEEP; nsec <<= 1){
		if (connect(sockfd, addr, alen) == 0){
			LOG( VRB,("ConnectRetry ok\n"));
			LOG( VRB,("<--\n"));
			return 0;
		}
		if (nsec <= MAXSLEEP/2){
			sleep(nsec);
			LOG( HCK,("sleep %d.\n",nsec));
		}
	}
	LOG( HCK,("ConnectRetry fail\n"));
	LOG( VRB,("<--\n"));
	return -1;
}

/*
*Readn
*
*/
ssize_t Readn(int sfd, char *buf, ssize_t len ){
	ssize_t nread;
	ssize_t nleft = len;
	while (nleft > 0){		
		nread = recv(sfd, buf, nleft, 0);
		if (nread < 0){
			if ( errno == EINTR ){
				continue;
			}
			
			if( errno == EAGAIN ){		//what happed?
				continue;
			}
			
			LOG(HCK,("recv < 0\n"));
			
			return -1;
			//disconnected?
		}else if(0 == nread){				//here is differant
			LOG(HCK,("recv = 0\n"));
			return 0;
			//disconnected?
		}else{
			nleft -= nread;
			buf += nread;	
		}
	}
	return len -nleft;	
}

/*
*ReadvRec()
*
*
*/
ssize_t ReadvRec(int sfd, char *buf, U32 len, int &errorcode){
	U32 reclen;
	ssize_t rec;
	assert(len > 0);
	rec = Readn(sfd, (char*)&reclen,sizeof(U32) );
	if (rec != sizeof(U32)){
		LOG(HCK, ("header != sizeof(U32)?\n"));
		return rec < 0?-1:0;	
	}
	reclen = ntohl(reclen); 
	if (reclen > len ){  //bigger than the rev buffer
		LOG(HCK, ("message len bigger than buffer\n"));
		while (reclen > 0){	//readout  all of the message frome tha stack			
			rec = Readn(sfd, buf, len);
			if (rec != len){
				return rec < 0 ? -1:0;
			}else {
				reclen -= len;
				if (reclen < len){
					len = reclen;	
				}	
				if (reclen == 0){
					break;
				}
			}
		}
		errorcode =  EMSGSIZE;
		return -1;
	}else{		
		//revice the record	
		rec = Readn(sfd, buf, reclen);
		if (rec != reclen){
			LOG(HCK,("recive record body error\n"));
			return rec < 0?-1:0;
		}else{
			return rec;	
		}
	}
}

U64  Htonl64(U64   host) {   
	U64   ret = 0;   
	U64   high,low;
	low   =   host & 0xFFFFFFFF;
	high   =  (host >> 32) & 0xFFFFFFFF;
	low   =   htonl(low);   
	high   =   htonl(high);   
	ret   =   low;
	ret   <<= 32;   
	ret   |=   high;   
	return   ret;   
}

U64  Ntohl64(U64   net){   

	U64   ret = 0;   
	U64   high,low;
	low   =   net & 0xFFFFFFFF;
	high   =  (net >> 32) & 0xFFFFFFFF;
	low   =   ntohl(low);   
	high   =   ntohl(high);   
	ret   =   low;
	ret   <<= 32;   
	ret   |=   high;   
	return   ret;   
}

bool TcpSend(int sfd, char* buf, U32 size){
        ssize_t tmp;
        ssize_t  len = size;
	U32 dbg = 0;	
        while (len > 0){
                 tmp = send(sfd, buf, len, MSG_DONTWAIT) ; 
                 if (tmp  < 0){                         // what error , we don't know.first let it go
                        if (errno == EINTR ){
                                LOG(HCK,("EINTR\n"));
                                continue;
                        }
                        if (errno == EAGAIN ){
                                LOG(HCK,("EAGAIN\n"));
                                continue;
                        }
			if (errno  ==  EPIPE)	{
				if(dbg == 0){
					dbg++;
				}
				LOG(HCK,("EPIPE\n"));
				return false;
			}
                        return false;
                 }else if (tmp  == 0){
			continue;
		 }
                 buf +=  tmp;
                 len -= tmp;
        }
        return true;
}

void DisableSigPipe(void){
	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	if (  -1  == sigaction( SIGPIPE, &sa, 0 ) ){
		assert(0);
	}
}

void PrintZKCallBackErrors(zhandle_t *zh, int type, int state, const char *path,void* ctx){
    if(zh==0 || ctx==0){
	LOG(HCK,("zh or ctx is NULLwhat is wrong\n"));
	return;
    }
    if (type == ZOO_SESSION_EVENT){
		LOG(HCK,("ZOO_SESSION_EVENT\n"));
			if (state ==  ZOO_EXPIRED_SESSION_STATE){
				LOG(HCK,("ZOO_EXPIRED_SESSION_STATE\n"));
			}
			else if (state ==  ZOO_CONNECTED_STATE){
				LOG(HCK,("ZOO_CONNECTED_STATE\n"));			
			}
			else if (state ==  ZOO_CONNECTING_STATE){
				LOG(HCK,("ZOO_CONNECTING_STATE\n"));			
			}			
			else if (state ==  ZOO_AUTH_FAILED_STATE){
				LOG(HCK,("ZOO_AUTH_FAILED_STATE\n"));			
			}
			else if (state ==  ZOO_ASSOCIATING_STATE){
				LOG(HCK,("ZOO_ASSOCIATING_STATE\n"));			
			}else{
				LOG(HCK,("unknown ZOO_SESSION_EVENT\n"));
			}
    }else if (type ==ZOO_CREATED_EVENT){
		LOG(HCK,("ZOO_CREATED_EVENT\n"));
    }else if (type == ZOO_DELETED_EVENT){
    		LOG(HCK,("ZOO_DELETED_EVENT\n"));
    }else if (type == ZOO_CHANGED_EVENT){
		LOG(HCK,("ZOO_CHANGED_EVENT\n"));
    }else if (type == ZOO_CHILD_EVENT){
		LOG(HCK,("ZOO_CHILD_EVENT\n"));	
    }else if (type == ZOO_NOTWATCHING_EVENT){
		LOG(HCK,("ZOO_NOTWATCHING_EVENT\n"));		
    }else{
		LOG(HCK,("This event type have not defined\n"));
    }
}

void PrintZooKeeperError(int error){
	switch(error){
		case ZNONODE:
			LOG(HCK,(": ZNONODE\n"));
			break;
		case ZBADARGUMENTS:
			LOG(HCK,(": ZBADARGUMENTS"));
			break;			
		case ZNOAUTH:
			LOG(HCK,(": ZBADARGUMENTS"));			
			break;		
		case ZINVALIDSTATE:
			LOG(HCK,(": ZINVALIDSTATE"));			
			break;
		case ZMARSHALLINGERROR :
			LOG(HCK,(": ZMARSHALLINGERROR"));			
			break;		
		default:
			LOG(HCK,(": undefined error"));			
			break;
	}
}
