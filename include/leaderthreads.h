/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#ifndef __LEADERTHREADS__
#define __LEADERTHREADS__

void* tcpserver_thread(void *);
void* proposer_thread(void *);
void* logwriter_thread(void *arg);
void* leadercatchup_thread(void *arg);
void* commitsync_thread(void *arg);
void* read_thread(void *arg);
void leadercatchup_cleanup_handler(void *p);
#endif
