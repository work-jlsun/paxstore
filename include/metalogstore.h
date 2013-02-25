/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: manager the meta log message(include cmt,epoch,rangenum)
*/

#ifndef PAXSTORE_METALOGSTORE_
#define PAXSTORE_METALOGSTORE_
#include <fstream>

#include "kvproposal.h"

class P12Info;

extern class MetaLogStore mlstore;

class MetaLogStore{
public:
	MetaLogStore():meta_log_do_recovery_(0){
	}
	virtual ~MetaLogStore(){
	}
	bool MetaStoreInit(void);
	void StoreMetaInfo(U32 rangenum,  ProposalNum& pn);
	void StoreMetaInfo(U32 rangenum, class P12Info & pinfo);
	int  GetCommitPn(U32 rangenum , ProposalNum &pn);	

	int get_meta_log_do_recovery();
	void set_meta_log_do_recovery(int flag);
private:
	int meta_log_do_recovery_;
};
#endif
