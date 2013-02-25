/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: manager the meta log message(include cmt,epoch,rangenum)
*/

#include "metalogstore.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <assert.h>

#include "pqueue.h"
#include "cfg.h"

class MetaLogStore mlstore;

bool MetaLogStore::MetaStoreInit(void) {
	char command[600];
	if (meta_log_do_recovery_ == 0) {
		remove(cmt_filename_);
			sprintf(command, "touch  %s", cmt_filename_);
			if ( system(command) != 0) {
				assert(0);
			}
	}
	return true;
}

void MetaLogStore::StoreMetaInfo(U32 rangenum, class P12Info & pinfo) {
	std::ofstream outfile(cmt_filename_,std::ios::out);
	if (outfile << rangenum << " "<<pinfo.epoch() << " " << pinfo.get_commited_sequence_unlocked())
		outfile.close();
	else
		assert(0);
}

void MetaLogStore::StoreMetaInfo(U32 rangenum, ProposalNum & pn) {
	std::ofstream outfile(cmt_filename_,std::ios::out);
	if (outfile << rangenum << " " << pn.epoch << " "<< pn.sequence)
		outfile.close();
	else
		assert(0);
}
int MetaLogStore::GetCommitPn(U32 rangenum, ProposalNum & pn) {
	U64 epoch,sequence;
	std::ifstream infile(cmt_filename_,std::ios::in);
	if (infile >> rangenum >> epoch >> sequence) {
		infile.close();
		pn.epoch = epoch;
		pn.sequence = sequence;
		return GETOK;
	}
	else
		return GETEMPTY;
}

int MetaLogStore::get_meta_log_do_recovery() {
	return meta_log_do_recovery_;
}

void MetaLogStore::set_meta_log_do_recovery(int flag) {
	meta_log_do_recovery_ = flag;
}
