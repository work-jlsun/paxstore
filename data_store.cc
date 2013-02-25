/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: the implementation of how we operate storage engine(in this project we use modified version of leveldb)
*/

#include "data_store.h"

#include <assert.h>
#include <sys/stat.h>
#include <stdlib.h>

#include <fstream>

class StoreEngineManager datastore;

StoreEngineManager::StoreEngineManager()
	:store_log_do_recovery_(0) {
	db_name_ = (std::string)DBNAME;
	options.create_if_missing = true;
	options.write_buffer_size = (int)LEVELDB_MEMTABLE_SIZE;
	imm_name_ = (std::string)DBNAME + "/imm";
	mem_name_ = (std::string)DBNAME + "/mem";
}

StoreEngineManager::~StoreEngineManager() {
	delete db_;
}

bool StoreEngineManager::Open() {
	leveldb::Status status = leveldb::DB::Open(options, db_name_, &db_);
	assert(status.ok());
	return true;
}

// when this node start ,we should do local recovery to recovery the lost records in the memtable or imm memtable
// so we should get the begin sequence through imm_name_ file and mem_name_ file  and get the end sequence from cmt file.
U64 StoreEngineManager::LocalRecovery() {
	U64 mem_begin,imm_begin,imm_end;
	bool mem_exit = true;
	bool imm_exit = true;
	U64 recover_begin;
	
	if (store_log_do_recovery_==1) {
		std::ifstream memfile(mem_name_.c_str(),std::ios::in);
		if (!(memfile>>mem_begin)) {
			mem_begin = 0;
			mem_exit = false;
		}
		memfile.close();

		std::ifstream immfile(imm_name_.c_str(),std::ios::in);
		if (!(immfile>>imm_begin>>imm_end)) {
			imm_begin = 0;
			imm_end = 0;
			imm_exit = false;
		}		
		immfile.close();

		if (imm_exit) {
			return imm_begin;
		}
		else if (mem_exit) {
			return mem_begin;
		}
		else {
			LOG(HCK,("no record need to recover\n"));
			return 0;
		}
	}
	else {
		LOG(HCK,("what's wrong,store_log_do_recovery==0\n"));
		assert(0);
	}
}

bool StoreEngineManager::StoreInit() {
	char command[600];
	if (store_log_do_recovery_==0) {
		sprintf(command, "rm -r  %s", db_name_.c_str());
		if ( system(command) != 0) {
			LOG(HCK,("no data.\n"));
		}
	}
	return true;
}

bool StoreEngineManager::WriteData(std::string key,std::string value, U64 seq) {
	leveldb::Status s ;
	s = db_->Put(leveldb::WriteOptions(), key, value,seq);
	if(s.ok())
		return true;
	else {
		LOG(HCK,("insert to leveldb data error\n"));
		return false;
	}
}

bool StoreEngineManager::ReadData(std::string key,std::string & value) {
	leveldb::Status s ;
	s = db_->Get(leveldb::ReadOptions(), key, &value);
	if(s.IsNotFound())
		return false;
	else
		return true;
}

void StoreEngineManager::set_store_log_do_recovery(int data) {
	store_log_do_recovery_ = data;
}
