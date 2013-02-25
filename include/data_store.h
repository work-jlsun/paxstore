/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: the implementation of how we operate storage engine(in this project we use modified version of leveldb)
*/

#ifndef PAXSTORE_DATA_STORE_
#define PAXSTORE_DATA_STORE_
#include "leveldb/db.h"

#include "kvproposal.h"

extern class StoreEngineManager datastore;

class StoreEngineManager {
public:
	StoreEngineManager();
	~StoreEngineManager();

	bool Open();
	U64 LocalRecovery();
	bool StoreInit();

	bool WriteData(std::string key,std::string value, U64 seq);
	bool ReadData(std::string key,std::string & value);

	void set_store_log_do_recovery(int data);
private:
	int store_log_do_recovery_;	// when this node start , this flag tell us if this node need local recovery
	leveldb::Options options;

	leveldb::DB* db_;			 // first we should open db_ and then use db_ to read or write data
	std::string db_name_;		// the database name
	std::string imm_name_;	// this file will record the first and last record sequence in the imm memtable
	std::string mem_name_;	// this file will record the first and last record sequence in the memtable
};
#endif
