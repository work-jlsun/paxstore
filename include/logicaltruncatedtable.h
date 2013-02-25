/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#ifndef __LOGICALTRUNCATEDTABLE__
#define  __LOGICALTRUNCATEDTABLE__
#include <fstream>
#include <map>

#include "cfg.h"
#include "kvproposal.h"

//static
extern int logical_truncated_table_do_recovery ;

class LogicalTruncatedTable{
public:
	LogicalTruncatedTable(){
		file_name = (std::string)LOGICAL_TRUNCATED_TABLE_DB_FILE_NAME;
		num_file_name = (std::string)LOGICAL_TRUNCATED_TABLE_NUM_FILE_NAME;

	}
	bool OpenInit(void){
		bool flag;
		if ( !(flag = LogicalTruncatedTableInit()) ){
			return false;
			//assert(0);
		}
		return true;
	}
	virtual ~LogicalTruncatedTable(){
		bool flag;
		//LOG(HCK,("-->"));	
		if ( !(flag = LogicalTruncatedTableShutdown())){
			assert(0);
		}
		//LOG(HCK,("<--"));	
	}
	
	//bool GetEpochNum(U64 epoch, U64 &epochnums);	
	bool GetEpochNum(U64 &epochnums);
	bool UpdateEpochNum(U64 epoch);	
	bool GetLogicalTurncatedTable(std::map<U64,U64>&);
	void PrintLogicalTurncatedTable(void);	
	bool CheckLogicalTurncatedTable(U64 epoch);
	bool UpdateLogicalTurncatedTable(U64 epoch, U64 sequence);	
private:
	std::string file_name;
	std::string num_file_name;
	bool LogicalTruncatedTableInit(void);
	bool LogicalTruncatedTableShutdown(void);
	//DISALLOW_COPY_AND_ASSIGN(LogicalTruncatedTable);
};

void UnitTest7(void);
class InMemLogicalTable{
friend void UnitTest7(void);
public:
	InMemLogicalTable();
	virtual ~InMemLogicalTable(){
		LOG(HCK,("-->"));
		LOG(HCK,("<--"));		
	};
	bool GetEpochOfTheSeq(U64 sequence, U64 & epoch);
	bool LogicalTableWarmUp(void);
	bool UpdateLogicalTable(U64 epoch, U64 sequence);
	void PrintLogicalTable(void);
	int32_t GetLastEpochVectorItem(U64 &epoch, U64 &sequence);
	bool empty(void){
		if (epoch_table_.empty()){
			return true;
		}else{
			return false;
		}
	}
	U64 Size(void){
		U64 sz;
		sz = (U64)epoch_table_.size();
		return sz;
	}
private:
	bool  if_opened_;
	std::map<U64,U64> epoch_table_;			// the data in the map is ordered
	class LogicalTruncatedTable logical_truncated_table_;
};
#endif 
