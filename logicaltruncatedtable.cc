/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * modify: 2012/11/23 
  * function:
*/

#include "logicaltruncatedtable.h"

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <utility> 

 int logical_truncated_table_do_recovery = 1;

InMemLogicalTable::InMemLogicalTable(){
	epoch_table_.clear();
	if_opened_ = false;
}

/*
*we can see that open can only once,
*but the GetLogicalTurncatedTable can do many times 
*the epoch vector is store througth  the map so the same "key"
*item will not store twice
*/
bool InMemLogicalTable::LogicalTableWarmUp(void) {
	if (false == if_opened_) {
		if(!logical_truncated_table_.OpenInit()) {
			LOG(HCK,("logical_truncated_table_ open init error\n"));
			return false;
		}
		if_opened_ = true;
	}
	return 
		logical_truncated_table_.GetLogicalTurncatedTable(epoch_table_);
}

bool InMemLogicalTable::GetEpochOfTheSeq(U64 sequence, U64 & epoch) {
	std::map<U64,U64>::const_iterator map_it;
	U32 count = epoch_table_.size();
	U64 tmp_epoch = 0;
	while (count--) {
		map_it = epoch_table_.find(tmp_epoch);
		if (map_it == epoch_table_.end()) {
			LOG(HCK,("how can this happend\n"));
			assert(0);
			return false;
		}
		if (map_it->second >= sequence) {
			epoch = map_it->first;
			return true;
		} else {
			tmp_epoch++;
			continue;	
		}	
	}
//	LOG(HCK,("can't find the correspond epoch in the logical table, so it the biggest epoch (in the logical table ) +1\n "));
	epoch = epoch_table_.size();
	return true;
}

/*
*
*
*return value :  -1 -> if error
			0 -> the local epoch vector is empty
*			1 -> the local epoch vector is not empty, result is store int the   paremeters
*/
// function:find the last epoch and sequence from the map
int32_t InMemLogicalTable::GetLastEpochVectorItem(U64 &epoch, U64 &sequence) {
	U64 sz;
	U64 last_epoch;
	std::map<U64,U64>::const_iterator map_it;	
	 sz = epoch_table_.size();
	 if (0 == sz) {
	 	LOG(HCK,("the Epoch Vector is empty\n"));
		return 0;
	 } else {
		epoch = sz -1; // because the vector is from [0,1,2,3....] 
		map_it = epoch_table_.find(epoch);
		if (map_it == epoch_table_.end()) {
			LOG(HCK,("maybe the  epoch vector table is not continues\n"));	
			return -1;
		}
		assert(map_it->first == epoch);
		sequence =  map_it->second;
		return 1;
	 } 
}

bool InMemLogicalTable::UpdateLogicalTable(U64 epoch, U64 sequence) {
	//LOG(HCK,("-->"));
	if (!logical_truncated_table_.UpdateLogicalTurncatedTable(epoch, sequence)) {
		LOG(HCK,("UpdateLogicalTurncatedTable error\n"));
		return false;
	}
	//the update the in mem logical table
	std::pair<std::map<U64,U64>::iterator, bool> ret;
	ret = epoch_table_.insert( std::pair<U64,U64>(epoch, sequence));
	if (ret.second == false) {
		LOG(HCK,("Update  in mem Logical Turncated Table error\n"));
		return false;
	}
	//LOG(HCK,("<--"));
	return true;
}

void InMemLogicalTable::PrintLogicalTable(void) {
	logical_truncated_table_.PrintLogicalTurncatedTable();
}

bool LogicalTruncatedTable::LogicalTruncatedTableInit() {
	struct stat sb;
	//Check if the environment dir and db file exists
	int dir_exists = (stat(LOGICAL_TRUNCATED_TABLE_DB_PATH, &sb) == 0);
	int db_exists = (stat(LOGICAL_TRUNCATED_TABLE_DB_FILE_NAME, &sb) == 0);

	if(logical_truncated_table_do_recovery && (!dir_exists || !db_exists)) {	
		printf("Error: Acceptor recovery failed!\n");
		printf("The file:%s does not exist\n", LOGICAL_TRUNCATED_TABLE_DB_FILE_NAME);
        	return false;
	}

	if(!logical_truncated_table_do_recovery && dir_exists) {
        	char rm_command[600];
        	sprintf(rm_command, "rm -r %s", LOGICAL_TRUNCATED_TABLE_DB_PATH);
        	if((system(rm_command) != 0) || 
            		(mkdir(LOGICAL_TRUNCATED_TABLE_DB_PATH,S_IRWXU) != 0)) {
            		printf("Failed to recreate empty env dir %s\n", LOGICAL_TRUNCATED_TABLE_DB_PATH);
			return false;
        	}

		sprintf(rm_command, "touch  %s %s", LOGICAL_TRUNCATED_TABLE_DB_FILE_NAME,LOGICAL_TRUNCATED_TABLE_NUM_FILE_NAME);
		if(system(rm_command)!= 0){
			LOG(HCK,("touch file error\n"));
			return false;
		}			
    	}
	
	if (!logical_truncated_table_do_recovery && !dir_exists) {		// if the ./logical directory doesn't exist,we neeed create it
        	char mkdircommand[600],rm_command[600];
        	sprintf(mkdircommand, "mkdir %s", LOGICAL_TRUNCATED_TABLE_DB_PATH);
        	if(system(mkdircommand) != 0) {
            		printf("Failed to create %s directory.\n", LOGICAL_TRUNCATED_TABLE_DB_PATH);
			assert(0);
        	}

		sprintf(rm_command, "touch  %s %s", LOGICAL_TRUNCATED_TABLE_DB_FILE_NAME,LOGICAL_TRUNCATED_TABLE_NUM_FILE_NAME);
		if(system(rm_command)!= 0){
			LOG(HCK,("touch file error\n"));
			return false;
		}
    	}
	return true;
}

bool LogicalTruncatedTable::LogicalTruncatedTableShutdown() {
	return true;
}

bool LogicalTruncatedTable::GetEpochNum(U64 &epochnums) {
	U64 epochnum;
	std::ifstream infile(num_file_name.c_str(),std::ios::in);
	if (infile >> epochnum) {
		infile.close();
		epochnums = epochnum;
		return true;
	}
	else {
		infile.close();
		LOG(HCK,("the epochnum is not exist\n"));
		epochnums = 0;
		return true;
	}
}

bool LogicalTruncatedTable::UpdateEpochNum(U64 epoch) {
	std::ofstream outfile(num_file_name.c_str(),std::ios::out);
	if (outfile << epoch) {
		outfile.close();
		return true;
	}
	else {
		outfile.close();
		LOG(HCK,(" put error:%s\n"));		
		return false;
	}
}

bool LogicalTruncatedTable::UpdateLogicalTurncatedTable(U64 epoch, U64 sequence) {
	//LOG(HCK,("-->\n"));
	if (!CheckLogicalTurncatedTable(epoch)) {
		LOG(HCK,("CheckLogicalTurncatedTable is not right\n"));
		return false;
	}
	std::ofstream outfile(file_name.c_str(),std::ios::app);
	if (!(outfile << " " << epoch << " " << sequence)) {
		LOG(HCK,(" put error:%s\n"));	
		outfile.close();
		return false;
	}
	if (!UpdateEpochNum(epoch + 1)) {
		outfile.close();
		return false;
	}
	outfile.close();
	//LOG(HCK,("<--\n"));	
	return true;	
}

bool LogicalTruncatedTable::CheckLogicalTurncatedTable(U64 epoch) {
		std::ifstream infile(file_name.c_str(),std::ios::in);
		U64 sequence;
		for (U64 i = 0; i != epoch; i++) {
			if (!(infile>>i>>sequence)) {
				infile.close();
				LOG(HCK,("Record does not exist\n"));   	 	
				return false;
			}
			else {
				continue;
			}			
		}
		infile.close();
		return true;
}

void LogicalTruncatedTable::PrintLogicalTurncatedTable(void) {
	U64 epochnums,sequence;
	if (!GetEpochNum(epochnums)) {
		return ;
	}
	std::ifstream infile(file_name.c_str(),std::ios::in);
	for (U64 i = 0; i != epochnums; i++) {
		if (!(infile >> i >> sequence)) {
			infile.close();
			LOG(HCK,("Record does not exist\n"));   	 	
			return ;
		}
		else {   
			LOG(HCK,("epoch:%llu   sequence:%llu\n", i, sequence));			
    		}	
	}
	infile.close();
	return;
}

bool LogicalTruncatedTable::GetLogicalTurncatedTable(std::map<U64,U64> &epoch_table) {
	U64 epochnums,sequence;
	if (!GetEpochNum(epochnums)) {
		LOG(HCK,("GetEpochNum error\n" ));
		return false;
	}
	if (epochnums == 0) {
		LOG(HCK,("the Truncated table is empty now\n"));
		return true;
	} else {
		LOG(HCK,("the Truncated table item num is %llu\n", epochnums));
	}
	std::ifstream infile(file_name.c_str(),std::ios::in);
	for (U64 i = 0; i != epochnums; i++) {
		if (!(infile >> i >> sequence)) {
			infile.close();
			LOG(HCK,("Record does not exist\n"));   	 	
			return false;
		}
		else {			
    			std::map<U64, U64>::iterator it;
 			std::pair<std::map<U64,U64>::iterator, bool> ret;
			it = epoch_table.find(i);
			if (it == epoch_table.end()) {
    				ret = epoch_table.insert(std::pair<U64,U64>(i,sequence));
				if (ret.second == false ) {
					return false;
				}
				LOG(HCK,("insert epoch_table map ok\n"));
			} else {
				LOG(HCK,("this item have exist\n"));
				continue;
			}
		}			
	}
	infile.close();
	return true;
}

void UnitTest1(void) {
	logical_truncated_table_do_recovery = 0;
	InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()) {
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}
	imtable.UpdateLogicalTable(0, 123);	
	imtable.UpdateLogicalTable(1, 124);
	imtable.UpdateLogicalTable(2, 125);
	imtable.UpdateLogicalTable(3, 126);
	imtable.UpdateLogicalTable(4, 127);
	imtable.PrintLogicalTable();
	sleep(3);
}

void UnitTest2(void) {
	logical_truncated_table_do_recovery = 1;
	InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}
	imtable.UpdateLogicalTable(5, 123);	
	imtable.UpdateLogicalTable(6, 124);
	imtable.UpdateLogicalTable(7, 125);
	imtable.UpdateLogicalTable(8, 126);
	imtable.UpdateLogicalTable(9, 127);
	imtable.PrintLogicalTable();
}
/*
* test for down after update
*/
void UnitTest3(void) {
	logical_truncated_table_do_recovery = 1;
	InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}
	imtable.UpdateLogicalTable(10, 123);	
	imtable.UpdateLogicalTable(11, 124);
	imtable.UpdateLogicalTable(12, 125);
	imtable.UpdateLogicalTable(13, 126);
	imtable.UpdateLogicalTable(14, 127);
	imtable.PrintLogicalTable();
	sleep(3);
}

/*
*used to check if the former update is sync
*/
void UnitTest4(void){
	logical_truncated_table_do_recovery = 1;
	InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}
	imtable.PrintLogicalTable();
}
/*
* test error insert
*/
void UnitTest5(void){
	logical_truncated_table_do_recovery = 0;
	InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}
	imtable.UpdateLogicalTable(0, 123);	
	imtable.UpdateLogicalTable(1, 124);
	imtable.UpdateLogicalTable(2, 125);
	imtable.UpdateLogicalTable(4, 126);
	imtable.PrintLogicalTable();
}

/*
* test lookup the epoch
*/
void UnitTest6(void){
	logical_truncated_table_do_recovery = 0;
	InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}
	imtable.UpdateLogicalTable(0, 0);	
	imtable.UpdateLogicalTable(1, 223);
	imtable.UpdateLogicalTable(2, 323);
	imtable.UpdateLogicalTable(3, 423);
	U64 epoch;
	imtable.GetEpochOfTheSeq( 0 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));

	imtable.GetEpochOfTheSeq( 100 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));


	imtable.GetEpochOfTheSeq( 123 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));

	imtable.GetEpochOfTheSeq( 124 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));

	imtable.GetEpochOfTheSeq( 144 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));


	imtable.GetEpochOfTheSeq( 423 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));


	imtable.GetEpochOfTheSeq( 424 ,epoch);
	LOG(HCK,("epoch:%llu\n", epoch));
}

void UnitTest7(void){
        InMemLogicalTable  imtable;
	if (!imtable.LogicalTableWarmUp()){
		LOG(HCK,("LogicalTableWarmUp error\n"));
	}		
        U64 epochnum = 0;		
        imtable.logical_truncated_table_.GetEpochNum(epochnum);
        LOG(HCK,("the epochnum is %llu\n", epochnum));
}
