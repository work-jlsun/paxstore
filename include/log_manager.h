/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: log manager
*/

#ifndef PAXSTORE_LOG_MANAGER_
#define PAXSTORE_LOG_MANAGER_
#include <string>

#include <vector>

#include "kvproposal.h"

class LogWriter;
class LogReader;
class ReadableFile;
class WritableFile;

typedef struct LogKV {
	U32 key_len;
	U32 value_len;
	char	kv[];
}LogKV;

class LogManager {
public:
	LogManager();
	~LogManager();

	bool LogManagerInit();
	bool LogManagerShutdown();

	void LogManagerStart();
	void FollowerLogStart();
	bool RecoverFileMetadata();	
	bool AddRecord(proposal &pr);	
	bool AddRecord(fproposal & fpr);		
	bool AddRecord(class FCatchupItem &fci);	

	bool ReadRecord(ProposalNum pn);	
	bool ReadRecord( class  CatchupItem *citem,int fd_num);


	void CatchUpInit();		
	bool CatchUpBegin(class  CatchupItem *citem,int fd_num);		
	bool CatchUpEnd(int fdfd) ;
	bool CatchUp(class CatchupItem *citem,int fdfd);

	int LogGetLsn(U64 epoch, U64 sequence, U64 &lsn);
	U64 get_last_sequence();
	U64 get_current_epoch();

	int fd[max_follower];			// this array represent the follower fd when catch up

	ProposalNum ReadCmt();
	void WriteCmt(class P12Info & pinfo);
	void WriteCmt(U32 rangenum, ProposalNum & pn);

	int log_do_recovery;
private:
	U64 cmt;			// the current cmt
	U64 last_sequence;		// the last sequence that has been written into log file
	U64 current_epoch_;		// the epoch of last record,if a new record that has a bigger epoch come,then we will update current_epoch_
	
	LogWriter *writer;		
	LogReader *reader[max_follower];			// used for catchup
	LogWriter *manifest;			// used for manifest file
	ReadableFile* reader_file_[max_follower];
	WritableFile* writer_file_;	//  the current log file
	WritableFile* manifest_file_;
	std::vector<FileMetaData> files_;	// record all of the log file metadata except the current log
	FileMetaData catchup_manifest_[max_follower];		// used forcatchup
	U64 current_log_number;	     // the current log number
	std::string current_log_name;
	std::string reader_name[max_follower];			// the current log that is been reading
	U64 reader_number[max_follower];
	U64 last_log_number;			// last log number
	std::string cmt_file_name_;
	std::string log_dir;
	std::string manifest_name;   
};
#endif
