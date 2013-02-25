/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: 
*/

#ifndef PAXSTORE_LOG_READER_
#define PAXSTORE_LOG_READER_
#include "kvproposal.h"
#include "leadercatchup.h"
#include "file_manager.h"

class LogReader{
public:
	LogReader(ReadableFile * file, bool checksum, U64 initial_offset,bool flag_end);
	~LogReader();

	bool ReadProposal(proposal * pr);	
	bool ReadCatchupItem(class  CatchupItem *citem);
	bool ReadFileMetaData(FileMetaData *f);

	bool ReadRecord(Slice * record, std::string * scratch);
	U64 LastRecordoffset();
private:
	ReadableFile * file_;
	bool flag_end_;
	bool const checksum_;
	char * const backing_store_;		// assist to read data
	Slice buffer_;						// to store a chunck that has been read from file
	bool eof_;						// to judge if a chunk that read from file has been used up
	U64 last_record_offset_;		       // the last record offset

	U64 end_of_buffer_offset_;		// to record the file offset after read a chunk last time
	U64 initial_offset_;

	enum {
		KEof = kMaxRecordType + 1,
		KBadRecord = kMaxRecordType + 2
	};
	bool SkipToInitialBlock();
	unsigned int ReadPhysicalRecord(Slice * result);
};
#endif
