/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: 
*/

#ifndef PAXSTORE_LOG_WRITER_
#define PAXSTORE_LOG_WRITER_
#include "file_manager.h"
#include "frecover.h"
#include "kvproposal.h"

class LogWriter {
public:
	explicit LogWriter(WritableFile* dest);
	~LogWriter();

	bool AddRecord(FileMetaData &f);
	bool AddRecord(proposal &pr);
	bool AddRecord(class FCatchupItem & fci);
	bool AddRecord(fproposal &fpr);
	bool Add(const Slice &slice);

	int get_file_size();
	int get_last_seq();
	int get_min_seq();
	int get_epoch();
	bool get_eof();
private:
	WritableFile* dest_;
	int block_offset_;       // Current offset in block
	int file_size_;			// current file size
	int last_seq_;			// last sequencenumber
	int min_seq_;			// the min sequencenumber
	int epoch_;			// current epoch
	int fill_number_;		//  current log file number
	bool eof_;			//  to judge if the file is eof,then the log_manager can change a new log
	bool min_seq_flag_;	// to represent the current file min_seq is initialize
	// crc32c values for all supported record types.  These are
	// pre-computed to reduce the overhead of computing the crc of the
	// record type stored in the header.

	U32 type_crc_[kMaxRecordType + 1];
	bool EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);
	// No copying allowed
	LogWriter(const LogWriter&);
	void operator=(const LogWriter&);
};
#endif
