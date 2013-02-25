/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: 
*/

#include "log_reader.h"

#include "cfg.h"
#include "log_encode.h"

LogReader::LogReader(ReadableFile * file, bool checksum, U64 initial_offset,bool flag_end)
	:
	file_(file),
	checksum_(checksum),
	backing_store_(new char[kBlockSize]),
	buffer_(),
	flag_end_(flag_end),
	eof_(false),
	last_record_offset_(0),
	end_of_buffer_offset_(0),
	initial_offset_(initial_offset) {
}

LogReader::~LogReader() {
	delete[] backing_store_;
}

bool LogReader::ReadProposal(proposal * pr) {
	Slice record;
	std::string scratch;
	if (!ReadRecord(&record, &scratch)) {
		LOG(HCK,("read proposal record fail\n"));
		return false;
	}
	DecodeProposal(pr, &record);
	return true;
}

bool LogReader::ReadCatchupItem(class CatchupItem * citem) {
	Slice record;
	std::string scratch;
	if (!ReadRecord(&record, &scratch)) {
		LOG(HCK,("read catchupitem record fail\n"));
		return false;
	}
	DecodeCatchupItem(citem,&record);
	return true;
}

bool LogReader::ReadFileMetaData(FileMetaData *f) {
	Slice record;
	std::string scratch;
	if (!ReadRecord(&record, &scratch)) {
		LOG(HCK,("read filemetadata record fail\n"));
		return false;
	}
	DecodeFileMetaData(f, &record);
	return true;
}

/*
  **** function: read a record from log
  */
bool LogReader::ReadRecord(Slice * record, std::string * scratch) {
	if (last_record_offset_ < initial_offset_) {	// to see if we need skip the initial offset
   	 	if (!SkipToInitialBlock()) {
			LOG(HCK,("skip init block fail\n"));  
			return false;
    		}
  	}
	scratch->clear();
	record->clear();
	bool in_fragmented_record = false;		// represent the currenty record is been divided into some chunk
	U64 prospective_record_offset = 0;
	Slice fragment;

	while (true) {
		U64 physical_record_offset = end_of_buffer_offset_ - buffer_.size();		// record the real place of the current record
		const unsigned int record_type = ReadPhysicalRecord(&fragment);		// read data from file
		switch (record_type) {
			case kFullType:
				if (in_fragmented_record) {
					if (scratch->empty()) {
						in_fragmented_record = false;
					}
					else {
						LOG(HCK,("kfulltype wrong!\n "));
						return false;
					}
					
				}
				prospective_record_offset = physical_record_offset;
				scratch->clear();
				*record = fragment;
				last_record_offset_ = prospective_record_offset;
				return true;
			case kFirstType:
				if (in_fragmented_record) {
					if (scratch->empty()) {
						in_fragmented_record = false;
					}
					else {
						LOG(HCK,("kfristtype wront!\n"));
						return false;
					}
						
				}
				prospective_record_offset = physical_record_offset;
				scratch->assign(fragment.data(),fragment.size());
				in_fragmented_record = true;
				break;
			case kMiddleType:
				if (!in_fragmented_record) {
					LOG(HCK,("kmiddle type wront\n"));
					return false;
				}
				else {
					scratch->append(fragment.data(),fragment.size());
				}
				break;
			case kLastType:
				if (!in_fragmented_record) {
					LOG(HCK,("klasttype wrong\n"));
					return false;
				}
				else {
					scratch->append(fragment.data(),fragment.size());
					*record = Slice(*scratch);
					// scratch ->clear();
					last_record_offset_ = prospective_record_offset;
					return true;
				}
				break;
			case KEof:
				if (in_fragmented_record) {
					scratch->clear();
					LOG(HCK,("no data in log\n"));
				}
				return false;
			case KBadRecord:
				if (in_fragmented_record) {
					in_fragmented_record = false;
					scratch ->clear();
				}
				// break;
				return false;		// in the leveldb it is break but return
			default:{
				in_fragmented_record = false;
				scratch ->clear();
				break;
			}
		}		
	}
	LOG(HCK,("unknow wrong\n"));
	return false;
}


U64 LogReader::LastRecordoffset() {
	return last_record_offset_;
}

/*
  **** function:skip the initial block of log
  */
bool LogReader::SkipToInitialBlock() {
	size_t offset_in_block = initial_offset_ % kBlockSize;		
	U64 block_start_location = initial_offset_ -offset_in_block;		
	bool skip_status = true;
	
	if (offset_in_block > kBlockSize - 6) {
		block_start_location += kBlockSize;
		offset_in_block = 0;		
	}
	end_of_buffer_offset_ = block_start_location ;	// record last time after read a record, the file site
	if (block_start_location > 0) {
		skip_status = file_->Skip(block_start_location);	// put the file pointer into the place that will be read
		if (skip_status == false) {
			LOG(HCK,("skip init block wrong!\n"));
		}
		else {
			LOG(HCK,("skip init block right\n"));
		}
	}
	return skip_status;
}

unsigned int LogReader::ReadPhysicalRecord(Slice *result) {
	while (true) {
		if (buffer_.size() < kHeaderSize) {
			if (!eof_) {
				buffer_.clear();
				bool status = file_->Read(kBlockSize, &buffer_, backing_store_);
				end_of_buffer_offset_ += buffer_.size();
				if (!status) {
					LOG(HCK,("read block fail\n"));
					buffer_.clear();
					eof_ = flag_end_;		//int the leveldb ,they make eof_=true
					return KEof;
				}
				else if (buffer_.size() < kBlockSize) {
					eof_ = flag_end_;		//int the leveldb ,they make eof_=true
				}
				continue;
			}
			else if (buffer_.size() == 0) {
				LOG(HCK,("bufer.size()=0\n"));
				return KEof;
			}
			else {
				size_t drop_size = buffer_.size();
        			buffer_.clear();
        			LOG(HCK,("truncated record at end of file %d.\n",drop_size));
        			return KEof;
			}
		}
		const char * header = buffer_.data();
		const U32 a = static_cast<U32>(header[4]) & 0xff;
 		const U32 b = static_cast<U32>(header[5]) & 0xff;
    		const unsigned int type = header[6];
    		const U32 length = a | (b << 8);
		if (kHeaderSize + length > buffer_.size()) {
			size_t drop_size = buffer_.size();
			buffer_.clear();
      			LOG(HCK,("bad record length %d.\n",drop_size));
			return KBadRecord;
		}

		if(type == kZeroType && length ==0) {
			buffer_.clear();
			LOG(HCK,("kzerotype \n"));
			return KBadRecord;
		}
		//to make crc checkout, in out project,we didn't realize
		buffer_.remove_prefix(kHeaderSize + length);
		if (end_of_buffer_offset_ -buffer_.size() - kHeaderSize -length < initial_offset_) {
			result->clear();
			LOG(HCK,("end_of_buffer_offset_ -buffer_.size() - kHeaderSize -length < initial_offset_\n"));
			return KBadRecord;
		}
		*result = Slice(header+kHeaderSize,length);
		return type;		
	}
}
