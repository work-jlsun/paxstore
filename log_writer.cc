/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: 
*/

#include "log_writer.h"

#include "cfg.h"
#include "log_encode.h"

LogWriter::LogWriter(WritableFile* dest)
    : dest_(dest),
      block_offset_(0),
      epoch_(0),
      last_seq_(0),
      min_seq_(0),
      fill_number_(),
      file_size_(0),
      min_seq_flag_(false),
      eof_(false) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
	type_crc_[i]=0;
  }
}

LogWriter::~LogWriter() {
}

bool LogWriter::AddRecord(FileMetaData &f) {
	std::string str;
	EncodeFileMetaData(f,&str);
	Slice data(str.data(),str.size());
	if(!Add(data)) {
		LOG(HCK,("insert filemetadata fail.\n"));
		return false;
	}
	return true;	
}

bool LogWriter::AddRecord(proposal & pr) {
	std::string str;
	EncodeProposal(pr,&str);
	Slice data(str.data(),str.size());
	if(!Add(data)) {
		LOG(HCK,("insert fail\n"));
		return false;
	}
	if(!min_seq_flag_) {
		min_seq_flag_ = true;
		min_seq_ = pr.pn.sequence;
	}
	epoch_ = pr.pn.epoch;
	last_seq_ = pr.pn.sequence;
	return true;
}

bool LogWriter::AddRecord(class FCatchupItem & fci) {
	std::string str;
	EncodeFCatchupItem(fci,&str);
	Slice data(str.data(),str.size());
	if(!Add(data)) {
		LOG(HCK,("insert fail\n"));
		return false;
	}
	if(!min_seq_flag_) {
		min_seq_flag_ = true;
		min_seq_ = fci.pn.sequence;
	}
	epoch_ = fci.pn.epoch;
	last_seq_ = fci.pn.sequence;
	return true;
}

bool LogWriter::AddRecord(fproposal & fpr) {
	std::string str;
	EncodeFproposal(fpr,&str);
	Slice data(str.data(),str.size());
	if (!Add(data)) {
		LOG(HCK,("insert fail\n"));
		return false;
	}
	if (!min_seq_flag_) {
		min_seq_flag_ = true;
		min_seq_ = fpr.pn.sequence;
	}
	epoch_ = fpr.pn.epoch;
	last_seq_ = fpr.pn.sequence;
	return true;
}

bool LogWriter::Add(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();
  size_t length = left;
  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  bool s=true;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }
    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s && left > 0);
  file_size_ += length;	// the file length
  if(file_size_ >= MaxLogSize)
  	eof_ = true;  
  return s;
}

int LogWriter::get_file_size() {
	return file_size_;
}

int LogWriter::get_last_seq() {
	return last_seq_;
}

int LogWriter::get_min_seq() {
	return min_seq_;
}
int LogWriter::get_epoch() {
	return epoch_;
}

bool LogWriter::get_eof() {
	return eof_;
}

bool LogWriter::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);
  // Format the header
  char buf[kHeaderSize];
  buf[0]=buf[1]=buf[2]=buf[3]='a';	// calculate crc checkout data
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);
/*
  // Compute the crc of the record type and the payload.
  U32 crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);
*/
  // Write the header and the payload
  bool s = dest_->Append(Slice(buf, kHeaderSize));
  if (s) {
    s = dest_->Append(Slice(ptr, n));
    if (s) {
      s = dest_->Flush();
    }
  }
  dest_->Sync();		/// sync data to disk-----------
  block_offset_ += kHeaderSize + n;
  return s;
}
