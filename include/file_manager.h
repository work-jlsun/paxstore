/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: operate the file to manager log
*/

#ifndef PAXSTORE_FILE_MANAGER_
#define PAXSTORE_FILE_MANAGER_
#include <string>

#include "slice.h"
#include "kvproposal.h"

class ReadableFile{
public:
	ReadableFile(const std::string& fname, FILE* f)
		: filename_(fname), file_(f) { }
	~ReadableFile() { fclose(file_); }
	FILE* file(){
		return file_;
	}
	bool Read(size_t n, Slice* result, char* scratch) ;	
	bool Skip(U64 n) ;
private:
	std::string filename_;
	FILE* file_;
};
/*
class WritableFile {
public:
	WritableFile(const std::string& fname, int fd, size_t page_size)
		: filename_(fname),
        fd_(fd),
        page_size_(page_size),
        map_size_(Roundup(65536, page_size)),
        base_(NULL),
        limit_(NULL),
        dst_(NULL),
        last_sync_(NULL),
        file_offset_(0),
        pending_sync_(false) {
		assert((page_size & (page_size - 1)) == 0);
	}
	
	~WritableFile() {
		if (fd_ >= 0) {
			WritableFile::Close();
		}
	}
	  bool Append(const Slice& data) ;
	  bool Close() ;
	  bool Flush() ;
	  bool Sync() ;
private:
	std::string filename_;
	int fd_;
	size_t page_size_;
	size_t map_size_;       // How much extra memory to map at a time
	char* base_;            // The mapped region
	char* limit_;           // Limit of the mapped region
	char* dst_;             // Where to write next  (in range [base_,limit_])
	char* last_sync_;       // Where have we synced up to
	U64 file_offset_;  // Offset of base_ in file
	
	// Have we done an munmap of unsynced data?
	bool pending_sync_;
	
	// Roundup x to a multiple of y
	static size_t Roundup(size_t x, size_t y) {
		return ((x + y - 1) / y) * y;
	}
	size_t TruncateToPageBoundary(size_t s) {
		s -= (s & (page_size_ - 1));
		assert((s % page_size_) == 0);
		return s;
	}
	bool UnmapCurrentRegion() ;
	bool MapNewRegion() ;
};
*/
class WritableFile {
private:
	std::string filename_;
	int fd_;
	uint64_t file_offset_;  // Offset of base_ in file
	
public:
	WritableFile(const std::string& fname, int fd, size_t page_size)
		: filename_(fname),
        fd_(fd),
        file_offset_(0) {
	}
	
	
	~WritableFile() {
		if (fd_ >= 0) {
			WritableFile::Close();
		}
	}
	
	  bool Append(const Slice& data) ;
	
	  bool Close() ;
	
	  bool Flush() ;
	
	  bool Sync() ;
};

bool NewWritableFile(const std::string& fname,WritableFile** result);
bool NewReadableFile(const std::string& fname,ReadableFile ** result) ;
//bool NewWritableFile(const std::string& fname,WritableFile** result);
//bool NewReadableFile(const std::string& fname,ReadableFile ** result) ;
#endif
