/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: operate the file to manager log
*/

#include "file_manager.h"

#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>

#define fread_unlocked fread

bool ReadableFile::Read(size_t n, Slice* result, char* scratch) {
	bool s = true;
	//LOG(HCK, ("the file offset:%llu \n",ftell(file_)));
	//LOG(HCK, ("filename: %s\n", filename_.c_str()));
	size_t r = fread_unlocked(scratch, 1, n, file_);
	//LOG(HCK,("%p.\n",file_));
	//LOG(HCK,("%s.\n",scratch));
	//LOG(HCK,("readed size %d\n", r));
	*result = Slice(scratch, r);

	if (r < n) {
		if (feof(file_)) {
			LOG(HCK,("feof\n"));
			// We leave bool as ok if we hit the end of the file
		} else {
			// A partial read with an error: return a non-ok bool
			s = false;
		}
	}
	return s;
}

bool ReadableFile::Skip(U64 n) {
	if (fseek(file_, n, SEEK_CUR)) {
		return false;
	}
	return true;
}
/*
bool WritableFile::Append(const Slice& data) {
	const char* src = data.data();
	size_t left = data.size();
	while (left > 0) {
		assert(base_ <= dst_);
		assert(dst_ <= limit_);
		size_t avail = limit_ - dst_;
		if (avail == 0) {	// 判断上次mmap的空间是否用完了?
			if (!UnmapCurrentRegion() ||
				!MapNewRegion()) {
				return false;
			}
		}
		
		size_t n = (left <= avail) ? left : avail;
		memcpy(dst_, src, n);
		dst_ += n;
		src += n;
		left -= n;
	}
	return true;
}

  bool WritableFile::Close() {
	bool s = true;
	size_t unused = limit_ - dst_;
	if (!UnmapCurrentRegion()) {
		s = false;
	} else if (unused > 0) {
		// Trim the extra space at the end of the file
		if (ftruncate(fd_, file_offset_ - unused) < 0) {
			s = true;
		}
	}
	
	if (close(fd_) < 0) {
		s = false;
	}
	fd_ = -1;
	base_ = NULL;
	limit_ = NULL;
	return s;
}

bool WritableFile::Flush() {
	return true;
}

bool WritableFile::Sync() {
	bool s = true;
	if (pending_sync_) {
		// Some unmapped data was not synced
		pending_sync_ = false;
//		if (fdatasync(fd_) < 0) {		
		if (fsync(fd_) < 0) {		
			// here we call fsync() function to write the data that hasn't unmap into disk.
			s = false;
		}
	}
	
	if (dst_ > last_sync_) {
		// Find the beginnings of the pages that contain the first and last
		// bytes to be synced.
		size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
		size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
		last_sync_ = dst_;
		if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
			s = false;
		}
	}
	return s;
}

bool WritableFile::UnmapCurrentRegion() {
	bool result = true;
	if (base_ != NULL) {
		if (last_sync_ < limit_) {
			// Defer syncing this data until next Sync() call, if any
			pending_sync_ = true;
		}
		if (munmap(base_, limit_ - base_) != 0) {
			result = false;
		}
		file_offset_ += limit_ - base_;
		base_ = NULL;
		limit_ = NULL;
		last_sync_ = NULL;
		dst_ = NULL;
		// Increase the amount we map the next time, but capped at 1MB
		if (map_size_ < (1<<20)) {
			map_size_ *= 2;
		}
	}
	return result;
}

bool WritableFile::MapNewRegion() {
	assert(base_ == NULL);
	if (ftruncate(fd_, file_offset_ + map_size_) < 0) {
		return false;
	}
	void* ptr = mmap(NULL, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
		fd_, file_offset_);
	if (ptr == MAP_FAILED) {
		return false;
	}
	base_ = reinterpret_cast<char*>(ptr);
	limit_ = base_ + map_size_;
	dst_ = base_;
	last_sync_ = base_;
	return true;
}
*/
bool NewWritableFile(const std::string& fname,WritableFile** result) {
    bool s = true;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
		*result = NULL;
		s = false;
    } else {
		*result = new WritableFile(fname, fd, getpagesize());
    }
    return s;
}
/*
bool NewWritableFile(const std::string& fname,WritableFile** result) {
    bool s = true;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
		*result = NULL;
		s = false;
    } else {
		*result = new WritableFile(fname, fd, getpagesize());
    }
    return s;
}
*/
bool NewReadableFile(const std::string& fname,ReadableFile ** result) {
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return false;
    } else {
      *result = new ReadableFile(fname, f);
      return true;
    }
}



bool WritableFile::Append(const Slice& data) {
	const char* src = data.data();
	int len = data.size();
	if ( write(fd_, src, len) != len ) {
       				LOG(HCK,("WRITER ERROR.\n"));
				assert(0);
        }
	return true;
}

  bool WritableFile::Close() {
	bool s = true;
	if (close(fd_) < 0) {
		s = false;
	}
	
	fd_ = -1;
	return s;
}

  bool WritableFile::Flush() {
	return true;
}

  bool WritableFile::Sync() {
	bool s = true;
		if (fdatasync(fd_) < 0) {
			s = false;
		}
	
	return s;
}

