/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: the implementation of log manager(include how to write all kinds of log data and recovery log)
*/

#include "log_manager.h"

#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>

#include <sstream>
#include <fstream>

#include "file_manager.h"
#include "log_reader.h"
#include "log_writer.h"
#include "frecover.h"
#include "leadercatchup.h"
#include "pqueue.h"

std::string FileName(std::string & name, U64 number) {
	char buf[10];
	snprintf(buf,sizeof(buf),"%llu",number);	
	return name+buf;
}

bool FileIfEmpty(std::string filename) {
	struct stat info;
        stat(filename.c_str(), &info);
        int size = info.st_size;
        if (size == 0)
		return true;
	else
		return false;
}

/*
  ****function: get all of the file name of log directory
  */
bool GetFileName(std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return false;
    }
	std::string filename;
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
	if (entry->d_name != "." && entry->d_name != "..") {
		filename = dir + entry->d_name;
                if(!FileIfEmpty(filename))
			  result->push_back(entry->d_name);
	}
    }
    closedir(d);
    return true;
}

/*
  *****function : convert string into unsigned long long number
  */
bool ParseFileNumber(std::string str, U64 & number) {
	std::stringstream stream;
	stream << str;
	stream >> number;
	return true;
}


/*
  **** function: decode the last log number from the log directory
  */
U64 GetLastLogNumber(std::string str) {
	std::vector<std::string> filenames;		// used to store all of the log name
	GetFileName(str,&filenames);
	U64 max_number = 0,number = 0;
	for (size_t i=0;i < filenames.size(); i++) {
		if (ParseFileNumber(filenames[i],number) && number>max_number)
			max_number = number;
	}
	return max_number;
}

LogManager::LogManager()
	:
	last_log_number(0),
	current_log_number(1),
	last_sequence(0),
	current_epoch_(0),
	cmt(0),
	log_do_recovery(0){
	for (size_t i = 0; i < max_follower; i++) {		
		reader_number[i] = 0;
		fd[i] = -1;
		reader[i] = NULL;
		reader_file_[i] = NULL;
	}
	files_.clear();
	cmt_file_name_ = (std::string)CMT_FILENAME;
	manifest_name = (std::string)MANIFEST_NAME;
	log_dir = (std::string)LOG_DIR;
	writer = NULL;
	writer_file_ = NULL;
	manifest = NULL;
	manifest_file_ = NULL;
}

LogManager::~LogManager() {
	LogManagerShutdown();
}

bool LogManager::LogManagerInit() {
	char command[600];
	if (log_do_recovery == 0) {
		sprintf(command, "rm -rf %s", log_dir.c_str());		// delete the log directory
		if ( system(command) != 0) {
			LOG(HCK,("./log/ directory doesn't exist.\n"));
		}
		sprintf(command, "mkdir %s", log_dir.c_str());		// set up the log directory
		if ( system(command) != 0) {
			assert(0);
		}

		sprintf(command, "rm -rf %s", (char*)META_DIR);		// delete the meta directory
		if ( system(command) != 0) {
			LOG(HCK,("./meta/ directory doesn't exist.\n"));
		}	
		sprintf(command, "mkdir %s", (char*)META_DIR);		// set up the meta directory
		if ( system(command) != 0) {
			assert(0);
		}
		// remove(manifest_name.c_str());					// the above code has delete the manifest file
		sprintf(command, "touch  %s", manifest_name.c_str());	// set up a new empty manifest file
		if ( system(command) != 0) {
			assert(0);
		}
	}
	LogManagerStart();
	return true;
}

bool LogManager::LogManagerShutdown() {
	int i = 0;
	if (writer != NULL) {
		delete writer;
		writer = NULL;
	}
	for (i = 0;i < max_follower;i++)
		if (reader[i]!=NULL) {
			delete reader[i];
			reader[i] = NULL;
		}
	if (manifest != NULL) {
		delete manifest;
		manifest = NULL;
	}
	for (i = 0;i < max_follower; i++)
		if (reader_file_[i]!=NULL) {
			delete reader_file_[i];
			reader_file_[i] = NULL;
		}
	if (writer_file_ != NULL) {
		delete writer_file_;
		writer_file_ = NULL;
	}
	if (manifest_file_ != NULL) {
		delete manifest_file_;
		manifest_file_ = NULL;
	}

	for (size_t i = 0;i < max_follower;i++) {		
		reader_number[i] = 0;
		fd[i] = -1;
	}
	last_log_number = 0;
	current_log_number = 1;
	files_.clear();	
	last_sequence = 0;
	current_epoch_ = 0;
	last_log_number = 0;
	cmt = 0;
}

/*
 ***** function: the original log start method, it will not delete any log data
  */
void LogManager::LogManagerStart() {
	last_log_number = GetLastLogNumber(log_dir);	// the last log number
	// read all of the file metadata from log
	if (last_log_number>0) {
		LogReader *read_manifest;
		ReadableFile *read_manifest_file;
		NewReadableFile(manifest_name, &read_manifest_file);
		read_manifest = new LogReader(read_manifest_file,true,0,true);
		FileMetaData f;
		
		while (read_manifest->ReadFileMetaData(&f)) {
			files_.push_back(f);
			last_sequence = f.largest;		
			current_epoch_ = f.epoch;
		}
		if (read_manifest_file != NULL)
			delete read_manifest_file;
		if (read_manifest != NULL)
			delete read_manifest;
		//the above code read all of the file metadata
		if (files_.size() > 0) {
			f = files_[files_.size()-1];
			if (last_log_number > f.number) {		//recovery the last file metadata
				LogReader *reader_log;
				ReadableFile *reader_log_file;
				std::string reader_log_name = FileName(log_dir, f.number+1);
				last_log_number = f.number + 1;
				NewReadableFile(reader_log_name, &reader_log_file);
				reader_log= new LogReader(reader_log_file,true,0,true);	

				proposal pr;
				while (reader_log->ReadProposal(&pr)) {
					free(pr.kv.kv.key);
					free(pr.kv.kv.value);
					last_sequence=pr.pn.sequence;		// recovery the lsn
					current_epoch_ = pr.pn.epoch;
				}
				FileMetaData ff;
				ff.file_size = 0;
				ff.number = last_log_number;
				ff.refs = 0;
				ff.smallest = f.largest +1;
				ff.largest = last_sequence;
				ff.epoch = current_epoch_;
				files_.push_back(ff);

				if (reader_log != NULL)
					delete reader_log;
				if (reader_log_file != NULL)
					delete reader_log_file;
			}
			else{
				if (last_log_number < f.number)
					assert(0);
			}
		}
		else {
			if (last_log_number == 1) {
				LogReader *reader_log;
				ReadableFile *reader_log_file;
				std::string reader_log_name = FileName(log_dir, 1);
				NewReadableFile(reader_log_name, &reader_log_file);
				reader_log= new LogReader(reader_log_file,true,0,true);	

				proposal pr;
				while (reader_log->ReadProposal(&pr)) {
					free(pr.kv.kv.key);
					free(pr.kv.kv.value);
					last_sequence=pr.pn.sequence;		// recovery the lsn
					current_epoch_ = pr.pn.epoch;
				}
				FileMetaData ff;
				ff.file_size = 0;
				ff.number = last_log_number;
				ff.refs = 0;
				ff.smallest = 1;
				ff.largest = last_sequence;
				ff.epoch = current_epoch_;
				files_.push_back(ff);

				if (reader_log != NULL)
					delete reader_log;
				if (reader_log_file != NULL)
					delete reader_log_file;
			}
			else{
				LOG(HCK,("---------------log num wrong\n"));
				assert(0);
			}
		}
	}
	NewWritableFile(manifest_name, &manifest_file_);
	manifest = new LogWriter(manifest_file_);

	for (size_t i = 0;i < files_.size(); i++) {	
		//  write the file metadata into log, then all of the start process execute end.
		FileMetaData f = files_[i];
		if(!manifest->AddRecord(f))
			assert(0);
	}
	current_log_number = last_log_number + 1;		// use a new log number for new data
	current_log_name = FileName(log_dir, current_log_number);
	NewWritableFile(current_log_name, &writer_file_);
	writer= new LogWriter(writer_file_);

	char command[50];
		sprintf(command, "dd if=/dev/zero of=%s bs=10M count=1 ; sync ", current_log_name.c_str());
	if ( system(command) != 0){
		LOG(HCK,("new log file %s fail.",current_log_name.c_str()));
		assert(0);
	}

	
}

// this start method will delete all of the data after cmt
void LogManager::FollowerLogStart() {		
	LogManagerShutdown();
	cmt = ReadCmt().sequence;		// get the cmt data
	last_log_number = GetLastLogNumber(log_dir);	// get the last log number

	if (last_log_number > 0) {
		// initialize the filemeta data message
		if(cmt != 0)
			CatchUpInit();		// recovery the last time metadata
		else{	// delete all of the data in the log directory
			system("rm ../log/*");
			last_log_number = 0;
		}
	}
	NewWritableFile(manifest_name, &manifest_file_);
	manifest = new LogWriter(manifest_file_);

	for (size_t i = 0;i < files_.size(); i++) {		
		//  write the file metadata into log, then all of the start process execute end.
		FileMetaData f = files_[i];
		if(!manifest->AddRecord(f))
			assert(0);
	}
	current_log_number = last_log_number + 1;		// use a new log number for new data
	current_log_name = FileName(log_dir, current_log_number);
	NewWritableFile(current_log_name, &writer_file_);
	writer= new LogWriter(writer_file_);

	char command[50];
		sprintf(command, "dd if=/dev/zero of=%s bs=10M count=1 ; sync ", current_log_name.c_str());
	if ( system(command) != 0){
		LOG(HCK,("new log file %s fail.",current_log_name.c_str()));
		assert(0);
	}

}

/*
  **** function: recovery the log metadata befor node crash, the last log metadata may not write into log
  */
bool LogManager::RecoverFileMetadata() {
	bool flag = true;
	LogReader *read_manifest;
	ReadableFile *read_manifest_file;
	NewReadableFile(manifest_name, &read_manifest_file);
	read_manifest = new LogReader(read_manifest_file,true,0,true);
	FileMetaData f;
	
	while (read_manifest->ReadFileMetaData(&f)) {
		if (f.largest < cmt) {
			files_.push_back(f);
			last_sequence = f.largest;
			current_epoch_ = f.epoch;
		}
		else if (f.largest == cmt) {
			files_.push_back(f);
			last_sequence = f.largest;
			current_epoch_ = f.epoch;
			flag = false;	
			// this flag represent all of the file after this file is useless, the file that contain cmt will not need recovery
			break;
		}
		else
			break;
	}
	if (read_manifest_file != NULL) {
		delete read_manifest_file;
	}
	if (read_manifest != NULL) {
		delete read_manifest;
	}
	return flag;
}

bool LogManager::AddRecord(proposal &pr) {
	if (!writer->get_eof())		// if the log file is not full,we can write data again
	{
		if(!writer->AddRecord(pr)) {
			LOG(HCK,("add proposal record error.\n"));
			return false;
		}
		if(!writer_file_->Sync()) {
			LOG(HCK,("sync log error.\n"));
			return false;
		}
	}
	else{			//the current log if full,then we need new a new log 
		FileMetaData f;
		f.file_size = writer->get_file_size();
		f.number = current_log_number;
		f.smallest = writer->get_min_seq();
		f.largest = writer->get_last_seq();
		f.epoch = writer->get_epoch();
		files_.push_back(f);		// write the log metadata into vector
		if(!manifest->AddRecord(f))	 { //  write the log metadata into manifest file
			LOG(HCK,("add manifest message error.\n"));
			return false;
		}
		if (writer_file_ != NULL) {
			delete writer_file_;
			writer_file_ = NULL;
		}
		if (writer != NULL){
			delete writer;
			writer = NULL;
		}
		last_log_number = current_log_number;
		++current_log_number;
		current_log_name = FileName(log_dir, current_log_number);
		NewWritableFile(current_log_name, &writer_file_);
		writer= new LogWriter(writer_file_);
		//LOG(VRB,("epoch:%llu  key:%llu\n", pr.pn.epoch, pr.pn.sequence));

		char command[50];
		sprintf(command, "dd if=/dev/zero of=%s bs=10M count=1 ; sync ", current_log_name.c_str());
		if ( system(command) != 0){
			LOG(HCK,("new log file %s fail.",current_log_name.c_str()));
			assert(0);
		}

		if(!writer->AddRecord(pr)) {
			LOG(HCK,("add proposal record error.\n"));
			return false;
		}
		if(!writer_file_->Sync()) {
			LOG(HCK,("sync log error.\n"));
			return false;
		}
	}
	last_sequence = pr.pn.sequence;
	current_epoch_ = pr.pn.epoch;
	return true;
}

bool LogManager::AddRecord(fproposal &fpr) {
	if (!writer->get_eof())		// if the log file is not full,we can write data again
	{
		if(!writer->AddRecord(fpr)) {
			LOG(HCK,("add fproposal record error.\n"));
			return false;
		}
		if(!writer_file_->Sync()) {
			LOG(HCK,("sync log error.\n"));
			return false;
		}
		//LOG(VRB,("epoch:%llu  sequence:%llu\n", fpr.pn.epoch, fpr.pn.sequence));
		//LOG(VRB,("key:%s.\n",fpr.kv.key));
	}
	else {			//the current log if full,then we need new a new log 
		FileMetaData f;
		f.file_size = writer->get_file_size();
		f.number = current_log_number;
		f.smallest = writer->get_min_seq();
		f.largest = writer->get_last_seq();
		f.epoch = writer->get_epoch();
		files_.push_back(f);		// write the log metadata into vector
		if(!manifest->AddRecord(f))	 { //  write the log metadata into manifest file
			LOG(HCK,("add manifest message error.\n"));
			return false;
		}
		if(writer_file_ != NULL){
			delete writer_file_;
			writer_file_ = NULL;
		}
		if (writer!=NULL) {
			delete writer;
			writer = NULL;
		}
		last_log_number = current_log_number;
		++current_log_number;
		current_log_name = FileName(log_dir, current_log_number);
		NewWritableFile(current_log_name, &writer_file_);
		writer = new LogWriter(writer_file_);
		//LOG(VRB,("epoch:%llu  sequence:%llu\n", fpr.pn.epoch, fpr.pn.sequence));

		char command[50];
		sprintf(command, "dd if=/dev/zero of=%s bs=10M count=1 ; sync ", current_log_name.c_str());
		if ( system(command) != 0){
			LOG(HCK,("new log file %s fail.",current_log_name.c_str()));
			assert(0);
		}

		if(!writer->AddRecord(fpr)) {
			LOG(HCK,("add fproposal record error.\n"));
			return false;
		}
		if(!writer_file_->Sync()) {
			LOG(HCK,("sync log error.\n"));
			return false;
		}
	}
	last_sequence = fpr.pn.sequence;
	current_epoch_ = fpr.pn.epoch;
	return true;
}

bool LogManager::AddRecord(class FCatchupItem &fci) {
	if (!writer->get_eof())		// if the log file is not full,we can write data again
	{
		if(!writer->AddRecord(fci)) {
			LOG(HCK,("add fcatchupitem record error.\n"));
			return false;
		}
		if(!writer_file_->Sync()) {
			LOG(HCK,("sync log error.\n"));
			return false;
		}
		//LOG(VRB,("epoch:%llu  sequence:%llu\n", fci.pn.epoch, fci.pn.sequence));
             //LOG(VRB,("key:%s\t\tvalue:%s\n",fci.kv.key,fci.kv.value));
	}
	else{			//the current log if full,then we need new a new log 
		FileMetaData f;
		f.file_size = writer->get_file_size();
		f.number = current_log_number;
		f.smallest = writer->get_min_seq();
		f.largest = writer->get_last_seq();
		f.epoch = writer->get_epoch();
		files_.push_back(f);		// write the log metadata into vector
		if(!manifest->AddRecord(f))	 { //  write the log metadata into manifest file
			LOG(HCK,("add manifest message error.\n"));
			return false;
		}
		if (writer_file_ != NULL){
			delete writer_file_;
			writer_file_ = NULL;
		}
		if (writer != NULL) {
			delete writer;
			writer = NULL;
		}
		last_log_number = current_log_number;
		++current_log_number;
		current_log_name = FileName(log_dir, current_log_number);
		NewWritableFile(current_log_name, &writer_file_);
		writer = new LogWriter(writer_file_);
		//LOG(HCK,("epoch:%llus  key:llu\n", fci.pn.epoch, fci.pn.sequence));

		char command[50];
		sprintf(command, "dd if=/dev/zero of=%s bs=10M count=1 ; sync ", current_log_name.c_str());
		if ( system(command) != 0){
			LOG(HCK,("new log file %s fail.",current_log_name.c_str()));
			assert(0);
		}

		if(!writer->AddRecord(fci)) {
			LOG(HCK,("add fcatchupitem record error.\n"));
			return false;
		}
		if(!writer_file_->Sync()) {
			LOG(HCK,("sync log error.\n"));
			return false;
		}
	}
	last_sequence = fci.pn.sequence;
	current_epoch_ = fci.pn.epoch;
	return true;
}

bool LogManager::ReadRecord( class  CatchupItem *citem,int fd_num) {
	U64 seq = citem->pn.sequence;
	U64 epo = citem->pn.epoch;
	if (seq > last_sequence || epo > current_epoch_) {
		return false;
	}
	else {
		while (true) {
			if(reader[fd_num]->ReadCatchupItem(citem) ) {
				if (citem->pn.sequence == seq&& citem->pn.epoch == epo)
					break;
				if(citem->kv.key!=NULL)
					free(citem->kv.key);
				if(citem->kv.value!=NULL)
					free(citem->kv.value);					
			}
			else {				// the data requested is not in the current file,we need change a file
				reader_number[fd_num] = 0;
				if (reader_file_[fd_num] != NULL) {
					delete reader_file_[fd_num];
					reader_file_[fd_num] = NULL;
				}
				if (reader[fd_num] != NULL) {
					delete reader[fd_num];
					reader[fd_num] = NULL;
				}
				if(!CatchUpBegin(citem,fd_num)) {
					LOG(HCK,("catch up begin error.\n"));
					return false;
				}
				else {
					break;
				}
			}
		}
	}
	//LOG(HCK,("key:%s\t value:%s.\n",citem->kv.key,citem->kv.value));
	return true;
}

/*
 ***** function: recovery the log metadata into memory and recovery the last log metadata(that may not been written into manifest file)
  **********this function is the follower's functioon, it represent after the node crash,how we can do
  */
void LogManager::CatchUpInit() {
	bool recover_flag;	// this flag represent the return data of RecoverFileMetada() function
	recover_flag = RecoverFileMetadata();
	U64 recover_log_number;
	bool flag = false;		
	// this flag represent last time if there are log file metadata that haven't been written into manifest file
	// recovery the last log file that its metadata hasn't been written into manifest
	if (recover_flag) {
		if (files_.size() > 0){
			FileMetaData f = files_[files_.size()-1];
			if (last_log_number > f.number) {		// delete all of the log data that bigger than cmt
				recover_log_number = f.number+1;
				// delete all of the useless log file
				for (U64 i = recover_log_number + 1; i <= last_log_number; i++) {
					std::string name = FileName(log_dir, i);
					remove(name.c_str());
				}
				flag = true;
			}
		}
		else if (last_log_number > 0) {
			recover_log_number = 1;
			for(U64 i = recover_log_number + 1;i <= last_log_number;i++){ // delete all of the useless log file
				std::string name = FileName(log_dir, i);
				remove(name.c_str());
			}
			flag = true;
		}
	}
	else {		//  here,we write the last log file metadata into files_
		for(U64 i = files_[files_.size()-1].number + 1;i <= last_log_number; i++){
			std::string name = FileName(log_dir, i);
			remove(name.c_str());
		}
		last_log_number = files_[files_.size()-1].number;
	}

	if (flag&&recover_flag) {
		LogReader *read_log;
		ReadableFile *read_log_file;
		std::string read_log_name = FileName(log_dir, recover_log_number);
		NewReadableFile(read_log_name, &read_log_file);
		read_log = new LogReader(read_log_file,true,0,true);
		LogWriter *write_log;
		WritableFile *write_log_file;
		std::string write_log_name = "tmp";
		NewWritableFile(write_log_name, &write_log_file);
		write_log = new LogWriter(write_log_file);
		proposal pr;

		while (read_log->ReadProposal(&pr) && pr.pn.sequence <= cmt) {
			write_log->AddRecord(pr);
			free(pr.kv.kv.key);
			free(pr.kv.kv.value);
		}
		std::string last_log_name = FileName(log_dir, recover_log_number);
		// delete the recover_log_number file and change tmp file into recover_log_number file
		remove(last_log_name.c_str());
		rename(write_log_name.c_str(),last_log_name.c_str());		
		last_log_number = recover_log_number;	
		// reflash last_log_number,because maybe the latter log file is deleted
		FileMetaData f;
		f.file_size = write_log->get_file_size();
		f.number = last_log_number;
		f.smallest = write_log->get_min_seq();
		f.largest = write_log->get_last_seq();
		f.epoch = write_log->get_epoch();
		files_.push_back(f);		// write the file metadata into manifest file
		last_sequence = f.largest;
		current_epoch_ = f.epoch;

		if (read_log_file != NULL) {
			delete read_log_file;
		}
		if (read_log != NULL) {
			delete read_log;
		}
		if (write_log_file != NULL) {
			delete write_log_file;
		}
		if (write_log != NULL) {
			delete write_log;
		}
	}
}

bool LogManager::CatchUpBegin(class  CatchupItem *citem, int fd_num) {
	U64 seq = citem ->pn.sequence;
	U64 epo = citem->pn.epoch;	
	if (seq > last_sequence || epo > current_epoch_) {
		return false;
	}
	else {
		for (U32 i = 0;i < files_.size(); i++) {
			if (seq >= files_[i].smallest && seq <= files_[i].largest) {
				reader_number[fd_num] = files_[i].number;
				catchup_manifest_[fd_num] = files_[i];
				break;
			}
		}
		if (reader_number[fd_num] != 0) {	
			// the request data is in a full log file
			reader_name[fd_num] = FileName(log_dir, reader_number[fd_num]);
			NewReadableFile(reader_name[fd_num], &reader_file_[fd_num]);
			// reader[fd_num] = new LogReader(reader_file_[fd_num],true,0,false);
			reader[fd_num] = new LogReader(reader_file_[fd_num],true,0,true);
			while (true) {
				if(reader[fd_num]->ReadCatchupItem(citem) ) {
					if (citem->pn.sequence == seq && citem->pn.epoch == epo)
						break;
					if(citem->kv.key!=NULL)
						free(citem->kv.key);
					if(citem->kv.value!=NULL)
						free(citem->kv.value);					
				}
			}
		}
		else{				
			if (seq > last_sequence) {
				return false;
			}
			else {
				 // the request data is in the current written log
				reader_number[fd_num] = current_log_number;
				reader_name[fd_num] = current_log_name;
				NewReadableFile(reader_name[fd_num], &reader_file_[fd_num]);
				//reader[fd_num] = new LogReader(reader_file_[fd_num],true,0,false);
				reader[fd_num] = new LogReader(reader_file_[fd_num],true,0,true);
				while (true) {
					if(reader[fd_num]->ReadCatchupItem(citem) ) {
						if (citem->pn.sequence == seq && citem->pn.epoch == epo)
							break;
						if(citem->kv.key!=NULL)
							free(citem->kv.key);
						if(citem->kv.value!=NULL)
							free(citem->kv.value);					
					}
				}
			}
		}
	}	
	return true;
}

bool LogManager::CatchUpEnd(int fdfd) {
	bool flag = false;
	for (size_t i = 0; i < max_follower; i++) {
		if (fd[i] == fdfd) {
			if (reader_file_[i] != NULL) {
				delete reader_file_[i];
				reader_file_[i] = NULL;
			}
			if (reader[i] != NULL) {
				delete reader[i];
				reader[i] = NULL;
			}
			fd[i] = -1;
			reader_number[i] = 0;
			LOG(HCK,("%d catch up end ok.\n",fdfd))
			flag = true;
			break;
		}
	}
	return true;
}

bool LogManager::CatchUp(class CatchupItem * citem, int fdfd) {
	size_t i = 0,j = -1;
	bool flag = false;
	for (i = 0; i < max_follower; i++) {
		if (fd[i] == fdfd) {
			flag = true;
			if (ReadRecord(citem, i))
				return true;
			break;				
		}
		else if (fd[i] == -1)
			j = i;
	}
	if (!flag) {		
		fd[j] = fdfd;
		if (CatchUpBegin(citem, j))
			return true;
	}
	return false;
}

int LogManager::LogGetLsn(U64 epoch, U64 sequence, U64 &lsn) {
	int ret_flag = 0;
	if (sequence == last_sequence + 1) {
		lsn = last_sequence;
		LOG(HCK,("no item readed\n"));
		return 0;
	}
	else if (sequence < last_sequence + 1) {
		lsn = last_sequence;
		return 1;
	}
	else {
		LOG(HCK,("cmt > last_sequence what's wrong\n"));
		assert(0);
		return -1;
	}
}
U64 LogManager::get_last_sequence(){
	return last_sequence;
}

U64 LogManager::get_current_epoch(){
	return current_epoch_;
}

/*
  ***** function: read the cmt data from the cmt file
  */
ProposalNum LogManager::ReadCmt() {
	U64 epoch,sequence,rangenum;
	std::ifstream infile(cmt_file_name_.c_str(),std::ios::in);
	ProposalNum pn;
	if (infile >> rangenum >> epoch >> sequence) {
		infile.close();
		pn.epoch = epoch;
		pn.sequence = sequence;
	}
	else {
		pn.epoch = 0;
		pn.sequence = 0;
	}
	return pn;
}
