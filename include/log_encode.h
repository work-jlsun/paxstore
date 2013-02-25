/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: use these functions to encode or decode data when reading or writing log data
*/

#ifndef PAXSTORE_LOG_ENCODE_
#define PAXSTORE_LOG_ENCODE_
#include <string.h>
#include <malloc.h>

#include "slice.h"
#include "kvproposal.h"
#include "leadercatchup.h"
#include "frecover.h"

enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};

/*
  *****function: put unsighed int into char*
  */
void EncodeFixed32(char* buf, U32 value) ;
/*
  *****function: put unsighed long long into char*  
  */
void EncodeFixed64(char* buf, U64 value) ;
U32 DecodeFixed32(const char* ptr) ;
U64 DecodeFixed64(const char* ptr) ;
/*
  *****function: put unsighed int into string
  */
void PutVar32(std::string* dst, U32 v) ;
/*
  *****function: put unsighed long long into char*
  */
void PutVar64(std::string* dst, U64 v);

/*
  **** function: put char * value and its length into string
  */
void PutLengthPrefixedSlice(std::string* dst, char* value) ;

void EncodeProposal(proposal &pr,std::string* rep);
void EncodeFproposal(fproposal &fpr,std::string* rep);
void DecodeProposal(proposal * pr, Slice *record);
void EncodeFileMetaData(FileMetaData &f,std::string *rep);
void DecodeFileMetaData(FileMetaData *f,Slice *record);
void EncodeFCatchupItem(FCatchupItem &fci,std::string *rep);
void DecodeCatchupItem(class CatchupItem *citem, Slice *record);
#endif
