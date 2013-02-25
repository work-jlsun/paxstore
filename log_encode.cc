/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing 
  * email: china_dyx@163.com
  * date: 2012/11/23
  * function: use this functions to encode or decode data when reading or writing log data
*/

#include "log_encode.h"

void EncodeFixed32(char* buf, U32 value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
}

void EncodeFixed64(char* buf, U64 value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
}

U32 DecodeFixed32(const char* ptr) {
    return ((static_cast<U32>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<U32>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<U32>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<U32>(static_cast<unsigned char>(ptr[3])) << 24));
}

U64 DecodeFixed64(const char* ptr) {
    U64 lo = DecodeFixed32(ptr);
    U64 hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
}

void PutVar32(std::string* dst, U32 v) {
	char buf[5];
   	EncodeFixed32(buf, v);
  	dst->append(buf, sizeof(U32));
}

void PutVar64 (std::string* dst, U64 v) {
	char buf[10];
	EncodeFixed64(buf,v);
	dst->append(buf,sizeof(U64));
}

void PutLengthPrefixedSlice (std::string* dst, char* value) {
	size_t size=strlen(value);
	PutVar32(dst, size);
	dst->append(value, size);
}

/*
  *****function:put a proposal message into string 
  ***data format:data=epoch+sequence+ktype+key_len+key_data+value_len+value_data 
  */
void EncodeProposal(proposal &pr,std::string* rep) {
	PutVar64(rep,pr.pn.epoch);
	PutVar64(rep,pr.pn.sequence);
	PutLengthPrefixedSlice(rep,pr.kv.kv.key);
	PutLengthPrefixedSlice(rep,pr.kv.kv.value);
}

/*
  *****function:put a fproposal message into string 
  ***data format:data=epoch+sequence+ktype+key_len+key_data+value_len+value_data 
  */
void EncodeFproposal(fproposal &fpr,std::string* rep) {
	PutVar64(rep,fpr.pn.epoch);
	PutVar64(rep,fpr.pn.sequence);
	PutLengthPrefixedSlice(rep,fpr.kv.key);
	PutLengthPrefixedSlice(rep,fpr.kv.value);
}

void DecodeProposal(proposal * pr, Slice *record) {
	U32 key_len,value_len;
	pr->pn.epoch = DecodeFixed64(record->data());
	record->remove_prefix(8);
	pr->pn.sequence = DecodeFixed64(record->data());
	record->remove_prefix(8);
	key_len = DecodeFixed32(record->data());
	record->remove_prefix(4);
	pr->kv.kv.key =  (char *)malloc(key_len+1) ;
	memset(pr->kv.kv.key,0,key_len+1);
	memcpy(pr->kv.kv.key,record->data(),key_len);
	record->remove_prefix(key_len);
	value_len = DecodeFixed32(record->data());	
	record->remove_prefix(4);
	pr->kv.kv.value = (char *) malloc (value_len+1);
	memset(pr->kv.kv.value,0,value_len+1);
	memcpy(pr->kv.kv.value,record->data(),value_len);
}

/*
  *****function:put a filemetadata message into string 
  ***data format:data=refs+filenumber+file_size+smallest+largest 
  */
void EncodeFileMetaData(FileMetaData &f,std::string *rep) {
	PutVar32(rep, f.refs);
	PutVar64(rep, f.number);
	PutVar64(rep, f.file_size);
	PutVar64(rep, f.smallest);
	PutVar64(rep, f.largest);	
	PutVar64(rep, f.epoch);
}

void DecodeFileMetaData(FileMetaData *f,Slice *record) {
	f->refs = DecodeFixed32(record->data());
	record->remove_prefix(4);
	f->number = DecodeFixed32(record->data());
	record->remove_prefix(8);
	f->file_size = DecodeFixed32(record->data());
	record->remove_prefix(8);
	f->smallest = DecodeFixed32(record->data());
	record->remove_prefix(8);
	f->largest = DecodeFixed32(record->data());
	record->remove_prefix(8);
	f->epoch = DecodeFixed32(record->data());
}

/*
  *****function:put a fcatchupitem message into string 
  ***data format:data=epoch+sequence+ktype+key_len+key_data+value_len+value_data
  */
void EncodeFCatchupItem(FCatchupItem &fci,std::string *rep) {
	PutVar64(rep,fci.pn.epoch);
	PutVar64(rep,fci.pn.sequence);
	// rep->push_back(static_cast<char>(kTypeValue));
	PutLengthPrefixedSlice(rep,fci.kv.key);
	PutLengthPrefixedSlice(rep,fci.kv.value);
}

void DecodeCatchupItem(class CatchupItem *citem, Slice *record) {
	U32 key_len,value_len;
	citem->pn.epoch = DecodeFixed64(record->data());
	record->remove_prefix(8);
	citem->pn.sequence = DecodeFixed64(record->data());
	record->remove_prefix(8);
	key_len = DecodeFixed32(record->data());
	record->remove_prefix(4);
	citem->kv.key =  (char *)malloc(key_len+1) ;
	memset(citem->kv.key,0,key_len+1);
	memcpy(citem->kv.key,record->data(),key_len);
	record->remove_prefix(key_len);
	value_len = DecodeFixed32(record->data());	
	record->remove_prefix(4);
	citem->kv.value = (char *) malloc (value_len+1);
	memset(citem->kv.value,0,value_len+1);
	memcpy(citem->kv.value,record->data(),value_len);
	citem->key_len = key_len+1;
	citem->value_len = value_len+1;
}
