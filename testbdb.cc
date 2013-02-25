/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Dang Yongxing
  * email:  china_dyx@163.com
  * date: 2012/11/23
  * function:
*/

#include "log_manager.h"

#include <iostream>

#include "kvproposal.h"

using namespace std;

class P12Info;
/*
class  CatchupItem{
public:
	U8 ifsynced;
	ProposalNum pn;
	U32 key_len;
	U32 value_len;	
	char buf[8050];
	U32 citem_encode_size(void){
		U32 total_size = sizeof(ifsynced) +
					    sizeof(pn.epoch) +
					    sizeof(pn.sequence) +
					    sizeof(key_len)	+
					    sizeof(value_len)	+
					    key_len +
					    value_len;
		return total_size;
	}
};
*/
void readmeta(){
	cout<<"read meta***********\n";
	int ret;
	ProposalNum pn;
	MetaLogStore ml;
	ml.meta_log_do_recovery_ = 1;
	ml.MetaStoreInit();
//	class P12Info pinfo;

	ret =ml.GetCommitPn( 1,   pn);
	if (ret == 0){
		cout << "right:empty\n";
	}
	if (ret == 1){		
		cout <<"(gotit1)"<< pn.epoch << ":" << pn.sequence << endl;
	}
/*
	pinfo.set_epoch(12);
	pinfo.set_commited_sequence(39);
	ml.StoreMetaInfo(1,   pinfo);
	ret = ml.GetCommitPn( 0,   pn);
	if (ret == GETOK){		
		cout <<"(gotit2)"<< pn.epoch << ":" << pn.sequence << endl;
	}

	sleep(2);
	return 0;
	*/
}

void readdata(){
	cout<<"read data***************\n";
	int num = 10;
	Version log;
	log.log_do_recovery = 1;
	proposal  pr;

	log.LogManagerInit();

	pr.pn.epoch = 100;
	pr.pn.sequence = 100;
	pr.kv.kv.key = "key";
	pr.kv.kv.value = "value";
	
	for( int i = 0; i != num; i++ ){
		log.LogAppendProposal(pr);
		pr.pn.sequence++;
	}
	
	ProposalNum pn;
	LogKV  *lkv;
	while(1){
		cout<<"\tinput pn.epoch:";
		scanf("%llu",&pn.epoch);
		cout<<"\tinput pn.sequence:";
		scanf("%llu",&pn.sequence);
		lkv = log.LogGetProposal(pn);
		if (lkv){
			cout << lkv->kv << ":" << (lkv->kv + lkv->key_len) << endl; 
		}
	}
	/*
	LogKV  *lkv;
	for(  int j = 0; j != num; j++ ){
		lkv = log.LogGetProposal(pn);
		if (lkv){
			cout << lkv->kv << ":" << (lkv->kv + lkv->key_len) << endl; 	
		}
		pn.sequence++;
	}
	log.LogManagerShutdown();	
	return 0;
	*/
}

int main(){
	//readmeta();
	readdata();
	return 0;
}


