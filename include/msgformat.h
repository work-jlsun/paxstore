/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#ifndef __MSGFORMAT__
#define __MSGFORMAT__

/*
*the catchup message formags***********************************************
*/
typedef  struct catchup_header{
	U32 msg_type;
	U32 proposal_numbers;
	U8 endflag;

	void init(U32 mst, U32 pnum, U8 ef){
		msg_type = mst;
		proposal_numbers = pnum;
		endflag = ef;
	}
	bool if_catchup_ended(void){
		if (proposal_numbers == 0 &&  endflag  == 1){
			LOG(HCK, ("endflag received, catchup ok\n"));
			return true;
		}else{
			return false;
		}
	}
}catchup_header;
#endif
