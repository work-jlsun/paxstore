/*
  * Copyright (c) 2012 The PAXSTORE Authors. All rights reserved.
  * author: Sun Jianliang
  * email: 
  * date: 2012/11/23
  * function:
*/

#ifndef __PQUEUE__
#define __PQUEUE__
#include <pthread.h>

#include "kvproposal.h"
#include "metalogstore.h"

class LeaderFailover;
class CatchupItem;
class ClientKeyValue;

extern class P12Info  Global_P12Info;
extern class ProposalQueue  Global_ProposalQueue;

typedef struct CmtRange{
	U64 from;
	U64 to;
}CmtRange;

enum pstate{
	initial,
	synced,
	commmited
};

class QProposal{
public:
	QProposal(class ClientKeyValue*, U64);
	//QProposal(proposal&);
	QProposal();
	virtual ~QProposal();
	void Init(class ClientKeyValue*, U64);
	void Clear(void);
	void SetTimeOut(unsigned int usec);
	bool IsTimeOut(void);
	void SetPromise(unsigned int mid);	//machine id
	void SetState(pstate ps){
		state_ = ps;
	}
	unsigned int promise_num(void){
		return promise_cnt_;
	}
	proposal & pinstance(void){
		return pinstance_;
	}
	proposal * ppinstance(void){   
		return &pinstance_;
	}
private:
	proposal pinstance_;	//the value
	unsigned int promise_cnt_;
	unsigned int promise_bitmap_;
	 pstate state_; //this proposal's state
	struct timeval timeout_; //used for timeout retransmission	
	//struct event timoutevent;
	DISALLOW_COPY_AND_ASSIGN(QProposal);
};

/*
 * set when get a promise frome the proposal
 *
 *
 */
inline void  QProposal::SetPromise(unsigned int follower_id){
	if(promise_bitmap_ & 1 << follower_id){
		//drop duplicated promise
		return;  	
	}else{
		promise_bitmap_ |= (1 << follower_id);
		promise_cnt_++;	
	}
}

class ProposalQueue {
public:
	ProposalQueue();
	virtual ~ProposalQueue();
	void AllClear(void){}	//no needed ,because the QProposal has it's own construct and destruct	
	unsigned int GetQproposalOffset(U64);
	class QProposal * GetQproposal(U64 sequence){
			unsigned int  index = GetQproposalOffset(sequence);
			return &(qp_[index]);
	}
	bool GetQproposal(class CatchupItem *citem);
private:
	class QProposal qp_[PROPOSAL_QUEUE_SIZE];
	pthread_rwlock_t	plock_;	//is is a user-producer lock,to protect the buffer
};

inline unsigned int ProposalQueue::GetQproposalOffset(U64 sequence){
	return sequence % PROPOSAL_QUEUE_SIZE;
}


class P12Info{
	friend class LeaderFailover;
public:
	P12Info();
	virtual ~P12Info(){}
	U64 epoch(void);
	void set_epoch(U64);
	U64 next_unused_sequence(void);
	void inc_next_unused_sequence(void);
	bool update_lsn_sequence(U64,U64);
	U64 lsn_sequence(void){
		return __sync_fetch_and_add(&lsn_sequence_,0);
	}
	//use this carefully this can only can be used in the only write thread
	U64 get_commited_sequence_unlocked(void){
		LOG(VRB,("cmt_sequence%llu\n",commited_sequence_));
		return commited_sequence_;
	}
	U64 get_commited_sequence_locked(void){
		return __sync_fetch_and_add(&commited_sequence_,0);
	}
	
	CmtRange update_commited_sequence(U64, U64);
	CmtRange update_localdelay_commited_sequence(void);

	//ensure that only one thread can use this function in the  same time
	void __update_commited_sequence(U64 cmt){
		U64 Dval;
		ProposalNum pn;
		pn.epoch = epoch_;
		pn.sequence = cmt;
		if (cmt <= commited_sequence_){
			return;
		}else{
			Dval = cmt -commited_sequence_;
			__sync_fetch_and_add(&commited_sequence_, Dval);

			//mlstore.StoreMetaInfo(range_num(),pn);	//modify by Dang Yongxing

		}
	}
	U64 get_last_synced_cmt_sequence_unlocked(void){
		return  last_synced_cmt_sequence_;
	}
	U64 get_last_synced_cmt_sequence_locked(void){
		return  __sync_fetch_and_add(&last_synced_cmt_sequence_,0);
	}
	//ensure that only one thread can use this function in the  same time
	void update_last_synced_cmt_sequence(U64 synced_sequence){
		U64 Dval;
		if(synced_sequence <= last_synced_cmt_sequence_){
			LOG(ERR,("oh no , what is wrong\n"));
			return;
		}else{
			Dval = synced_sequence - last_synced_cmt_sequence_;
			__sync_fetch_and_add(&last_synced_cmt_sequence_, Dval);
		}
	}
	
	bool update_highest_promise_sequence(U64);
	U64 get_highest_promise_sequence(void){
		return __sync_fetch_and_add(&highest_promise_sequence_,0);
	}
	unsigned int open_cnt(void){
		//return __sync_fetch_and_add(&open_cnt,0);
		return open_cnt_;
	}
	void inc_open_cnt(void){
		// __sync_fetch_and_add(&open_cnt,1);
		open_cnt_++;
		LOG(VRB, ("open cnt:%d\n",open_cnt_));
	}
	void dec_open_cnt(void){
		// __sync_fetch_and_add(&open_cnt,1);
		open_cnt_--;
		LOG(VRB, ("open cnt:%ul\n",open_cnt_));
	}		
	U32 range_num(void){
		//fixme
		return 1;
	}
private:
	U64 epoch_;
	U64 next_unused_sequence_;
	U64 commited_sequence_;			// who will update
	U64 last_synced_cmt_sequence_;			//the cmt_sequece that have send to the follower 
	U64 lsn_sequence_;				// who will update
	U64 highest_promise_sequence_;	// who will updata
	unsigned int open_cnt_;
	U32 range_num_;  //the key range_num
};

inline P12Info::P12Info(){
	//fix me here, when in leader failer over,or follower failer over,it will be different
	epoch_ = 0;
	next_unused_sequence_ = 1;	//the sequence is from one
	commited_sequence_ = 0;
	last_synced_cmt_sequence_ = 0;
	lsn_sequence_ = 0;
	highest_promise_sequence_ = 0;
	open_cnt_ = 0;
}

inline U64 P12Info::epoch(void){
		return epoch_;
}

inline void P12Info::set_epoch(U64 new_epoch){
		epoch_ = new_epoch;	
}

/*
	log_writer: (lsn + 1) ==  __sync_fetch_and_add(&next_unused_sequence,0), 
*/

inline U64 P12Info::next_unused_sequence(void){
		//U64 ret;
		//ret  = next_unused_sequence;
		//next_unused_sequence++;  the add will be acctually added when a proposal is enter the	proposal_queue 
		//return ret;
		return __sync_fetch_and_add(&next_unused_sequence_, 0);
}

inline void P12Info::inc_next_unused_sequence(void){
	__sync_fetch_and_add(&next_unused_sequence_,1);
}


/*
 *log disk thread commit the log one by one   
 *
 *
 */
inline bool P12Info::update_lsn_sequence(U64 vepoch, U64 vsequence){
		if (vepoch == epoch_){
			if (vsequence  == lsn_sequence_ + 1){
				 __sync_fetch_and_add(&lsn_sequence_,1);
			}else{
					assert(0);
			}
		}
		if (vepoch !=  epoch_ ){		//fixed me
			if (vsequence  == lsn_sequence_ + 1){
				//lsn_sequence++;
				 __sync_fetch_and_add(&lsn_sequence_,1);
			}else{
					assert(0);
			}
		}
		return true;
}

inline CmtRange P12Info::update_commited_sequence(U64 vepoch, U64 vsequence){
		CmtRange cr;
		if ( vepoch == epoch_ ){
			//fixed me acctually this shoud be used in the timeout function, not here,whan revice a  propossl response
			U64 tmp;
			if ( highest_promise_sequence_ >=  (tmp = __sync_fetch_and_add( &lsn_sequence_, 0 ) )){	//this is to forbid the vsequence is bigger than ths lsn at that time ,but
				if (commited_sequence_ <= tmp ){																			//after recive this message ,no other messages,
					cr.from = commited_sequence_;   //(cr.from,cr.to ]
					cr.to = tmp;
					//commited_sequence = tmp;
					__update_commited_sequence(tmp);

				}else{
					assert(0);		//commit sequence will not bigger than lsn_seequence
					cr.from = commited_sequence_;	  // no newly commited
					cr.to = commited_sequence_;					
				}
			//when the sequence cache up, will shoud update the commit
			}else{
				if( commited_sequence_ <=  highest_promise_sequence_){
					cr.from = commited_sequence_;   //(cr.from,cr.to ]
					cr.to = highest_promise_sequence_;
					//commited_sequence = highest_promise_sequence;
					__update_commited_sequence(highest_promise_sequence_);
				}else{
					assert(0);
					cr.from = commited_sequence_;
					cr.to = commited_sequence_;
				}
			}
			
			if (vsequence  > 	commited_sequence_ ){	
				U64 tmp_lsn = __sync_fetch_and_add( &lsn_sequence_, 0 );
				if (vsequence <= tmp_lsn){	//fix me, the lsn_sequence is a race var /with log bdb thread
						cr.to = vsequence;
						//commited_sequence = vsequence ;
						__update_commited_sequence(vsequence);
				}else if( vsequence >  tmp_lsn ){     //here the vsequence  log entry have get a promise, if the log writer commit this sequence ,it is ok now 
						cr.to = tmp_lsn;
						assert(cr.from <= cr.to);
						//commited_sequence = tmp_lsn;
						__update_commited_sequence(tmp_lsn);
						//update_highest_promise_sequence(vsequence);
				}
			}else{
				
			}

		}else{		//fixed me , leadder  shutdown, epoch will be bigger
			assert(0); //
		}
		return cr;
}

inline CmtRange P12Info::update_localdelay_commited_sequence(void) {
	U64 tmp;
	CmtRange cr;
	if ( highest_promise_sequence_ >=  (tmp = __sync_fetch_and_add( &lsn_sequence_, 0 ) )){	
		if (commited_sequence_ < tmp ){																		
			cr.from = commited_sequence_;   //(cr.from,cr.to ]
			cr.to = tmp;
			//commited_sequence = tmp;
			__update_commited_sequence(tmp);
		}else{
			cr.from = commited_sequence_;	  // no newly commited
			cr.to = commited_sequence_;					
		}
	//when the sequence cache up, will shoud update the commit
	}else{
		if( commited_sequence_ <  highest_promise_sequence_){
			cr.from = commited_sequence_;   //(cr.from,cr.to ]
			cr.to = highest_promise_sequence_;
			//commited_sequence = highest_promise_sequence;
			__update_commited_sequence(highest_promise_sequence_);
		}else{
			cr.from = commited_sequence_;
			cr.to = commited_sequence_;
		}
	}
	return cr;
}
inline bool P12Info::update_highest_promise_sequence(U64 vsequence){
		U64 delt; 
		if(vsequence > highest_promise_sequence_){
				delt = vsequence - highest_promise_sequence_;
				// highest_promise_sequence = vsequence;
				 __sync_fetch_and_add(&highest_promise_sequence_,delt);
		}
		return true;
}
#endif
