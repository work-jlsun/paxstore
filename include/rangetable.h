#ifndef	__RANGETABLE_H_
#define __RANGETABLE_H_
#include <vector>
#include <map>
#include <string>

class RangeTable;

class RangeItem{
public:
	bool init(int range_start, int range_end, std::vector<int> &range_servers, 
			std::vector<int> &ports);
	int start_;
	int end_;
	std::vector<int> servers_;   //store the id of the server	
	int leader_write_port_;	//the port for leader to receive write
	int leader_catchup_port_; //leader to wait for follower do recovery
	int follower_listen_port_;  //follower to receive data from leader 
	int read_port_;		// the leader and follower all use this port to provide read to the client  	
};

struct CmdItem{
	int id;
	int range;
};

class RangeTable{
friend  void getmachineinstances(class RangeTable &, std::map <int, std::vector <int> > & );
friend  void genstartcmds(class RangeTable &, std::vector< struct CmdItem> & );
public:
	RangeTable(){};
	bool init(void);
	~RangeTable(){};
	bool GetServerIP(int id, std::string &ip){
		if(server_ips_.find(id) == server_ips_.end()){
			return false;	
		}
		ip = server_ips_[id];
		return true;
	}	
	std::string BinPath(void){
		return bin_path_;	
	}
	std::string StorePath(void){
		return store_path_;
	}
	int my_id(void){
		return my_id_;	
	}

	int get_range_num(){
		return range_num_;
	}

	class RangeItem GetRangeItem(int range_id){
		return range_items_[range_id];
	}
private:
	std::string bin_path_;
	std::string store_path_; 	
	int server_num_;
	std::map<int, std::string>  server_ips_;

	int my_id_;
	
	int ranger_; //represent the biggest hash value
	int range_num_;
	
	std::map<int, class RangeItem>  range_items_;	
	
};

#endif

