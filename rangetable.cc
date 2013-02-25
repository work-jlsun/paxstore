#include <string.h>
#include <stdlib.h>
#include "rangetable.h"
#include "cfgfile.h"
#include "cfg.h"

char * PAXSTORE_CONF_PATH = "/etc/paxstore.conf";

bool RangeItem::init(int range_start, int range_end, std::vector<int> & range_servers,
			std::vector<int> &ports)
{
	start_ = range_start;
	end_ = range_end;
	servers_ = range_servers;
	if (ports.size() != 4){
		return false;	
	}
	leader_write_port_ = ports[0];
	leader_catchup_port_ = ports[1];
	follower_listen_port_ = ports[2];
	read_port_ = ports[3];
	return true;	
}


bool  RangeParser(char *str, int & range_begin, int & range_end ){
	if (NULL == str){
		return false;
	}
	//the first part
	char * begin =  str;
	while ( *str != '\0'){
		if ( ':' == *str){
			break;
		}
		if(*str >= '0' && *str <= '9'  ){
			str++;
			continue;
		}else{
			return false;
		}
	}
	int len = str - begin;
	if (0 == len){
		return false;
	}
	char * part1 = new char[len + 1];
	strncpy(part1, begin, len);
	part1[len] = '\0';
	range_begin = atoi(part1);
	delete[] part1;
	
	//the second part
	if ( '\0' == *str){
		return false;
	}
	
	begin = str +1;
	str = begin;
	while (*str != '\0') {
		if(*str >= '0' && *str <= '9'  ){
			str++;
			continue;
		}else{
			return false;
		}
	}
    	range_end  =  atoi(begin);
	return true;
}

bool  ServerParser(char *str, std::vector<int> &ids ){
	if (NULL == str){
		return false;
	}
	for(;;) {	
		char * begin =  str;
		while ( *str != '\0') {
			if ( ',' == *str ){
				break;
			}
			if(*str >= '0' && *str <= '9'  ){
				str++;
				continue;
			}else{
				return false;
			}
		}
		int len = str - begin;
		if (0 == len){
			break;
		}
		char * part = new char[len + 1];
		strncpy(part, begin, len);
		part[len] = '\0';
		ids.push_back(atoi(part));
		delete[] part;

		if ( '\0' ==*str ){

			break;
		} 	
		str = str + 1;
	}

	if ( ids.size() < 2){
		return false;
	}
	return true;
}

bool  PortParser(char *str, std::vector<int> &ports ){
	if (NULL == str){
		return false;
	}
	for(;;) {	
		char * begin =  str;
		while ( *str != '\0'){
			if ( ',' == *str){
				break;
			}
			if(*str >= '0' && *str <= '9'  ){
				str++;
				continue;
			}else{
				return false;
			}
		}
		int len = str - begin;
		if (0 == len){
			break;
		}
		char * part = new char[len + 1];
		strncpy(part, begin, len);
		part[len] = '\0';
		ports.push_back(atoi(part));
		delete[] part;

		if ( '\0' ==*str ){

			break;
		} 	
		str = str + 1;
	}
	if (ports.size() != 4){
		LOG(ERR, ("the port number is error\n"));
	}
	return true;
}




bool RangeTable::init(void){
	char server_ip_lable[20];
	char* server_ip;
	conf_path = PAXSTORE_CONF_PATH;
	conf_init();
	char * bin_path = conf_get_str("PATH","BINPATH");
	if (!bin_path) {
		return false;
	}
	bin_path_ = bin_path;
	
	char * store_path = conf_get_str("PATH","STOREPATH");
	if (!store_path) {
		return false;
	}
	store_path_ = store_path;
	
	//get the servers ip
	server_num_ = conf_get_num("SERVERS", "NumServer", 0 );
	if ( 0 == server_num_ ){
		return false;	
	}
	
	for (int i = 1; i  != server_num_ + 1; i++) {
		sprintf(server_ip_lable, "SERVER%d_IP", i);
		server_ip = conf_get_str("SERVERS", server_ip_lable);
		if( !server_ip ){
			return false;
		}
		std::string  ip(server_ip);
		server_ips_[i] = ip;
	}

	my_id_ = conf_get_num("SERVERS", "MY_ID", 0 );
	if ( 0 == my_id_ ) {
		return false;
	}
	
	//get the key_range table
	ranger_=  conf_get_num("KEYRANGE", "RANGER", -1);
	if ( -1 == ranger_ ){
		return false;
	}


	range_num_ =  conf_get_num("KEYRANGE", "RANGE_NUM", 0);
	if ( 0 == range_num_ ){
		return false;
	}
	

	for (int i = 1; i != range_num_ + 1; i++){
		char * range_str;
		char * range_servers;
		char * port_str;
		char range_lable[50];
		char range_servers_lable[50];
		char range_ports_lable[50];
		sprintf(range_lable, "RANGE%d", i);
		range_str =  conf_get_str("KEYRANGE", range_lable);
		int range_begin, range_end;
		if ( !RangeParser(range_str, range_begin, range_end) ){

			return false;
		}
		sprintf(range_servers_lable, "RANGE%d_SERVERS", i);
		std::vector<int>  serverids;
		if  ( !ServerParser( conf_get_str("KEYRANGE", range_servers_lable),  serverids) ){
			return false;
		}
		
		sprintf(range_ports_lable, "RANGE%d_PORT", i);
		std::vector<int>  ports;
		if ( !PortParser(conf_get_str("PORT", range_ports_lable), ports) ){
			return false;
		}	
		
		class RangeItem * item = new RangeItem;
		
		item->init(  range_begin, range_end, serverids, ports);
		
		range_items_[i] = *item;
	}
	return true;
}

