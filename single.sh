#!/bin/bash


bindir=/home/dyx/project/paxstore/
binname=leaderelectiontests
outputfile=output


echo " bindiris: $bindir"

[ -d $bindir ] || exit 0


start(){
	
	if [ -z "$1" ];then
		echo "need an range"
		return
	fi

	pid=$(ps -ef | grep "$binname *$1"  | grep -v grep | awk '{print $2}'| head -n 1 )
		
	if [ -n "$pid" ]; then
		
		echo "the $binname have started pid is $pid"
		return
	fi
		 	
	cd $bindir
	if [ -e $binname ]; then
		outputfilename=${outputfile}_range${1}
		./$binname $1 $2  1>./$outputfilename 2>&1
	else
		echo "$binname is not in dir $bindir"
	fi  
	echo "$binname start completed"

}

stop(){

	if [ -z "$1" ];then
		echo "need an range"
		return
	fi

	pid=$(ps -ef | grep "$binname *$1"  | grep -v grep | awk '{print $2}'| head -n 1 )
	if [ -z "$pid" ]; then
		echo " $binname have stopped"
		return
	fi
	echo $pid
	kill -9 $pid
	echo "$binname stop completed"
}


restart(){
	stop $1  

	sleep 1

	start $1 $2
}








case "$1" in 
	start )
		start $2 $3
		;;	
	stop )
		stop  $2
		;;
	restart )
		restart	$2 $3
		;;
	*)
		echo $"Usage: $0 {start|stop|restart}"
esac














