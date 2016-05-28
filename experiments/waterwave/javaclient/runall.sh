#!/bin/bash

m=100
n=100
mstep=10
nstep=10
timesteps=100
writeOp=FFFSwrite
buffersize=1200
writeint=50

if [ $1 == "init" ]; then
	echo "init"
	hadoop dfs -mkdir /data
	hadoop dfs -put tdata /real_data
	hadoop dfsadmin -allowSnapshot /data;
else

	#ssh client1 -f "bash runclient.sh 0 0 10 100" &
	#ssh client2 -f "bash runclient.sh 10 0 20 100" &
	#ssh client3 -f "bash runclient.sh 20 0 30 100" &
	#ssh client4 -f "bash runclient.sh 30 0 40 100" &
	#ssh client5 -f "bash runclient.sh 40 0 50 100" &
	#ssh client6 -f "bash runclient.sh 50 0 60 100" &
	#ssh client7 -f "bash runclient.sh 60 0 70 100" &
	#ssh client8 -f "bash runclient.sh 70 0 80 100" &
	#ssh client9 -f "bash runclient.sh 80 0 90 100" &
	#ssh client10 -f "bash runclient.sh 90 0 100 100" &

	ts=`date +%s.%N | awk '{split($1,s,".");printf("%ld\n",(s[1]+20)*1000 + s[2]/1000000);}'`
	ssh client1 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 0,0,10,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client2 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 10,0,20,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client3 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 20,0,30,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client4 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 30,0,40,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client5 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 40,0,50,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client6 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 50,0,60,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client7 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 60,0,70,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client8 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 70,0,80,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client9 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 80,0,90,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	ssh client10 -f "hadoop jar Client.jar Client -Dtest.buffersize=$buffersize -Dtest.timelimit=$timesteps -Dtest.writeinterval=$writeint 90,0,100,100,$mstep,$nstep,$ts $writeOp /real_data/data" &
	
	echo "ts:$ts"
	if [ $writeOp == "HDFSwrite" ]; then
		frames=$(($timesteps))
                sttime=$(($ts))
		hadoop jar Client.jar Client $sttime,$writeint,$frames snapshot /data;
	fi

	wait
fi
