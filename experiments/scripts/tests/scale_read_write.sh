#!/bin/bash
# test scalability performance

tester_jar=FileTester.jar
result_folder=scale_read_write
current_folder=`pwd`
deploy_folder=../deploy/
number_loop=1
# how long we wait before we start run the client
wait_secs=30
#data_file_size=1073741824
data_file_size=67108864
transfer_size=131072
number_threads=4
estimated_secs=10

# STEP 0 checks
if [[ ! -e $tester_jar ]]; then
  echo "$tester_jar not found, check <prj>/experiment/java"
  exit -1;
fi

cd $deploy_folder
source env.sh
cd $current_folder

# STEP 1 create the result folder
rm -rf $result_folder
mkdir $result_folder

# STEP 2 upload test jar to clients
uploadClients $tester_jar $tester_jar

# STEP 3 function scalable read and write test
# scale_write_exp
function scale_write_exp
{
  exp=$1
  cfg=$2
  r_folder=$3
  file_size=$4
  write_size=$5
  num_threads=$6

  for block_size in 64M # block size
  do
    for page_size in 4096 # page size
    do
      for((i=0;i<$number_loop;i++))
      do
        for packet_size in 131072
        do
          ## prepare the environment.
          cd $deploy_folder
          ./prepare.sh $exp $cfg $block_size $packet_size $page_size
          cd $current_folder

          ## start the clients.
          start_time=`expr \`date +%s\` \+ ${wait_secs}`

          ## write test.
          echo -e "Header\tnum_client_hosts\tnum_clients_per_host\tfile_size write_size" >> ${r_folder}/wthp$i
          echo -e "${num_clients}\t${num_threads}\t${file_size}\t${write_size}" >> ${r_folder}/wthp$i
          for cl in ${clients}
          do
            ssh ${cl} "nohup hadoop jar ${tester_jar} syncmultiwrite /${cl} ${start_time} ${file_size} ${write_size} ${num_threads} > ${tester_jar}.out 2>${tester_jar}.err < /dev/null &"
          done

          ## wait
          sleep $wait_secs
          tmpfile='tmpfile'
          touch ${tmpfile}
          echo "${tmpfile} expr ${num_threads} \* ${num_clients}"
          ## try collecting results
          while [[ `wc -l < ${tmpfile}` -lt `expr ${num_threads} \* ${num_clients}` ]]
          do
            sleep $estimated_secs
            rm ${tmpfile}
            ## message
            echo "Trying collecting write results..."
            ## download it
            for cl in ${clients}
            do
              echo "rsync ${cl}:${tester_jar}.out ${tester_jar}.out"
              rsync ${cl}:${tester_jar}.out ${tester_jar}.out
              sleep 1
              if [[ $? -ne 0 ]]; then
                echo "error"
                break;
              else
                echo "ok"
                cat ${tester_jar}.out >> ${tmpfile}
                rm ${tester_jar}.out
              fi
            done
            ## info
            wc -l < ${tmpfile}
            expr ${num_threads} \* ${num_clients}
          done

          cat ${tmpfile} >> ${r_folder}/wthp$i
          rm -rf ${tmpfile}

          ## clean up client
          for cl in ${clients}
          do
            ssh ${cl} "rm ${tester_jar}.out ${tester_jar}.err"
          done

        done
      done
    done
  done
}

# scale_read_exp
function scale_read_exp
{
  exp=$1
  cfg=$2
  r_folder=$3
  read_size=$4
  num_threads=$5

  for block_size in 64M # block size
  do
    for page_size in 4096 # page size
    do
      for((i=0;i<$number_loop;i++))
      do
        for packet_size in 131072
        do

          ## start the clients.
          start_time=`expr \`date +%s\` \+ ${wait_secs}`

          ## read test.
          for cl in ${clients}
          do
            if [[ $cfg == rdma ]]; then
              ssh ${cl} "nohup hadoop jar ${tester_jar} syncmultizerocopyread /${cl} ${start_time} ${read_size} ${num_threads} > ${tester_jar}.out 2>${tester_jar}.err < /dev/null &"
            else
              ssh ${cl} "nohup hadoop jar ${tester_jar} syncmultiread /${cl} ${start_time} ${read_size} ${num_threads} > ${tester_jar}.out 2>${tester_jar}.err < /dev/null &"
            fi
          done

          ## wait
          sleep $wait_secs
          tmpfile='tmpfile'
          touch ${tmpfile}
          ## try collecting results
          while [[ `wc -l < ${tmpfile}` -lt `expr ${num_threads} \* ${num_clients}` ]]
          do
            sleep $estimated_secs
            rm ${tmpfile}
            ## message
            echo "Trying collecting read results..."
            ## download it
            for cl in ${clients}
            do
              echo "rsync ${cl}:${tester_jar}.out ${tester_jar}.out"
              rsync ${cl}:${tester_jar}.out ${tester_jar}.out
              sleep 1
              if [[ $? -ne 0 ]]; then
                echo "error"
                break;
              else
                echo "ok"
                cat ${tester_jar}.out >> ${tmpfile}
                rm ${tester_jar}.out
              fi
            done
            ## info
            wc -l < ${tmpfile}
            expr ${num_threads} \* ${num_clients}
          done

          cat ${tmpfile} >> ${r_folder}/rthp$i
          rm -rf ${tmpfile}

          ## clean up client
          for cl in ${clients}
          do
            ssh ${cl} "rm ${tester_jar}.out ${tester_jar}.err"
          done
        done
      done
    done
  done
}


# STEP 4 run experiment
# - FFFS TCP
# mkdir -p ${result_folder}/fffs.tcp
# scale_write_exp fffs tcp ${result_folder}/fffs.tcp ${data_file_size} ${transfer_size} ${number_threads}
# scale_read_exp fffs tcp ${result_folder}/fffs.tcp ${transfer_size} ${number_threads}
# - FFFS RDMA
mkdir -p ${result_folder}/fffs.rdma
scale_write_exp fffs rdma ${result_folder}/fffs.rdma ${data_file_size} ${transfer_size} ${number_threads}
# scale_read_exp fffs rdma ${result_folder}/fffs.rdma ${transfer_size} ${number_threads}
# - HDFS ORG
#mkdir -p ${result_folder}/hdfs.org
#scale_write_exp hdfs org ${result_folder}/hdfs.org ${data_file_size} ${transfer_size} ${number_threads}
#scale_read_exp hdfs org ${result_folder}/hdfs.org ${transfer_size} ${number_threads}

# STEP 5 remove the garbage
runClients rm $tester_jar
