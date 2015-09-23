PROG=Exp1.jar
JCLS=Exp1.class
JSRC=Exp1.java
#HADOOP_HOME=../../../hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1
HADOOP_HOME=/home/weijia/ws/hadoop-crtc-src/hadoop-dist/target/hadoop-2.4.1
CLASSPATH="${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.4.1.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.4.1.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-auth-2.4.1.jar:"

$(PROG): $(JSRC)
	javac -cp ${CLASSPATH} $(JSRC)
	jar cvfm $(PROG) Manifest.txt *.class

clean:
	rm $(JCLS) $(PROG)
