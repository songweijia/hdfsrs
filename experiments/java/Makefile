PROG=DataCollector.jar FileTester.jar FilesystemTester.jar
HADOOP_HOME=/opt/hadoop-2.4.1/
CLASSPATH="${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.4.1.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.4.1.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-auth-2.4.1.jar:junit-4.12.jar"

all: $(PROG)

%.jar : %.java
	javac -cp ${CLASSPATH} $^
	jar cvfm $@ $(@:.jar=Manifest.txt) *.class

clean:
	rm $(PROG) *.class
