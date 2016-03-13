#!/bin/bash
for f in \
src/main/native/blog/JNIBlog.c \
src/main/native/blog/JNIBlog.h \
src/main/native/blog/types.h \
src/main/java/edu/cornell/cs/blog/JNIBlog.java \
src/main/java/edu/cornell/cs/blog/RecordParserFactory.java \
src/main/java/edu/cornell/cs/blog/IRecordParser.java \
src/main/java/edu/cornell/cs/blog/DefaultRecordParser.java \
src/main/java/edu/cornell/cs/blog/ts64RecordParser.java \
src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java \
src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java \
src/main/java/org/apache/hadoop/hdfs/BlockReaderFactory.java \
src/main/java/org/apache/hadoop/hdfs/RemoteBlockReader2.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/MemBlockReceiver.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/MemBlockSender.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockSender.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/FsDatasetSpi.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/HLCOutputStream.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/MemDatasetManager.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/MemDatasetImpl.java \
src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl.java \
src/main/java/org/apache/hadoop/hdfs/protocol/ClientDatanodeProtocol.java \
src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/Sender.java \
src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/Receiver.java \
src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/DataTransferProtocol.java \
src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientDatanodeProtocolTranslatorPB.java \
src/main/java/org/apache/hadoop/hdfs/protocolPB/ClientDatanodeProtocolServerSideTranslatorPB.java \
src/main/proto/ClientDatanodeProtocol.proto \
src/main/proto/datatransfer.proto \
src/test/java/org/apache/hadoop/hdfs/server/datanode/SimulatedFSDataset.java \
src/test/java/org/apache/hadoop/hdfs/server/datanode/TestDiskError.java \
src/test/java/org/apache/hadoop/hdfs/TestDataTransferProtocol.java
do
cp ../hadoop-2.4.1-src/hadoop-hdfs-project/hadoop-hdfs/$f ./hadoop-hdfs/$f
done

for f in \
src/main/java/org/apache/hadoop/fs/FileSystem.java
do
cp ../hadoop-2.4.1-src/hadoop-common-project/hadoop-common/$f ./hadoop-common/$f
done
