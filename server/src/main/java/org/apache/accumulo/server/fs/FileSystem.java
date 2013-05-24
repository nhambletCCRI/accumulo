package org.apache.accumulo.server.fs;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public interface FileSystem {
  
  void close() throws IOException;
  
  boolean closePossiblyOpenFile(Path path) throws IOException;
  
  FSDataOutputStream create(Path dest) throws IOException;
  
  FSDataOutputStream create(Path path, boolean b) throws IOException;
  
  FSDataOutputStream create(Path path, boolean b, int int1, short int2, long long1) throws IOException;
  
  boolean createNewFile(Path writable) throws IOException;
  
  FSDataOutputStream createSyncable(Path logPath, int buffersize, short replication, long blockSize) throws IOException;
  
  boolean delete(Path path) throws IOException;
  
  boolean deleteRecursively(Path path) throws IOException;
  
  boolean exists(Path newBulkDir) throws IOException;
  
  FileStatus getFileStatus(Path errorPath) throws IOException;
  
  org.apache.hadoop.fs.FileSystem getFileSystemByPath(Path path);
  
  org.apache.hadoop.fs.FileSystem getFileSystemByPath(String path);
  
  Collection<org.apache.hadoop.fs.FileSystem> getFileSystems();
  
  FileStatus[] listStatus(Path path) throws IOException;
  
  boolean mkdirs(Path directory) throws IOException;
  
  FSDataInputStream open(Path path) throws IOException;
  
  boolean rename(Path path, Path newPath) throws IOException;
  
  boolean moveToTrash(Path sourcePath) throws IOException;
  
  short getDefaultReplication(Path logPath);
  
  boolean isFile(Path path) throws IOException;
  
  boolean isReady() throws IOException;
  
  org.apache.hadoop.fs.FileSystem getDefaultNamespace();
  
  FileStatus[] globStatus(Path path) throws IOException;

  String getFullPath(Key key);
  
}
