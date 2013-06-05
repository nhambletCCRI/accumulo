package org.apache.accumulo.server.fs;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * A wrapper around multiple hadoop FileSystem objects, which are assumed to be different namespaces.
 */
public interface FileSystem {
  
  // close the underlying FileSystems
  void close() throws IOException;
  
  // the mechanism by which the master ensures that tablet servers can no longer write to a WAL
  boolean closePossiblyOpenFile(Path path) throws IOException;
  
  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path dest) throws IOException;
  
  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path path, boolean b) throws IOException;
  
  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path path, boolean b, int int1, short int2, long long1) throws IOException;
  
  // create a file, but only if it doesn't exist
  boolean createNewFile(Path writable) throws IOException;
  
  // create a file which can be sync'd to disk
  FSDataOutputStream createSyncable(Path logPath, int buffersize, short replication, long blockSize) throws IOException;
  
  // delete a file
  boolean delete(Path path) throws IOException;
  
  // delete a directory and anything under it
  boolean deleteRecursively(Path path) throws IOException;
  
  // forward to the appropriate FileSystem object
  boolean exists(Path path) throws IOException;
  
  // forward to the appropriate FileSystem object
  FileStatus getFileStatus(Path path) throws IOException;
  
  // find the appropriate FileSystem object given a path
  org.apache.hadoop.fs.FileSystem getFileSystemByPath(Path path);
  
  org.apache.hadoop.fs.FileSystem getFileSystemByPath(String path);
  
  // get a mapping of namespace to FileSystem
  Map<String, ? extends org.apache.hadoop.fs.FileSystem> getFileSystems();
  
  // return the item in options that is in the same namespace as source
  Path matchingFileSystem(Path source, String[] options);
  
  // create a new path in the same namespace as the sourceDir
  String newPathOnSameNamespace(String sourceDir, String suffix);
  
  // forward to the appropriate FileSystem object
  FileStatus[] listStatus(Path path) throws IOException;
  
  // forward to the appropriate FileSystem object
  boolean mkdirs(Path directory) throws IOException;
  
  // forward to the appropriate FileSystem object
  FSDataInputStream open(Path path) throws IOException;
  
  // forward to the appropriate FileSystem object, throws an exception if the paths are in different namespaces
  boolean rename(Path path, Path newPath) throws IOException;
  
  // forward to the appropriate FileSystem object
  boolean moveToTrash(Path sourcePath) throws IOException;
  
  // forward to the appropriate FileSystem object
  short getDefaultReplication(Path logPath);
  
  // forward to the appropriate FileSystem object
  boolean isFile(Path path) throws IOException;
  
  // all namespaces are ready to provide service (not in SafeMode, for example)
  boolean isReady() throws IOException;
  
  // ambiguous references to files go here
  org.apache.hadoop.fs.FileSystem getDefaultNamespace();
  
  // forward to the appropriate FileSystem object
  FileStatus[] globStatus(Path path) throws IOException;

  // Convert a file or directory !METADATA reference into a path
  String getFullPath(Key key);
  
  // Given a filename, figure out the qualified path given multiple namespaces
  String getFullPath(String paths[], String fileName) throws IOException;

  // forward to the appropriate FileSystem object
  ContentSummary getContentSummary(String dir);
}
