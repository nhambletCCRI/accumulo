package org.apache.accumulo.server.fs;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


/**
 * This is a glue object, to convert short file references to long references.
 * The !METADATA table may contain old relative file references.  This class keeps 
 * track of the short file reference, so it can be removed properly from the !METADATA table.
 */
public class FileRef implements Comparable<FileRef> {
  String metaReference;  // something like ../2/d-00000/A00001.rf
  Path fullReference;  // something like hdfs://nn:9001/accumulo/tables/2/d-00000/A00001.rf
  
  public FileRef(FileSystem fs, Key key) {
    metaReference = key.getColumnQualifier().toString();
    fullReference = new Path(fs.getFullPath(key));
  }
  
  public FileRef(String metaReference, Path fullReference) {
    this.metaReference = metaReference;
    this.fullReference = fullReference;
  }
  
  public FileRef(String path) {
    this.metaReference = path;
    this.fullReference = new Path(path);
  }
  
  public String toString() {
    return fullReference.toString();
  }
  
  public Path path() {
    return fullReference;
  }
  
  public Text meta() {
    return new Text(metaReference);
  }

  @Override
  public int compareTo(FileRef o) {
    return path().compareTo(o.path());
  }

  @Override
  public int hashCode() {
    return path().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FileRef) {
      return compareTo((FileRef)obj) == 0;
    }
    return false;
  }
  
  
}
