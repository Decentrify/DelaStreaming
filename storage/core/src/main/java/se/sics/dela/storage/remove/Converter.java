package se.sics.dela.storage.remove;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.dela.storage.StreamStorage;
import se.sics.dela.storage.disk.DiskEndpoint;
import se.sics.dela.storage.disk.DiskResource;
import se.sics.dela.storage.gcp.GCPEndpoint;
import se.sics.dela.storage.gcp.GCPResource;
import se.sics.dela.storage.hdfs.HDFSEndpoint;
import se.sics.dela.storage.hdfs.HDFSResource;
import se.sics.nstream.StreamId;
import se.sics.nstream.storage.durable.util.MyStream;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Converter {

  public static Set<Pair<StreamId, StreamStorage>> streams(List<Pair<StreamId, MyStream>> vals) {
    Set<Pair<StreamId, StreamStorage>> result = new HashSet<>();
    vals.forEach((val) -> result.add(stream(val)));
    return result;
  }
  
  public static Pair<StreamId, StreamStorage> stream(Pair<StreamId, MyStream> val) {
    return Pair.with(val.getValue0(), stream(val.getValue1()));
  }
  
  public static StreamStorage stream(MyStream val) {
    if (val.resource instanceof se.sics.nstream.hops.storage.disk.DiskResource) {
      se.sics.nstream.hops.storage.disk.DiskResource r
        = (se.sics.nstream.hops.storage.disk.DiskResource) val.resource;
      DiskEndpoint endpoint = new DiskEndpoint();
      DiskResource resource = new DiskResource(r.dirPath, r.fileName);
      return new StreamStorage(endpoint, resource);
    } else if (val.resource instanceof se.sics.nstream.hops.storage.hdfs.HDFSResource) {
      se.sics.nstream.hops.storage.hdfs.HDFSEndpoint e
        = (se.sics.nstream.hops.storage.hdfs.HDFSEndpoint) val.endpoint;
      se.sics.nstream.hops.storage.hdfs.HDFSResource r
        = (se.sics.nstream.hops.storage.hdfs.HDFSResource) val.resource;
      HDFSEndpoint endpoint = new HDFSEndpoint(e.hdfsConfig, e.user);
      HDFSResource resource = new HDFSResource(r.dirPath, r.fileName);
      return new StreamStorage(endpoint, resource);
    } else if (val.resource instanceof se.sics.nstream.hops.storage.gcp.GCPResource) {
      se.sics.nstream.hops.storage.gcp.GCPEndpoint e
        = (se.sics.nstream.hops.storage.gcp.GCPEndpoint) val.endpoint;
      se.sics.nstream.hops.storage.gcp.GCPResource r
        = (se.sics.nstream.hops.storage.gcp.GCPResource) val.resource;
      GCPEndpoint endpoint = new GCPEndpoint(e.credentials, e.projectName);
      GCPResource resource = new GCPResource(r.libDir, r.relativePath, r.file);
      return new StreamStorage(endpoint, resource);
    } else {
      throw new RuntimeException("unhandled stream:" + val);
    }
  }
}
