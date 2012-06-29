package com.orientechnologies.orient.core.storage.impl.local;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 29.06.12
 */
public interface ORecordMemoryCacheFlusher {
  public void flushRecord(int clusterId, long clusterPosition, int dataSegmentId, byte[] content,
      ORecordMemoryCache.RecordState recordState);
}
