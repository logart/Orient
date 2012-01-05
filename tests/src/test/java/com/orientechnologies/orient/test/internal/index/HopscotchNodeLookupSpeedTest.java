package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.collection.OMVRBTreeSet;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OHopscotchHashSet;
import com.sun.media.sound.RIFFInvalidDataException;
import org.testng.annotations.Test;

import java.util.Set;

public class HopscotchNodeLookupSpeedTest extends SpeedTestMonoThread
{
  private Set<ORID> hashSet = new OMVRBTreeSet<ORID>();
  private ORID[] rids = new ORID[512];

  public  HopscotchNodeLookupSpeedTest()
  {
    super(100000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    for(int i = 0; i < 512; i++) {
      final ORecordId recordId = new ORecordId(1, i);

      hashSet.add(recordId);
      rids[i] = recordId;
    }
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception
  {
    for(int i = 0; i < 512; i++) {
      hashSet.contains(rids[i]);
    }
  }
}
