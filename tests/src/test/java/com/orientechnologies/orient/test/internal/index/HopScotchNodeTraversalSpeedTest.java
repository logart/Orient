package com.orientechnologies.orient.test.internal.index;


import com.orientechnologies.common.collection.OMVRBTreeSet;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OHopscotchHashSet;
import org.testng.annotations.Test;

import java.util.Set;

public class HopScotchNodeTraversalSpeedTest extends SpeedTestMonoThread
{
  private Set<ORID> hashSet = new OHopscotchHashSet(512);
  private ORID[] rids = new ORID[512];

  public  HopScotchNodeTraversalSpeedTest()
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
    for (final ORID rid : hashSet) {
      final ORID oridOne = rid;
    }
  }
}
