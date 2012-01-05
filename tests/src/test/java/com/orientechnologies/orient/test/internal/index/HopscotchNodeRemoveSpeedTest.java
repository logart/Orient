package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.collection.OMVRBTreeSet;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import org.testng.annotations.Test;

import java.util.Set;

public class HopscotchNodeRemoveSpeedTest extends SpeedTestMonoThread
{
  private Set<ORID> hashSet = new OMVRBTreeSet<ORID>();

  public HopscotchNodeRemoveSpeedTest()
  {
    super(100000);
  }


  @Override
  @Test(enabled = false)
  public void beforeCycle() throws Exception {
    for(int i = 0; i < 512; i++) {
      final ORecordId recordId = new ORecordId(1, i);

      hashSet.add(recordId);
    }
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception
  {
    for (int i = 0; i < 512; i++) {
      final ORecordId recordId = new ORecordId(1, i);

      hashSet.remove(recordId);
    }
  }
}
