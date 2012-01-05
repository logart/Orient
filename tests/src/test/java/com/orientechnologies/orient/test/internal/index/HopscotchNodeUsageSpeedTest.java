package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.collection.OMVRBTreeSet;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OHopscotchHashSet;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.Set;

public class HopscotchNodeUsageSpeedTest extends SpeedTestMonoThread
{
  private Set<ORID> hashSet = new OMVRBTreeSet<ORID>();

  public HopscotchNodeUsageSpeedTest()
  {
    super(100000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    for(int i = 0; i < 512; i++) {
      final ORecordId recordId = new ORecordId(1, i);

      hashSet.add(recordId);
    }
  }


  @Override
  @Test(enabled = false)
  public void cycle() throws Exception
  {
    final Random random = new Random();
    final double action = random.nextDouble();
    ORID ridToAdd = null;
    ORID ridToRemove = null;

    if(action <= 0.2) {
      ridToAdd = new ORecordId(1, random.nextLong());
      hashSet.add(ridToAdd);

      if(ridToRemove != null)
        hashSet.remove(ridToRemove);

      ridToRemove = ridToAdd;
    } else {
      ORID rid = new ORecordId(1, random.nextLong());
      hashSet.contains(rid);
    }
  }
}