package com.orientechnologies.orient.test.internal.index;

import java.util.Set;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OHopscotchHashSet;
import org.testng.annotations.Test;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 23.12.11
 */
public class HopscotchNodeCreationSpeedTest extends SpeedTestMonoThread
{
  private Set<ORID> hashSet = new OHopscotchHashSet(512);

  public HopscotchNodeCreationSpeedTest()
  {
    super(100000);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception
  {
    hashSet = new OHopscotchHashSet(512);
    for(int i = 0; i < 512; i++) {
      final ORecordId recordId = new ORecordId(1, i);

      hashSet.add(recordId);
    }
  }
}