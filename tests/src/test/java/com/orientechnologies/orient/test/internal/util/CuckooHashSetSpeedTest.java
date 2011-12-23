package com.orientechnologies.orient.test.internal.util;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;
import com.orientechnologies.common.collection.OCuckooHashSet;
import com.orientechnologies.common.collection.OMVRBTreeSet;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OMemoryStream;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 23.12.11
 */
@Test
public class CuckooHashSetSpeedTest extends SpeedTestMonoThread
{
  private OCuckooHashSet hashSet = new OCuckooHashSet( 1024, 10 );

  public CuckooHashSetSpeedTest()
  {
    super(10000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception
  {
    for(int i = 0; i < 512; i++) {
      OMemoryStream memoryStream = new OMemoryStream( 10 );
      memoryStream.set( i );
      memoryStream.set( (short) 1 );

      hashSet.add( memoryStream.getInternalBuffer() );
    }
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception
  {
    for(int i = 0; i < 512; i++) {
      OMemoryStream memoryStream = new OMemoryStream( 10 );
      memoryStream.set( i );
      memoryStream.set( (short) 1 );

      hashSet.contains( memoryStream.getInternalBuffer() );
    }
  }
}