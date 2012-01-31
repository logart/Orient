package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created by IntelliJ IDEA.
 * User: ALoginov
 * Date: 1/30/12
 * Time: 6:18 PM
 * To change this template use File | Settings | File Templates.
 */
@Test
public class HopscotchHashSetAdditionalTest {

  private OHopscotchHashSet hashSet;

  @BeforeMethod
  public void beforeMethod() {
    hashSet = new OHopscotchHashSet(64);
  }

  @Test(enabled = false)
  public void rehashingAfterRemovingBucketCell() {
    ORID[] keys = new ORID[64];
    final int C1 = 543;

    for (int i = 0; i < 64; i ++){
      keys[i] = new ORecordId((C1 - i) - 31, 31 * i);
    }

    for (int i = 0; i < 64; i ++){
      Assert.assertTrue(hashSet.add(keys[i]), "Key " + keys[i] + " was not added" );
    }

    Assert.assertTrue(hashSet.remove(keys[0]), "Key " + keys[0] + " was not removed" );

    Assert.assertTrue(hashSet.add(keys[0]), "Key " + keys[0] + " was not added" );

  }

  @Test(enabled = false)
  public void rehashing() {
    ORID[] keys = new ORID[256];
    final int C1 = 543;
    final int C2 = 544;
    final int C3 = 545;
    final int C4 = 546;

    for (int i = 0; i < 64; i ++){
      keys[i] = new ORecordId((C1 - i) - 31, i);
    }

    for (int i = 64; i < 128; i ++){
      keys[i] = new ORecordId((C2 - i) - 31, 31 * i);
    }

    for (int i = 128; i < 192; i ++){
      keys[i] = new ORecordId((C3 - i) - 31, 31 * i);
    }

    for (int i = 192; i < 256; i ++){
      keys[i] = new ORecordId((C4 - i) - 31, 31 * i);
    }

    for (ORID key : keys){
      Assert.assertTrue(hashSet.add(key), "Key " + key + " was not added" );
    }

//    Assert.assertTrue(hashSet.remove(keys[0]), "Key " + keys[0] + " was not removed" );
//
//    Assert.assertTrue(hashSet.add(keys[0]), "Key " + keys[0] + " was not added" );

  }
}
