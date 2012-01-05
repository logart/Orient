package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 22.12.11
 */
@Test
public class HopscotchHashSetTest
{
  private OHopscotchHashSet hashSet;

  @BeforeMethod
  public void beforeMethod() {
    hashSet = new OHopscotchHashSet(64);
  }

  @Test(dataProvider = "testKeys")
  public void testAddition(ORID[] keys, ORID[] absentKeys) {
    for(int i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( hashSet.size(), i + 1 );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " is absent" );
    }

    for( final ORID key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key + " is absent" );
    }

    for( final ORID absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.contains( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testValueAdditionValueIsContainedInSet(ORID[] keys, ORID[] absentKeys) {
    for(int i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertFalse( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " is absent" );
    }

    for( final ORID key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key + " is absent" );
    }

    for( final ORID absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.contains( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testValueRemoveValue(ORID[] keys, ORID[] absentKeys) {
    for(int i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertTrue( hashSet.remove( keys[i] ), "Key " + keys[i] + " was not removed" );
      Assert.assertEquals( i , hashSet.size() );


      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not removed" );

      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );
    }

    for( final ORID absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.remove( absentKey ) );
    }

    for(int i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.remove( keys[i] ), "Key " + keys[i] + " was not removed" );
      Assert.assertEquals( keys.length - i - 1, hashSet.size() );
    }

    for( final ORID key : keys ) {
      Assert.assertFalse( hashSet.contains( key ), "Key " + key + " was not removed" );
    }

    for( final ORID absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.remove( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testClear(ORID[] keys, ORID[] absentKeys) {
    for( final ORID key : keys ) {
      Assert.assertTrue( hashSet.add( key ) );
    }

    hashSet.clear();

    Assert.assertEquals( hashSet.size(), 0 );

    for( final ORID key : keys ) {
      Assert.assertFalse( hashSet.contains( key ), "Key " + key + " was not removed" );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testIterator(ORID[] keys, ORID[] absentKeys) {
    List<ORID> addedKeys = new ArrayList<ORID>(  );
    for( final ORID key : keys ) {
      Assert.assertTrue(hashSet.add(key));
      addedKeys.add( key );
    }

    Iterator<ORID> iterator = hashSet.iterator();
    for(int i = 0; i < keys.length; i++) {
      Assert.assertTrue( iterator.hasNext(), i +  "-th item is absent in iterator" );

      ORID key = iterator.next();

      Assert.assertTrue( addedKeys.remove( key ), "Key " + key + " was not removed.");
    }

    Assert.assertFalse( iterator.hasNext(), "Iterator contains more items than expected" );

    try {
      iterator.next();
      Assert.fail();
    } catch( NoSuchElementException e){
    }
  }

  @Test(dataProvider = "testKeys")
  public void testIteratorRemove(ORID[] keys, ORID[] absentKeys) {
    for( final ORID key : keys ) {
      Assert.assertTrue( hashSet.add( key ) );
    }

    Iterator<ORID> iterator = hashSet.iterator();
    iterator.next();

    try {
      iterator.remove();
      Assert.fail();
    } catch(  UnsupportedOperationException e ){
    }
  }

  @Test
  public void testCapacityCalculationOne() {
    final OHopscotchHashSet hopScotchHashSet = new OHopscotchHashSet( 3 );
    Assert.assertEquals( hopScotchHashSet.capacity(), 64 );
  }

  @Test
  public void testCapacityCalculationTwo() {
    final OHopscotchHashSet hopScotchHashSet = new OHopscotchHashSet( 65 );
    Assert.assertEquals( hopScotchHashSet.capacity(), 128 );
  }

  @DataProvider(name = "testKeys")
  public Object[][] testKeys() {
    ORID[] keysOne = new ORID[128];
    ORID[] absentKeysOne = new ORID[128];
    for(int i = 0; i < 128; i++) {
      keysOne[i] = new ORecordId(i , (i + 1));
    }

    for(int i = 128; i < 256; i++) {
      absentKeysOne[i - 128] = new ORecordId(i , (i + 1));
    }

    ORID[] keysTwo = new ORID[64];
    ORID[] absentKeysTwo = new ORID[64];

    for(int i = 0; i < 64; i++) {
      keysTwo[i] = new ORecordId(i* 2 , (i * 2 + 1));
    }
    for(int i = 64; i < 128; i++) {
      absentKeysTwo[i -  64] = new ORecordId(i* 2 , (i * 2 + 1));
    }

    ORID[] keysThree = new ORID[20];
    ORID[] absentKeysThree = new ORID[20];

    for(byte i = 0; i < 20; i++) {
      keysThree[i] = new ORecordId(i * 2, i + 1);
    }
    for(byte i = 20; i < 40; i++) {
      absentKeysThree[i - 20] = new ORecordId(i * 2, i + 1);
    }

    ORID[] keysFour = new ORID[10];
    ORID[] absentKeysFour = new ORID[10];

    for(byte i = 0; i < 10; i++) {
      keysFour[i] = new ORecordId(i + 5, i - 5);
    }
    for(byte i = 10; i < 20; i++) {
      absentKeysFour[i - 10] = new ORecordId(i * 2 + 5, i * 2 - 5);
    }

    ORID[] keysFive = new ORID[10];
    ORID[] absentKeysFive = new ORID[10];

    for(byte i = 0; i < 10; i++) {
      keysFive[i] = new ORecordId(i * 2 + 5, i * 2 - 5);
    }
    for(byte i = 10; i < 20; i++) {
      absentKeysFive[i - 10] = new ORecordId(i + 15, i - 15);
    }

    ORID[] keysSix = new ORID[5];
    ORID[] absentKeysSix = new ORID[5];
    for(byte i = 0; i < 5; i++) {
      keysSix[i] = new ORecordId(i, i + 1);
    }

    for(byte i = 5; i < 10; i++) {
      absentKeysSix[i - 5] = new ORecordId(i, i + 1);
    }

    ORID[] keysSeven = new ORID[70];
    ORID[] absentKeysSeven = new ORID[70];
    for(byte i = 0; i < 70; i++) {
      keysSeven[i] = new ORecordId(i, i + 1);
    }

    for(int i = 70; i < 140; i++) {
      absentKeysSeven[i - 70] = new ORecordId(i , i + 1);
    }

    ORID[] keysEight = new ORID[70];
    ORID[] absentKeysEight = new ORID[30];

    for(byte i = 0; i < 70; i++) {
      keysEight[i] = new ORecordId(i * 2, i * 2 + 1);
    }
    for(int i = 70; i < 100; i++) {
      absentKeysEight[i -  70] = new ORecordId(i * 2, i * 2 + 1);
    }

    ORID[] keysNine = new ORID[70];
    ORID[] absentKeysNine = new ORID[30];

    for(byte i = 0; i < 70; i++) {
      keysNine[i] = new ORecordId(i * 2, i + 1);
    }
    for(int i = 70; i < 100; i++) {
      absentKeysNine[i - 70] = new ORecordId(i * 2, i + 1);
    }

    ORID[] keysTen = new ORID[20];
    ORID[] absentKeysTen = new ORID[20];
    for(byte i = 0; i < 20; i++) {
      keysTen[i] = new ORecordId(10, i + 50);
    }

    for(byte i = 0; i < 20; i++) {
      absentKeysTen[i] = new ORecordId(2, i);
    }

    ORID[] keysEleven = new ORID[20];
    ORID[] absentKeysEleven = new ORID[20];
    for(byte i = 0; i < 20; i++) {
      keysEleven[i] = new ORecordId(i + 50, 10);
    }

    for(byte i = 0; i < 20; i++) {
      absentKeysEleven[i] = new ORecordId(i, 2);
    }

    ORID[] keysTwelve = new ORID[20];
    ORID[] absentKeysTwelve = new ORID[20];
    for(byte i = 0; i < 20; i++) {
      keysTwelve[i] = new ORecordId(15, i * 2 + 50);
    }

    for(byte i = 0; i < 20; i++) {
      absentKeysTwelve[i] = new ORecordId(2, i * 2);
    }

    ORID[] keysThirteen = new ORID[20];
    ORID[] absentKeysThirteen = new ORID[20];
    for(byte i = 0; i < 20; i++) {
      keysThirteen[i] = new ORecordId(i * 2 + 50, 15 );
    }

    for(byte i = 0; i < 20; i++) {
      absentKeysThirteen[i] =  new ORecordId(i, 2);
    }

    return new Object[][] {
      {keysOne, absentKeysOne},
      {keysTwo, absentKeysTwo},
      {keysThree, absentKeysThree},
      {keysFour, absentKeysFour},
      {keysFive, absentKeysFive},
      {keysSix, absentKeysSix},
      {keysSeven, absentKeysSeven},
      {keysEight, absentKeysEight},
      {keysNine, absentKeysNine},
      {keysTen, absentKeysTen},
      {keysEleven, absentKeysEleven},
      {keysTwelve, absentKeysTwelve},
      {keysThirteen, absentKeysThirteen},
    };
  }
}