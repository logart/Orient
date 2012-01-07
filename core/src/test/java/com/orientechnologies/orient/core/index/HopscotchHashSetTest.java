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
  public void testRemoveValue(ORID[] keys, ORID[] absentKeys) {
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
  public void testAddRemoveValue(ORID[] keys, ORID[] absentKeys) {
    for(int i = 0; i < keys.length / 2; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not added" );
    }

    for(int i = 0; i < keys.length / 4; i++) {
      Assert.assertTrue( hashSet.remove( keys[i] ) );
      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not removed" );
    }

    for(int i = keys.length / 2; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not added" );
    }


    for(int i = keys.length / 4; i < keys.length / 2; i++) {
      Assert.assertTrue( hashSet.remove( keys[i] ) );
      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not removed" );
    }

    for(int i = 0; i < keys.length / 2; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not added" );
    }

    for( final ORID absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.remove( absentKey ) );
    }

    for( final ORID key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key + " was not added" );
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
  public void testAddRemoveValueTwo(ORID[] keys, ORID[] absentKeys) {
    for(int i = 0; i < keys.length; i+= 2) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not added" );
    }

    for(int i = 1; i < keys.length; i+= 2) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not added" );
    }


    for(int i = 0; i < keys.length; i+= 2) {
      Assert.assertTrue( hashSet.remove( keys[i] ) );
      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not removed" );
    }

    for(int i = 0; i < keys.length; i+= 2) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i] + " was not added" );
    }

    for( final ORID absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.remove( absentKey ) );
    }

    for( final ORID key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key + " was not added" );
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
    ORID[] keysOne = new ORID[512];
    ORID[] absentKeysOne = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysOne[i] = new ORecordId(i , (i + 1));
    }

    for(int i = 512; i < 1024; i++) {
      absentKeysOne[i - 512] = new ORecordId(i , (i + 1));
    }

    ORID[] keysTwo = new ORID[512];
    ORID[] absentKeysTwo = new ORID[512];

    for(int i = 0; i < 512; i++) {
      keysTwo[i] = new ORecordId(i* 2 , (i * 2 + 1));
    }
    for(int i = 512; i < 1024; i++) {
      absentKeysTwo[i -  512] = new ORecordId(i* 2 , (i * 2 + 1));
    }

    ORID[] keysThree = new ORID[512];
    ORID[] absentKeysThree = new ORID[512];

    for(int i = 0; i < 512; i++) {
      keysThree[i] = new ORecordId(i * 2, i + 1);
    }

    for(int i = 512; i < 1024; i++) {
      absentKeysThree[i - 512] = new ORecordId(i * 2, i + 1);
    }

    ORID[] keysFour = new ORID[512];
    ORID[] absentKeysFour = new ORID[512];

    for(int i = 0; i < 512; i++) {
      keysFour[i] = new ORecordId(i + 5, i - 5);
    }
    for(int i = 512; i < 1024; i++) {
      absentKeysFour[i - 512] = new ORecordId(i * 2 + 5, i * 2 - 5);
    }

    ORID[] keysFive = new ORID[512];
    ORID[] absentKeysFive = new ORID[512];

    for(int i = 0; i < 512; i++) {
      keysFive[i] = new ORecordId(i * 2 + 5, i * 2 - 5);
    }
    for(int i = 512; i < 1024; i++) {
      absentKeysFive[i - 512] = new ORecordId(i + 15, i - 15);
    }

    ORID[] keysSix = new ORID[512];
    ORID[] absentKeysSix = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysSix[i] = new ORecordId(i * 3 + i, i + 1);
    }

    for(int i = 512; i < 1024; i++) {
      absentKeysSix[i - 512] = new ORecordId(i * 3 + 1, i + 1);
    }

    ORID[] keysSeven = new ORID[512];
    ORID[] absentKeysSeven = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysSeven[i] = new ORecordId(i * 4 + 10, i * 2 + 1);
    }

    for(int i = 512; i < 1024; i++) {
      absentKeysSeven[i - 512] = new ORecordId(i * 4 + 10, i * 2 + 1);
    }

    ORID[] keysEight = new ORID[512];
    ORID[] absentKeysEight = new ORID[512];

    for(int i = 0; i < 512; i++) {
      keysEight[i] = new ORecordId(i * 5 + i, i * 2 + 1);
    }
    for(int i = 512; i < 1024; i++) {
      absentKeysEight[i -  512] = new ORecordId(i * 5 + i, i * 2 + 1);
    }

    ORID[] keysNine = new ORID[512];
    ORID[] absentKeysNine = new ORID[512];

    for(int i = 0; i < 512; i++) {
      keysNine[i] = new ORecordId(i * 6 + i, i + 1);
    }
    for(int i = 512; i < 1024; i++) {
      absentKeysNine[i - 512] = new ORecordId(i * 6 + i, i + 1);
    }

    ORID[] keysTen = new ORID[512];
    ORID[] absentKeysTen = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysTen[i] = new ORecordId(10, i + 50);
    }

    for(int i = 0; i < 512; i++) {
      absentKeysTen[i] = new ORecordId(2, i);
    }

    ORID[] keysEleven = new ORID[512];
    ORID[] absentKeysEleven = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysEleven[i] = new ORecordId(i + 50, 10);
    }

    for(int i = 0; i < 512; i++) {
      absentKeysEleven[i] = new ORecordId(i * 2, 2);
    }

    ORID[] keysTwelve = new ORID[512];
    ORID[] absentKeysTwelve = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysTwelve[i] = new ORecordId(15, i * 2 + 50);
    }

    for(int i = 0; i < 512; i++) {
      absentKeysTwelve[i] = new ORecordId(2, i * 2);
    }

    ORID[] keysThirteen = new ORID[512];
    ORID[] absentKeysThirteen = new ORID[512];
    for(int i = 0; i < 512; i++) {
      keysThirteen[i] = new ORecordId(2000, i * 5 + i );
    }

    for(int i = 0; i < 512; i++) {
      absentKeysThirteen[i] =  new ORecordId(i, 2);
    }

    return new Object[][] {
//      {keysOne, absentKeysOne},
//      {keysTwo, absentKeysTwo},
//      {keysThree, absentKeysThree},
//      {keysFour, absentKeysFour},
      {keysFive, absentKeysFive},
//      {keysSix, absentKeysSix},
//      {keysSeven, absentKeysSeven},
//      {keysEight, absentKeysEight},
//      {keysNine, absentKeysNine},
//      {keysTen, absentKeysTen},
//      {keysEleven, absentKeysEleven},
//      {keysTwelve, absentKeysTwelve},
//      {keysThirteen, absentKeysThirteen},
    };
  }
}