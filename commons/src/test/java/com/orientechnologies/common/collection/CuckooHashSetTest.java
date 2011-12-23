package com.orientechnologies.common.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.orientechnologies.common.io.OFileUtils;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 22.12.11
 */
@Test
public class CuckooHashSetTest
{
  private OCuckooHashSet hashSet;

  @BeforeMethod
  public void beforeMethod() {
    hashSet = new OCuckooHashSet( 16, 2 );
  }

  @Test(dataProvider = "testKeys")
  public void testAddition(byte[][] keys, byte[][] absentKeys) {
    for(byte i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " is absent" );
    }

    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
    }

    for( final byte[] absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.contains( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testUndoAddition(byte[][] keys, byte[][] absentKeys) {
    if (keys.length < hashSet.capacity())
      return;

    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.add( key ) );
      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
    }

    try {
      hashSet.add( absentKeys[0] );
      Assert.fail();
    } catch(IllegalArgumentException iae ) {
      Assert.assertEquals( iae.getMessage(), "Table is full" );
    }


    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
    }

    for( final byte[] absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.contains( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testValueAdditionValueIsContainedInSet(byte[][] keys, byte[][] absentKeys) {
    for(byte i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertFalse( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " is absent" );
    }

    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
    }

    for( final byte[] absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.contains( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testValueRemoveValue(byte[][] keys, byte[][] absentKeys) {
    for(byte i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertTrue( hashSet.remove( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " was not removed" );
      Assert.assertEquals( i , hashSet.size() );


      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " was not removed" );

      Assert.assertTrue( hashSet.add( keys[i] ) );
      Assert.assertEquals( i + 1, hashSet.size() );
    }

    for( final byte[] absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.remove( absentKey ) );
    }

    for(byte i = 0; i < keys.length; i++) {
      Assert.assertTrue( hashSet.remove( keys[i] ) );
      Assert.assertEquals( keys.length - i - 1, hashSet.size() );
    }

    for( final byte[] key : keys ) {
      Assert.assertFalse( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " was not removed" );
    }

    for( final byte[] absentKey : absentKeys ) {
      Assert.assertFalse( hashSet.remove( absentKey ) );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testClear(byte[][] keys, byte[][] absentKeys) {
    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.add( key ) );
    }

    hashSet.clear();

    Assert.assertEquals( hashSet.size(), 0 );

    for( final byte[] key : keys ) {
      Assert.assertFalse( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " was not removed" );
    }
  }

  @Test(dataProvider = "testKeys")
  public void testIterator(byte[][] keys, byte[][] absentKeys) {
    List<List<Byte>> addedKeys = new ArrayList<List<Byte>>(  );
    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.add( key ) );
      List<Byte> listKey = new ArrayList<Byte>();
      listKey.add( key[0] );
      listKey.add( key[1] );
      addedKeys.add( listKey );
    }

    Iterator<byte[]> iterator = hashSet.iterator();
    for(int i = 0; i < keys.length; i++) {
      Assert.assertTrue( iterator.hasNext(), i +  "-th item is absent in iterator" );

      byte[] key = iterator.next();
      List<Byte> listKey = new ArrayList<Byte>(  );
      listKey.add( key[0] );
      listKey.add( key[1] );

      Assert.assertTrue( addedKeys.remove( listKey ), "Key " + key[0] + ":" + key[1] + " was not removed.");
    }

    Assert.assertFalse( iterator.hasNext(), "Iterator contains more items than expected" );

    try {
      iterator.next();
      Assert.fail();
    } catch( NoSuchElementException e){
    }
  }

  @Test(dataProvider = "testKeys")
  public void testIteratorRemove(byte[][] keys, byte[][] absentKeys) {
    for( final byte[] key : keys ) {
      Assert.assertTrue( hashSet.add( key ) );
    }

    Iterator<byte[]> iterator = hashSet.iterator();
    iterator.next();

    try {
      iterator.remove();
      Assert.fail();
    } catch(  UnsupportedOperationException e ){
    }
  }

  @Test
  public void testCapacityCalculationOne() {
    final OCuckooHashSet cuckooSet = new OCuckooHashSet( 3, 2 );
    Assert.assertEquals( cuckooSet.capacity(), 8 );
  }

  @Test
  public void testCapacityCalculationTwo() {
    final OCuckooHashSet cuckooSet = new OCuckooHashSet( 17, 2 );
    Assert.assertEquals( cuckooSet.capacity(), 32 );
  }

  @Test
  public void testCapacityCalculationMax() {
    final OCuckooHashSet cuckooSet = new OCuckooHashSet(  1 << 30, 1 );
    Assert.assertEquals( cuckooSet.capacity(), 1 << 29 );
  }

  @DataProvider(name = "testKeys")
  public Object[][] testKeys() {
    byte[][] keysOne = new byte[20][2];
    byte[][] absentKeysOne = new byte[20][2];
    for(byte i = 0; i < 20; i++) {
      keysOne[i] = new byte[] {i, (byte)(i + 1)};
    }

    for(byte i = 20; i < 40; i++) {
      absentKeysOne[i - 20] = new byte[] {i, (byte)(i + 1)};
    }

    byte[][] keysTwo = new byte[20][2];
    byte[][] absentKeysTwo = new byte[20][2];

    for(byte i = 0; i < 20; i++) {
      keysTwo[i] = new byte[] {(byte)(i * 2), (byte)(i * 2 + 1)};
    }
    for(byte i = 20; i < 40; i++) {
      absentKeysTwo[i -  20] = new byte[] {(byte)(i * 2), (byte)(i * 2 + 1)};
    }

    byte[][] keysThree = new byte[20][2];
    byte[][] absentKeysThree = new byte[20][2];

    for(byte i = 0; i < 20; i++) {
      keysThree[i] = new byte[] {(byte)(i * 2), (byte)(i + 1)};
    }
    for(byte i = 20; i < 40; i++) {
      absentKeysThree[i - 20] = new byte[] {(byte)(i * 2), (byte)(i + 1)};
    }

    byte[][] keysFour = new byte[10][2];
    byte[][] absentKeysFour = new byte[10][2];

    for(byte i = 0; i < 10; i++) {
      keysFour[i] = new byte[] {(byte)(i + 5), (byte)(i - 5)};
    }
    for(byte i = 10; i < 20; i++) {
      absentKeysFour[i - 10] = new byte[] {(byte)(i * 2 + 5), (byte)(i * 2 - 5)};
    }

    byte[][] keysFive = new byte[10][2];
    byte[][] absentKeysFive = new byte[10][2];

    for(byte i = 0; i < 10; i++) {
      keysFive[i] = new byte[] {(byte)(i * 2 + 5), (byte)(i * 2 - 5)};
    }
    for(byte i = 10; i < 20; i++) {
      absentKeysFive[i - 10] = new byte[] {(byte)(i + 15), (byte)(i - 15)};
    }

    byte[][] keysSix = new byte[5][2];
    byte[][] absentKeysSix = new byte[5][2];
    for(byte i = 0; i < 5; i++) {
      keysSix[i] = new byte[] {i, (byte)(i + 1)};
    }

    for(byte i = 5; i < 10; i++) {
      absentKeysSix[i - 5] = new byte[] {i, (byte)(i + 1)};
    }

    return new Object[][] {
      {keysOne, absentKeysOne},
      {keysTwo, absentKeysTwo},
      {keysThree, absentKeysThree},
      {keysFour, absentKeysFour},
      {keysFive, absentKeysFive},
      {keysSix, absentKeysSix}
    };
  }
}