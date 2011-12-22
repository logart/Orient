package com.orientechnologies.common.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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


  @Test
  public void testAddition() {
    byte[][] keys = new byte[20][2];
    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};
      
      Assert.assertTrue( hashSet.add( key ) );
      Assert.assertEquals( i + 1, hashSet.size() );
      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
      keys[i] = key;
    }


    for(int i = 0; i < 20; i++)
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " is absent" );

    for(byte i = 20; i < 56; i++)
      Assert.assertFalse( hashSet.contains( new byte[] {15, i}  ) );
  }

  @Test
  public void testUndoAddition() {
    byte[][] keys = new byte[20][2];
    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};

      Assert.assertTrue( hashSet.add( key ) );
      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
      keys[i] = key;
    }

    try {
      hashSet.add( new byte[] {21, 16} );
      Assert.fail();
    } catch(IllegalArgumentException iae ) {
      Assert.assertEquals( iae.getMessage(), "Table is full" );
    }


    for(int i = 0; i < 20; i++)
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " is absent" );

    for(byte i = 20; i < 56; i++)
      Assert.assertFalse( hashSet.contains( new byte[] {15, i}  ) );
  }

  @Test
  public void testValueAdditionValueIsContainedInSet() {
    byte[][] keys = new byte[20][2];
    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};

      Assert.assertTrue( hashSet.add( key ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertFalse( hashSet.add( key ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertTrue( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " is absent" );
      keys[i] = key;
    }

    for(int i = 0; i < 20; i++)
      Assert.assertTrue( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " is absent" );

    for(byte i = 20; i < 56; i++)
      Assert.assertFalse( hashSet.contains( new byte[] {15, i}  ) );
  }

  @Test
  public void testValueRemoveValue() {
    byte[][] keys = new byte[20][2];
    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};

      Assert.assertTrue( hashSet.add( key ) );
      Assert.assertEquals( i + 1, hashSet.size() );

      Assert.assertTrue( hashSet.remove( key ), "Key " + key[0] + ":" + key[1] + " was not removed" );
      Assert.assertEquals( i , hashSet.size() );


      Assert.assertFalse( hashSet.contains( key ), "Key " + key[0] + ":" + key[1] + " was not removed" );

      Assert.assertTrue( hashSet.add( key ) );
      Assert.assertEquals( i + 1, hashSet.size() );
      keys[i] = key;
    }

    for(byte i = 20; i < 56; i++)
      Assert.assertFalse( hashSet.remove( new byte[] {15, i}  ) );

    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};
      Assert.assertTrue( hashSet.remove( key ) );
      Assert.assertEquals( 20 - i - 1, hashSet.size() );

      keys[i] = key;
    }

    for(int i = 0; i < 20; i++)
      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " was not removed" );

    for(byte i = 20; i < 56; i++)
      Assert.assertFalse( hashSet.remove( new byte[] {15, i}  ) );
  }

  @Test
  public void testClear() {
    byte[][] keys = new byte[20][2];
    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};
      Assert.assertTrue( hashSet.add( key ) );
      keys[i] = key;
    }

    hashSet.clear();

    Assert.assertEquals( hashSet.size(), 0 );

    for(int i = 0; i < 20; i++)
      Assert.assertFalse( hashSet.contains( keys[i] ), "Key " + keys[i][0] + ":" + keys[i][1] + " was not removed" );
  }
  
  @Test
  public void testIterator() {
    final List<List<Byte>> keys = new ArrayList<List<Byte>>(  );
    for(byte i = 0; i < 20; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};
      Assert.assertTrue( hashSet.add( key ) );
      List<Byte> listKey = new ArrayList<Byte>(  );
      listKey.add( key[0] );
      listKey.add( key[1] );
      keys.add( listKey );
    }

    Iterator<byte[]> iterator = hashSet.iterator();
    for(int i = 0; i < 20; i++) {
      Assert.assertTrue( iterator.hasNext(), i +  "-th item is absent in iterator" );

      byte[] key = iterator.next();
      List<Byte> listKey = new ArrayList<Byte>(  );
      listKey.add( key[0] );
      listKey.add( key[1] );

      Assert.assertTrue( keys.remove( listKey ), "Key " + key[0] + ":" + key[1] + " was not removed.");
    }

    Assert.assertFalse( iterator.hasNext(), "Iterator contains more items than expected" );
  }

  @Test
  public void testPartialIterator() {
    final List<List<Byte>> keys = new ArrayList<List<Byte>>(  );
    for(byte i = 0; i < 10 ; i++) {
      byte[] key = new byte[] {i, (byte)(i + 1)};
      Assert.assertTrue( hashSet.add( key ) );
      List<Byte> listKey = new ArrayList<Byte>(  );
      listKey.add( key[0] );
      listKey.add( key[1] );
      keys.add( listKey );
    }

    Iterator<byte[]> iterator = hashSet.iterator();
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue( iterator.hasNext(), i +  "-th item is absent in iterator" );

      byte[] key = iterator.next();
      List<Byte> listKey = new ArrayList<Byte>(  );
      listKey.add( key[0] );
      listKey.add( key[1] );

      Assert.assertTrue( keys.remove( listKey ), "Key " + key[0] + ":" + key[1] + " was not removed.");
    }

    Assert.assertFalse( iterator.hasNext(), "Iterator contains more items than expected" );
  }
}