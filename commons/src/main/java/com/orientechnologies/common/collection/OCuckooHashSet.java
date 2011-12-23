/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.collection;

import com.orientechnologies.common.util.BitSet;
import com.orientechnologies.common.util.OMurmurHash3;

import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Hash set implementation that is based on Cuckoo Hashing algorithm.
 * It is intended to effectively store integer numbers that is presented by int array.
 * It uses MurmurHash {@link com.orientechnologies.common.util.OMurmurHash3} for hash generation.
 *
 * For the sake of performance optimization  <code>Set</code> does not check concurrent collection modifications during its iteration.
 */
public class OCuckooHashSet extends AbstractSet<byte[]> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final int BUCKET_SIZE = 4;
  private static final int MAX_TRIES_BASE = 10;
  private static final int MAX_STASH_SIZE = 4;
  private static final int FIRST_SEED = 0x3ac5d673;
  private static final int SECOND_SEED = 0x6d7839d0;

  private int tableSize;
  private int bucketsInTable;
  private int size;

  private int maxTries;

  private byte[] cuckooTable;
  private BitSet bitSet;
  private byte[] stash;
  private int stashSize;
  private int keySize;
  private int itemsInTable;

  public static  enum ADDITION_RESULT  {
    ITEM_WAS_ADDED,
    ITEM_ALREADY_IN_SET,
    TABLE_IS_FULL
  }

  public OCuckooHashSet(int initialCapacity, int keySize) {
    final int capacityBoundary = MAXIMUM_CAPACITY / keySize;

    if(initialCapacity > capacityBoundary)
      initialCapacity = capacityBoundary;

    int capacity = 2 * BUCKET_SIZE;
    while (capacity < initialCapacity)
      capacity <<= 1;

    this.keySize = keySize;

    cuckooTable = new byte[keySize * capacity ];
    bitSet = new BitSet(capacity);
    tableSize = cuckooTable.length >> 1;
    bucketsInTable = (capacity / BUCKET_SIZE) >> 1;
    itemsInTable = capacity >> 1;
    stash = new byte[keySize * MAX_STASH_SIZE];

    maxTries = (int)Math.round(MAX_TRIES_BASE * Math.log(bucketsInTable << 1));
  }

  @Override
  public boolean contains(final Object o) {
    if(size == 0)
      return false;

    byte[] value = (byte[]) o;

    final int hashOne = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, FIRST_SEED);

    final int bucketIndexOne = bucketIndex(hashOne);
    if (checkBucket(value, bucketIndexOne, 0))
      return true;

    final int hashTwo = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, SECOND_SEED);
    final int bucketIndexTwo = bucketIndex(hashTwo);

    if(checkBucket(value, bucketIndexTwo, 1))
      return true;

    for(int i = 0; i < stashSize; i++) {
      int j = 0;
      while (true) {
        if(stash[i * keySize + j] != value[j])
          break;

        if(j == keySize - 1)
          return true;
        j++;
      }
    }

    return false;
  }

  @Override
  public boolean add(final byte[] value) {
    final ADDITION_RESULT result = addItem( value );

    switch (result) {
      case ITEM_ALREADY_IN_SET:
        return false;
      case ITEM_WAS_ADDED:
        return true;
      default:
        throw new IllegalArgumentException( "Table is full" );
    }
  }

  public ADDITION_RESULT addItem(byte [] value) {
    if(contains(value))
      return ADDITION_RESULT.ITEM_ALREADY_IN_SET;

    int tries = 0;
    final int[] replaceHistory;
    if ( stashSize == MAX_STASH_SIZE ) {
      replaceHistory = new int[maxTries];
    } else {
      replaceHistory = null;
    }


    while (tries < maxTries) {
      final int hashOne = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, FIRST_SEED);
      final int bucketIndexOne = bucketIndex(hashOne);

      if(appendInBucket(value, bucketIndexOne, 0)) {
        size++;
        return ADDITION_RESULT.ITEM_WAS_ADDED;
      }

      final int hashTwo = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, SECOND_SEED);
      final int bucketIndexTwo = bucketIndex(hashTwo);

      if(appendInBucket(value, bucketIndexTwo, 1)) {
        size++;
        return ADDITION_RESULT.ITEM_WAS_ADDED;
      }

      value = replaceInBucket(value, bucketIndexTwo, 1, replaceHistory, tries);
      tries++;
    }

    if(stashSize < MAX_STASH_SIZE) {
      System.arraycopy(value, 0, stash, stashSize * keySize, keySize);
      stashSize++;
      size++;
      return ADDITION_RESULT.ITEM_WAS_ADDED;
    }

    undoBucketReplacement( value, replaceHistory, maxTries );

    return ADDITION_RESULT.TABLE_IS_FULL;
  }

  @Override
  public boolean remove( final Object o )
  {
    if(size == 0)
      return false;

    byte[] value = (byte[]) o;

    final int hashOne = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, FIRST_SEED);

    final int bucketIndexOne = bucketIndex(hashOne);
    if (removeFromBucket(value, bucketIndexOne, 0)) {
      size--;
      return true;
    }

    final int hashTwo = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, SECOND_SEED);
    final int bucketIndexTwo = bucketIndex(hashTwo);

    if(removeFromBucket(value, bucketIndexTwo, 1)) {
      size--;
      return true;
    }

    for(int i = 0; i < stashSize; i++) {
      int j = 0;
      while (true) {
        if(stash[i * keySize + j] != value[j])
          break;

        if(j == keySize - 1) {
          System.arraycopy( stash, (i + 1) * keySize, stash, i * keySize, (stashSize - i - 1) * keySize );
          size--;
          stashSize--;
          return true;
        }
        j++;
      }
    }
    return false;
  }

  @Override
  public void clear() {
    size = 0;
    bitSet.clear();
  }

  private byte[] replaceInBucket(final byte[] value,final int bucketIndex,final int tableIndex,final int[] replaceHistory,final int historySize) {
    final int beginItem = itemIndex(bucketIndex);
    final int beginIndex = itemIndexInArray(beginItem) + tableIndex * tableSize;

    final byte[] result = new byte[keySize];

    System.arraycopy(cuckooTable, beginIndex, result, 0, keySize);
    System.arraycopy(value, 0, cuckooTable, beginIndex, keySize);

    if(replaceHistory != null)
      replaceHistory[historySize] = beginIndex;

    return result;
  }

  private void undoBucketReplacement(final byte[] value,final int[] replaceHistory,final int historySize) {
    final byte tmp[] = new byte[keySize];

    for(int i = 0; i < historySize; i++) {
      System.arraycopy( cuckooTable, replaceHistory[i], tmp, 0, keySize );
      System.arraycopy( value, 0, cuckooTable, replaceHistory[i], keySize );
      System.arraycopy( tmp, 0, value, 0, keySize );
    }
  }

  private boolean appendInBucket(final byte[] value,final int bucketIndex,final int tableIndex) {
    final int itemOffset = tableIndex * itemsInTable;
    final int beginItem = itemIndex(bucketIndex);
    final int endItem = beginItem + BUCKET_SIZE;
    int currentItem = beginItem;

    while(bitSet.get(currentItem + itemOffset) && currentItem < endItem) {
      currentItem++;
    }

    if(currentItem < endItem) {
      final int beginIndex = itemIndexInArray(currentItem) + tableSize * tableIndex;
      System.arraycopy(value, 0, cuckooTable, beginIndex, keySize);
      bitSet.set(currentItem + itemOffset);
      return true;
    }

    return false;
  }

  private boolean checkBucket(final byte[] value,final int bucketIndex,final int tableIndex) {
    final int itemOffset = tableIndex * itemsInTable;
    final int beginItem = itemIndex(bucketIndex);
    final int endItem = beginItem + BUCKET_SIZE ;

    for (int i = beginItem; i < endItem; i++) {
      if (!bitSet.get(i + itemOffset))
        return false;

      if (containsValue(value, i, tableIndex))
        return true;
    }
    return false;
  }

  private boolean removeFromBucket(final byte[] value,final int bucketIndex,final int tableIndex) {
    final int itemOffset = tableIndex * itemsInTable;
    final int beginItem = itemIndex(bucketIndex);
    final int endItem = beginItem + BUCKET_SIZE ;

    for (int i = beginItem; i < endItem; i++) {
      if (!bitSet.get(i + itemOffset))
        return false;

      if (containsValue(value, i, tableIndex)) {
        final int srcIndex = itemIndexInArray( i + 1 ) + tableIndex * tableSize;
        final int destIndex = itemIndexInArray( i ) + tableIndex * tableSize;
        final int endIndex = itemIndexInArray( endItem ) + tableIndex * tableSize;

        System.arraycopy( cuckooTable, srcIndex, cuckooTable, destIndex, endIndex - srcIndex );

        int j = endItem - 1;
        while (!bitSet.get( j + itemOffset )) {
          j--;
        }
        bitSet.clear( j + itemOffset );

        return true;
      }
    }
    return false;
  }


  private boolean containsValue(final byte[] value,final int itemIndex,final int tableIndex) {
    final int beginIndex = itemIndexInArray( itemIndex ) + tableIndex * tableSize;
    int j = 0;
    while (true){
      if (value[j] != cuckooTable[beginIndex + j])
        break;

      if (j == keySize - 1)
        return true;
      j++;
    }

    return false;
  }

  private int bucketIndex(final int hash) {
    return (hash & (bucketsInTable - 1));
  }

  private int itemIndex(final int bucketIndex) {
    return bucketIndex * BUCKET_SIZE;
  }

  private int itemIndexInArray(final int itemIndex) {
    return  itemIndex * keySize;
  }

  @Override
  public Iterator<byte[]> iterator() {
    return new Iterator<byte[]>()
    {
      private int currentItemIndex = -1;
      private int processedItems;

      public boolean hasNext()
      {
        return processedItems < size;
      }

      public byte[] next()
      {
        if(processedItems >= size)
          throw new NoSuchElementException(  );

        currentItemIndex++;
        final int totalItemsAmount = itemsInTable * 2;
        while (currentItemIndex < totalItemsAmount && !bitSet.get( currentItemIndex )){
          currentItemIndex++;
        }

        final byte[] result = new byte[keySize];

        if(currentItemIndex < totalItemsAmount ) {
          final int arrayIndex = itemIndexInArray( currentItemIndex );
          System.arraycopy( cuckooTable, arrayIndex, result, 0, keySize );
        } else {
          final int stashIndex = currentItemIndex - totalItemsAmount;
          System.arraycopy( stash, stashIndex * keySize, result, 0, keySize );
        }

        processedItems++;
        return result;
      }

      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public int size() {
    return size;
  }

  public int capacity() {
    return itemsInTable << 1;
  }
}