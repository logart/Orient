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
import java.util.Iterator;

/**
 * Hash set implementation that is based on Cuckoo Hashing algorithm.
 * It is intended to effectively store integer numbers that is presented by int array.
 * It uses MurmurHash {@link com.orientechnologies.common.util.OMurmurHash3} for hash generation.
 */
public class OCuckooHashSet extends AbstractSet<byte[]> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final int BUCKET_SIZE = 4;
  private static final int MAX_TRIES_PERCENT = 10;
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

  public OCuckooHashSet(int initialCapacity, int keySize) {
    final int capacityBoundary = MAXIMUM_CAPACITY / (keySize * BUCKET_SIZE);

    if(initialCapacity > capacityBoundary)
      initialCapacity = capacityBoundary;

    int capacity = 1;
    while (capacity < initialCapacity)
      capacity <<= 2;

    this.keySize = keySize;

    cuckooTable = new byte[keySize * capacity * BUCKET_SIZE];
    bitSet = new BitSet(capacity * BUCKET_SIZE);
    tableSize = cuckooTable.length >> 1;
    bucketsInTable = capacity >> 1;
    stash = new byte[keySize * MAX_STASH_SIZE];

    maxTries = (capacity * 100) / MAX_TRIES_PERCENT;
  }

  @Override
  public boolean contains(Object o) {
    if(size == 0)
      return false;

    byte[] value = (byte[]) o;

    final int hashOne = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, FIRST_SEED);

    final int bucketIndexOne = bucketIndex(hashOne);
    if (checkBucket(value, bucketIndexOne, 0))
      return true;

    final int hashTwo = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, SECOND_SEED);
    final int bucketIndexTwo = bucketIndex(hashTwo) + tableSize;

    if(checkBucket(value, bucketIndexTwo, 1))
      return true;

    for(int i = 0; i < stashSize; i++)
      for (int j = 0; j < keySize; j++) {
        if(stash[i * keySize + j] != value[j])
          break;

        if(j == keySize - 1)
          return true;
      }

    return false;
  }

  @Override
  public boolean add(byte[] value) {
    if(contains(value))
      return false;

    int tries = 0;
    while (tries < maxTries) {
      final int hashOne = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, FIRST_SEED);
      final int bucketIndexOne = bucketIndex(hashOne);

      if(appendInBucket(value, bucketIndexOne, 0)) {
        size++;
        return true;
      }

      final int hashTwo = OMurmurHash3.murmurhash3_x86_32(value, 0, keySize, SECOND_SEED);
      final int bucketIndexTwo = bucketIndex(hashTwo);

      if(appendInBucket(value, bucketIndexTwo, 1)) {
        size++;
        return true;
      }

      value = replaceInBucket(value, bucketIndexTwo, 1);
      maxTries++;
    }

    if(stashSize < MAX_STASH_SIZE) {
      System.arraycopy(value, 0, stash, stashSize * keySize, keySize);
      stashSize++;
    }

    throw new IllegalArgumentException("Table is full");
  }

  @Override
  public void clear() {
    bitSet.clear();
  }

  private byte[] replaceInBucket(byte[] value, int bucketIndex, int tableIndex) {
    final int beginItem = itemIndex(bucketIndex);
    final int beginIndex = itemIndexInArray(beginItem) + tableIndex * tableSize;

    final byte[] result = new byte[keySize];

    System.arraycopy(cuckooTable, beginIndex, result, 0, keySize);
    System.arraycopy(cuckooTable, beginIndex + keySize, cuckooTable, beginIndex, (BUCKET_SIZE - 1) * keySize);
    System.arraycopy(value, 0, cuckooTable, beginIndex, keySize);

    return result;
  }

  private boolean appendInBucket(byte[] value, int bucketIndex, int tableIndex) {
    final int itemOffset = tableIndex * bucketsInTable;
    final int beginItem = itemIndex(bucketIndex);
    final int endItem = beginItem + BUCKET_SIZE;
    int currentItem = beginItem;

    while(bitSet.get(currentItem + itemOffset) && currentItem < endItem) {
      currentItem++;
    }

    if(currentItem < endItem) {
      final int beginIndex = itemIndexInArray(currentItem);
      System.arraycopy(value, 0, cuckooTable, beginIndex, keySize);
      bitSet.set(currentItem);
      return true;
    }

    return false;
  }

  private boolean checkBucket(byte[] value, int bucketIndex, int tableIndex) {
    final int itemOffset = tableIndex * bucketsInTable;
    final int beginItem = itemIndex(bucketIndex);
    final int endItem = beginItem + BUCKET_SIZE ;

    for (int i = bucketIndex; i < endItem; i++) {
      if (!bitSet.get(i + itemOffset))
        return false;

      if (containsValue(value, i, tableIndex))
        return true;
    }
    return false;
  }

  private boolean containsValue(byte[] value, int itemIndex, int tableIndex) {
    final int beginIndex = itemIndex * keySize + tableIndex * tableSize;
    for (int j = 0; j < keySize; j++) {
      if (value[j] != cuckooTable[beginIndex + j])
        break;

      if (j == keySize - 1)
        return true;
    }

    return false;
  }

  private int bucketIndex(int hash) {
    return (hash & (bucketsInTable - 1));
  }

  private int itemIndex(int bucketIndex) {
    return bucketIndex * BUCKET_SIZE;
  }

  private int itemIndexInArray(int itemIndex) {
    return  itemIndex * keySize;
  }

  @Override
  public Iterator<byte[]> iterator() {
    return null;
  }

  @Override
  public int size() {
    return size;
  }
}
