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
import java.util.Random;

/**
 * Hash set implementation that is based on Cuckoo Hashing algorithm.
 * It is intended to effectively store integer numbers that is presented by int array.
 * It uses MurmurHash {@link com.orientechnologies.common.util.OMurmurHash3} for hash generation.
 */
public class OCuckooSet extends AbstractSet<byte[]> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final int BUCKET_SIZE = 4;
  private static final int MAX_TRIES = 1000;
  private static final int FIRST_SEED = 0x3ac5d673;
  private static final int SECOND_SEED = 0x6d7839d0;

  private int tableSize;
  private int bucketsInTable;
  private int size;

  private int maxTries;

  private byte[] cuckooTable;
  private BitSet bitSet;

  private int keySize;
  private int capacity;

  public OCuckooSet(int initialCapacity,int keySize) {
    final int capacityBoundary = MAXIMUM_CAPACITY / (keySize * BUCKET_SIZE);

    if(initialCapacity > capacityBoundary)
      initialCapacity = capacityBoundary;

    capacity = 1;
    while (capacity < initialCapacity)
      capacity <<= 2;

    this.keySize = keySize;

    cuckooTable = new byte[keySize * capacity * BUCKET_SIZE];
    bitSet = new BitSet(capacity * BUCKET_SIZE);
    tableSize = cuckooTable.length >> 1;
    bucketsInTable = (int)(bitSet.size() >> 1);

    maxTries = Math.min(cuckooTable.length >> 1, MAX_TRIES);
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

    return checkBucket(value, bucketIndexTwo, 1);
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

      maxTries++;
    }

    throw new IllegalArgumentException("Table is full");
  }

  @Override
  public void clear() {
    bitSet.clear();
  }

  private boolean appendInBucket(byte[] value, int bucketIndex, int tableIndex) {
    final int beginItem = itemIndex(bucketIndex) + tableIndex * bucketsInTable;
    final int endItem = beginItem + BUCKET_SIZE;
    int itemIndex = beginItem;

    while(bitSet.get(itemIndex) && itemIndex < endItem) {
      itemIndex++;
    }

    if(itemIndex < endItem) {
      final int beginIndex = itemIndexInArray(itemIndex);
      System.arraycopy(value, 0, cuckooTable, beginIndex, keySize);
      bitSet.set(itemIndex);
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
    return (hash & (capacity - 1));
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
