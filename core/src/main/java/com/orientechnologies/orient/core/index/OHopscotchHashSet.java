package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.id.ORID;


import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OHopscotchHashSet extends AbstractSet<ORID>  {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final long WORD_MASK = 0xffffffffffffffffL;
  private static final int HOPSCOTCH_DISTANCE = Long.SIZE - 1;
  private static final double LOAD_FACTOR = 0.85;

  private static final class Cell {
    private long hopscotchInfo;
    private ORID value;
  }

  private Cell[] cells;
  private int size;
  private int threshold;

  public OHopscotchHashSet(int initialCapacity) {
    if(initialCapacity > MAXIMUM_CAPACITY)
      initialCapacity = MAXIMUM_CAPACITY;

    int capacity = 1;
    while (capacity < initialCapacity || capacity < HOPSCOTCH_DISTANCE + 1)
      capacity = capacity << 1;

    cells = new Cell[capacity];
    threshold = (int)Math.ceil(capacity * LOAD_FACTOR);
  }

  @Override
  public boolean contains(final Object o) {
    final ORID value = (ORID) o;

    final int index = cellIndex(value.hashCode());

    final Cell cell = cells[index];
    if (cell == null)
      return false;

    final long hopscotchInfo = cell.hopscotchInfo;
    for (int distance = nextBitSet(hopscotchInfo, 0); distance >= 0; distance = nextBitSet(hopscotchInfo, distance)) {
      int cellIndex = index + distance;
      if (cellIndex >= cells.length)
        cellIndex -= cells.length;

      final Cell cellToMatch = cells[cellIndex];
      if (value.equals(cellToMatch.value))
        return true;
      else
        distance++;

      if(distance > HOPSCOTCH_DISTANCE)
        return false;
    }

    return false;
  }

  @Override
  public boolean add(ORID value) {
    if (contains(value))
      return false;

    final int index = cellIndex(value.hashCode());

    Cell cell = cells[index];
    if (cell == null) {
      cell = new Cell();
      cells[index] = cell;
    }

    if (cell.value == null) {
      cell.hopscotchInfo = 1;
      cell.value = value;
      size++;

      if(size > threshold)
        rehash();

      return true;
    }

    final int emptyIndex = findFirstEmptyIndex(index);
    if (emptyIndex == -1) {
      rehash();
      return add(value);
    }

    int distance = distance(index, emptyIndex);

    if (distance <= HOPSCOTCH_DISTANCE) {
      final long bitmask = 1L << distance;
      cell.hopscotchInfo |= bitmask;

      cell = cells[emptyIndex];
      if (cell == null) {
        cell = new Cell();
        cells[emptyIndex] = cell;
      }

      cell.value = value;

      size++;

      if(size > threshold)
        rehash();

      return true;
    }

    int indexToProcess = emptyIndex;

    while (true) {
      final int[] moveInfo = findClosestCellToMove(indexToProcess);
      if (moveInfo == null) {
        rehash();
        return add(value);
      }

      final int bucketMoveIndex = moveInfo[0];
      final int cellMoveIndex = moveInfo[1];
      moveValues(bucketMoveIndex, cellMoveIndex, indexToProcess);

      distance = distance(index, cellMoveIndex);

      if (distance <= HOPSCOTCH_DISTANCE) {
        final long bitmask = 1L << distance;
        cell.hopscotchInfo |= bitmask;

        final Cell cellToSet = cells[cellMoveIndex];
        cellToSet.value = value;

        size++;

        if(size > threshold)
          rehash();

        return true;
      }

      indexToProcess = cellMoveIndex;
    }
  }

  @Override
  public boolean remove(Object o) {
    final ORID value = (ORID) o;

    final int index = cellIndex(value.hashCode());

    final Cell cell = cells[index];
    if (cell == null)
      return false;

    final long hopscotchInfo = cell.hopscotchInfo;
    for (int distance = nextBitSet(hopscotchInfo, 0); distance >= 0; distance = nextBitSet(hopscotchInfo, distance)) {
      int cellIndex = index + distance;
      if (cellIndex >= cells.length)
        cellIndex -= cells.length;

      final Cell cellToMatch = cells[cellIndex];
      if (value.equals(cellToMatch.value)) {
        cellToMatch.value = null;
        final long clearMask = ~(1L << distance);
        cell.hopscotchInfo &= clearMask;

        if(cellToMatch.hopscotchInfo == 0 && cellToMatch.value == null)
          cells[cellIndex] = null;

        size--;
        return true;
      } else
        distance++;

      if(distance > HOPSCOTCH_DISTANCE)
        return false;
    }

    return false;
  }

  @Override
  public void clear() {
    cells = new Cell[cells.length];
    size = 0;
  }

  public int capacity() {
    return cells.length;
  }
  
  private int nextBitSet(final long value, final int index) {
    final long word = value & (WORD_MASK << index);

    if (word == 0)
      return -1;

    return Long.numberOfTrailingZeros(word);
  }

  private int cellIndex(final int hash) {
    return (cells.length - 1) & hash;
  }

  private void moveValues(final int bucketCellIndex, final int fromCellIndex, final int toCellIndex) {

    final Cell fromCell = cells[fromCellIndex];

    Cell toCell = cells[toCellIndex];
    if (toCell == null) {
      toCell = new Cell();
      cells[toCellIndex] = toCell;
    }
    toCell.value = fromCell.value;

    final long clearMask = ~(1L << distance(bucketCellIndex, fromCellIndex));
    final long setMask = 1L << distance(bucketCellIndex, toCellIndex);

    final Cell bucketCell = cells[bucketCellIndex];
    bucketCell.hopscotchInfo &= clearMask;
    bucketCell.hopscotchInfo |= setMask;

    fromCell.value = null;
    if(fromCell.hopscotchInfo == 0)
      cells[fromCellIndex] = null;
  }

  private int distance(int fromIndex, int toIndex) {
    return toIndex > fromIndex ? toIndex - fromIndex : cells.length - fromIndex + toIndex;
  }

  private int[] findClosestCellToMove(final int index) {
    int beginWith = index - HOPSCOTCH_DISTANCE;
    if (beginWith > 0) {
      for (int i = beginWith; i < index; i++) {
        final Cell cell = cells[i];
        if (cell.hopscotchInfo > 0) {
          final int closestIndex = Long.numberOfTrailingZeros(cell.hopscotchInfo) + i;
          if (closestIndex < index)
            return new int[]{i, closestIndex};
        }
      }

      return null;
    } else {
      for (int i = cells.length + beginWith; i < cells.length; i++) {
        final Cell cell = cells[i];
        if (cell.hopscotchInfo > 0) {
          final int closestIndex = Long.numberOfTrailingZeros(cell.hopscotchInfo);

          if (closestIndex + i < cells.length)
            return new int[]{i, closestIndex + i};

          if (closestIndex + i - cells.length < beginWith)
            return new int[]{i, closestIndex + i - cells.length};
        }
      }

      for (int i = 0; i < index; i++) {
        final Cell cell = cells[i];
        if (cell.hopscotchInfo > 0) {
          final int closestIndex = Long.numberOfTrailingZeros(cell.hopscotchInfo) + i;
          if (closestIndex < index)
            return new int[]{closestIndex, i};
        }
      }

      return null;
    }
  }


  private int findFirstEmptyIndex(final int index) {
    for (int i = index + 1; i < cells.length; i++) {
      if (isEmpty(i))
        return i;
    }

    for (int i = 0; i < index; i++)
      if (isEmpty(i))
        return i;

    return -1;
  }

  private boolean isEmpty(final int index) {
    final Cell cell = cells[index];
    return cell == null || cell.value == null;

  }

  private void rehash() {
    final Cell[] oldCells = cells;
    cells = new Cell[oldCells.length << 1];
    threshold = (int)Math.ceil(cells.length * LOAD_FACTOR);
    size = 0;

    for (final Cell cell : oldCells) {
      if (cell != null && cell.value != null)
        add(cell.value);
    }
  }

  @Override
  public Iterator<ORID> iterator() {
    return new Iterator<ORID>() {

      private int processedItems = 0;
      private int currentIndex = 0;

      public boolean hasNext() {
        return processedItems < size;  
      }

      public ORID next() {
        if(processedItems >= size)
          throw new NoSuchElementException();

        while (currentIndex < cells.length && (cells[currentIndex] == null || cells[currentIndex].value == null))
          currentIndex++;

        final ORID result = cells[currentIndex].value;

        currentIndex++;
        processedItems++;

        return result;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public int size() {
    return size;
  }
}
