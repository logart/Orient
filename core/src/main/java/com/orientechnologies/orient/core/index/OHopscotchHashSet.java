package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.id.ORID;


import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OHopscotchHashSet extends AbstractSet<ORID> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final byte HOPSCOTCH_DISTANCE = 64;
  private static final byte EMPTY_BUCKET = HOPSCOTCH_DISTANCE + 1;
  private static final double LOAD_FACTOR = 0.85;

  private static final class Cell {
    private byte nextDistance;
    private byte firstDistance = EMPTY_BUCKET;
    private ORID value;
  }

  private Cell[] cells;
  private int size;
  private int threshold;

  public OHopscotchHashSet(int initialCapacity) {
    if (initialCapacity > MAXIMUM_CAPACITY)
      initialCapacity = MAXIMUM_CAPACITY;

    int capacity = 1;
    while (capacity < initialCapacity || capacity < HOPSCOTCH_DISTANCE)
      capacity = capacity << 1;

    cells = new Cell[capacity];
    threshold = (int) Math.ceil(capacity * (1 - LOAD_FACTOR));
  }

  @Override
  public boolean contains(final Object o) {
    final ORID value = (ORID) o;

    final int index = cellIndex(value.hashCode(), cells.length);

    final Cell cell = cells[index];
    if (cell == null || cell.firstDistance == EMPTY_BUCKET)
      return false;

    Cell cellToMatch;
    int cellIndex = normalizeCellIndex(index + cell.firstDistance, cells.length);

    do {
      cellToMatch = cells[cellIndex];
      if (value.equals(cellToMatch.value))
        return true;

      cellIndex = normalizeCellIndex(cellIndex + cellToMatch.nextDistance, cells.length);
    } while (cellToMatch.nextDistance != 0);

    return false;
  }

  @Override
  public boolean add(ORID value) {
    if (contains(value))
      return false;

    final int index = cellIndex(value.hashCode(), cells.length);

    Cell bucketCell = cells[index];
    if (bucketCell == null) {
      bucketCell = new Cell();
      cells[index] = bucketCell;

      bucketCell.firstDistance = 0;
      bucketCell.value = value;
      size++;

      if (size > threshold)
        rehash();

      return true;
    }

    final int emptyIndex = findFirstEmptyIndex(index);
    if (emptyIndex == -1) {
      rehash();
      return add(value);
    }

    int distance = distance(index, emptyIndex, cells.length);

    if (distance < HOPSCOTCH_DISTANCE) {
      Cell emptyCell = cells[emptyIndex];
      if (emptyCell == null) {
        emptyCell = new Cell();
        cells[emptyIndex] = emptyCell;
      }

      emptyCell.value = value;
      if (bucketCell.firstDistance == EMPTY_BUCKET) {
        bucketCell.firstDistance = (byte) distance;
      } else {
        final int lastCellIndex = findLastBucketCell(index);
        final Cell lastCell = cells[lastCellIndex];

        lastCell.nextDistance = (byte)calculateNextCellDistance(index, lastCellIndex, emptyIndex, cells.length);
        int t = 1;
      }
      size++;

      if (size > threshold)
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

      distance = distance(index, cellMoveIndex, cells.length);

      if (distance < HOPSCOTCH_DISTANCE) {
        final Cell cellToSet = cells[cellMoveIndex];
        cellToSet.value = value;

        if (bucketCell.firstDistance == EMPTY_BUCKET) {
          bucketCell.firstDistance = (byte) distance;
        } else {
          final int lastCellIndex = findLastBucketCell(index);
          final Cell lastCell = cells[lastCellIndex];

          lastCell.nextDistance = (byte)calculateNextCellDistance(index, lastCellIndex, cellMoveIndex, cells.length);
          int t = 1;
        }
        size++;

        if (size > threshold)
          rehash();

        return true;
      }

      indexToProcess = cellMoveIndex;
    }
  }

  @Override
  public boolean remove(Object o) {
    final ORID value = (ORID) o;

    final int index = cellIndex(value.hashCode(), cells.length);

    final Cell bucketCell = cells[index];
    if (bucketCell == null || bucketCell.firstDistance == EMPTY_BUCKET)
      return false;

    Cell cellToMatch;
    int cellIndex = normalizeCellIndex(index + bucketCell.firstDistance, cells.length);
    int prevCellIndex = -1;

    do {
      cellToMatch = cells[cellIndex];
      if (value.equals(cellToMatch.value)) {
        if (prevCellIndex != -1) {
          final Cell prevCell = cells[prevCellIndex];
          if (cellToMatch.nextDistance != 0) {
            final int nextCellIndex = normalizeCellIndex(cellIndex + cellToMatch.nextDistance, cells.length);
            prevCell.nextDistance = (byte)calculateNextCellDistance(index, prevCellIndex, nextCellIndex, cells.length);
          }
          else
            prevCell.nextDistance = 0;
        } else {
          if(cellToMatch.nextDistance != 0) {
            final int nextCellIndex = normalizeCellIndex(cellIndex + cellToMatch.nextDistance, cells.length);
            bucketCell.firstDistance = (byte)distance(index, nextCellIndex, cells.length);
          } else {
            if(bucketCell.value == null)
              cells[index] = null;
            else
              bucketCell.firstDistance = EMPTY_BUCKET;
          }

        }

        if(cellToMatch.firstDistance == EMPTY_BUCKET)
          cells[cellIndex] = null;
        else
          cellToMatch.value = null;

        size--;
        return true;
      }

      prevCellIndex = cellIndex;
      cellIndex = normalizeCellIndex(cellIndex + cellToMatch.nextDistance, cells.length);
    }

    while (cellToMatch.nextDistance != 0);

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

  private int findLastBucketCell(final int bucketCellIndex) {
    final Cell bucketCell = cells[bucketCellIndex];
    int cellIndex = normalizeCellIndex(bucketCellIndex + bucketCell.firstDistance, cells.length);
    Cell cell = cells[cellIndex];
    while (cell.nextDistance != 0) {
      cellIndex = normalizeCellIndex(cellIndex + cell.nextDistance, cells.length);
      cell = cells[cellIndex];
    }

    return cellIndex;
  }

  private static int calculateNextCellDistance(final int bucketIndex, final int lastCellIndex,
                                               final int cellToAddIndex, final int tableSize) {
    if(cellToAddIndex < bucketIndex && lastCellIndex > bucketIndex || cellToAddIndex > bucketIndex && lastCellIndex < bucketIndex)
      return tableSize - lastCellIndex + cellToAddIndex;
    return cellToAddIndex - lastCellIndex;
  }

  private static int normalizeCellIndex(final int cellIndex, final int tableSize) {
    if(cellIndex < 0)
      return cellIndex + tableSize;

    if(cellIndex >= tableSize)
      return cellIndex - tableSize;

    return cellIndex;
  }
  
  private static int cellIndex(final int hash, final int tableSize) {
    return (tableSize - 1) & hash;
  }

  private void moveValues(final int bucketCellIndex, final int fromCellIndex, final int toCellIndex) {
    final Cell bucketCell = cells[bucketCellIndex];
    final Cell fromCell = cells[fromCellIndex];

    Cell toCell = cells[toCellIndex];
    if (toCell == null) {
      toCell = new Cell();
      cells[toCellIndex] = toCell;
    }

    toCell.value = fromCell.value;
    fromCell.value = null;
    final int fromDistance = (byte) distance(bucketCellIndex, fromCellIndex, cells.length);
    if (bucketCell.firstDistance == fromDistance)
      bucketCell.firstDistance = (byte) distance(bucketCellIndex, toCellIndex, cells.length);
    else {
      final int lastCellIndex = findLastBucketCell(bucketCellIndex);
      final Cell lastCell = cells[lastCellIndex];

      lastCell.nextDistance = (byte)calculateNextCellDistance(bucketCellIndex, lastCellIndex, toCellIndex, cells.length);
      int t = 1;
    }

    if (fromCell.firstDistance == EMPTY_BUCKET)
      cells[fromCellIndex] = null;
  }

  private int[] findClosestCellToMove(final int index) {
    int beginWith = index - HOPSCOTCH_DISTANCE + 1;
    if (beginWith > 0) {
      for (int i = beginWith; i < index; i++) {
        final Cell cell = cells[i];
        if (cell.firstDistance != EMPTY_BUCKET) {
          final int closestIndex = cell.firstDistance + i;
          if (closestIndex < index)
            return new int[]{i, closestIndex};
        }
      }

      return null;
    } else {
      for (int i = cells.length + beginWith; i < cells.length; i++) {
        final Cell cell = cells[i];
        if (cell.firstDistance != EMPTY_BUCKET) {
          final int firstDistance = cell.firstDistance;
          if (firstDistance + i < cells.length)
            return new int[]{i, firstDistance + i};

          if (firstDistance + i - cells.length < beginWith)
            return new int[]{i, firstDistance + i - cells.length};
        }
      }

      for (int i = 0; i < index; i++) {
        final Cell cell = cells[i];
        if (cell.firstDistance != EMPTY_BUCKET) {
          final int closestIndex = cell.firstDistance + i;
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

  private static int distance(final int fromIndex, final int toIndex, final int tableSize) {
    if (toIndex > fromIndex)
      return toIndex - fromIndex;
    return toIndex - fromIndex + tableSize;
  }

  private void rehash() {
    final Cell[] oldCells = cells;
    cells = new Cell[oldCells.length << 1];
    threshold = (int) Math.ceil(cells.length * LOAD_FACTOR);
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
        if (processedItems >= size)
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