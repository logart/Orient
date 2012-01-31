package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.id.ORID;


import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OHopscotchHashSet extends AbstractSet<ORID> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final byte HOPSCOTCH_DISTANCE = 64;
  private static final int END_OF_CHAIN = -1;
  private static final double LOAD_FACTOR = 0.85;

  private static final class Cell {
    private int nextPosition = END_OF_CHAIN;
    private int firstPosition = END_OF_CHAIN;
    private int size = 0;
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
    threshold = (int) Math.ceil(capacity * LOAD_FACTOR);
  }

  @Override
  public boolean contains(final Object o) {
    final ORID value = (ORID) o;

    final int index = cellIndex(value.hashCode(), cells.length);

    assert index > -1 && index < cells.length;
    final Cell cell = cells[index];
    if (cell == null || cell.size == 0){
      return false;
    }

    Cell cellToMatch;
    int cellIndex = cell.firstPosition;

    do {
      assert cellIndex > -1 && cellIndex < cells.length;
      cellToMatch = cells[cellIndex];
      assert value != null;
      assert cellToMatch != null;
      if (value.equals(cellToMatch.value)) {
        return true;
      }
      cellIndex = cellToMatch.nextPosition;

    } while (cellIndex != END_OF_CHAIN);

    return false;
  }

  @Override
  public boolean add(ORID value) {
    if (contains(value)){
      return false;
    }

    final int index = cellIndex(value.hashCode(), cells.length);

    assert index > -1 && index<cells.length;
    Cell bucketCell = cells[index];
    if (bucketCell == null) {

      bucketCell = new Cell();
      assert index > -1 && index<cells.length;
      cells[index] = bucketCell;

      bucketCell.firstPosition = index;
      bucketCell.size++;
      bucketCell.value = value;

      size++;

      if (size > threshold)
        rehash();

      return true;
    }

    final int emptyIndex = findFirstEmptyIndex(index);

//    if (emptyIndex == -1) {
//      rehash();
//      return add(value);
//    }

    int distance = distance(index, emptyIndex, cells.length);
    if (distance < HOPSCOTCH_DISTANCE) {
      assert emptyIndex > -1;
      assert emptyIndex<cells.length;
      Cell emptyCell = cells[emptyIndex];

      if (emptyCell == null) {
        emptyCell = new Cell();
        assert emptyIndex > -1 && emptyIndex<cells.length;
        cells[emptyIndex] = emptyCell;
      }
      emptyCell.value = value;

      if (bucketCell.size == 0) {
        bucketCell.firstPosition = emptyIndex;
      } else {
        final int lastCellIndex = findLastBucketCell(index);
        assert lastCellIndex > -1 && lastCellIndex<cells.length;
        final Cell lastCell = cells[lastCellIndex];
        lastCell.nextPosition = emptyIndex;
      }

      bucketCell.size++;

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
        assert cellMoveIndex > -1 && cellMoveIndex<cells.length;
        if (cells[cellMoveIndex] == null){
          assert cellMoveIndex > -1 && cellMoveIndex<cells.length;
          cells[cellMoveIndex] = new Cell();
        }

        assert cellMoveIndex > -1 && cellMoveIndex<cells.length;
        final Cell cellToSet = cells[cellMoveIndex];

        assert cellToSet != null;

        cellToSet.value = value;

        if (bucketCell.size == 0) {
          bucketCell.firstPosition = cellMoveIndex;
        } else {
          final int lastCellIndex = findLastBucketCell(index);
          assert lastCellIndex > -1 && lastCellIndex<cells.length;
          final Cell lastCell = cells[lastCellIndex];
          lastCell.nextPosition = cellMoveIndex;
        }

        bucketCell.size++;
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

    assert index > -1 && index < cells.length;
    final Cell bucketCell = cells[index];
    if (bucketCell == null || bucketCell.size == 0)
      return false;

    Cell cellToMatch;
    int cellIndex = bucketCell.firstPosition;
    int prevCellIndex = -1;

    do {
      assert cellIndex > -1 && cellIndex < cells.length;
      cellToMatch = cells[cellIndex];
      assert value != null;
      assert cellToMatch != null;
      if (value.equals(cellToMatch.value)) {
        if (prevCellIndex != -1) {
          assert prevCellIndex > -1 && prevCellIndex < cells.length;
          final Cell prevCell = cells[prevCellIndex];
          if (cellToMatch.nextPosition != END_OF_CHAIN) {
            prevCell.nextPosition = cellToMatch.nextPosition;
          }else{
            prevCell.nextPosition = END_OF_CHAIN;
          }
        } else {
          if(cellToMatch.nextPosition != END_OF_CHAIN) {
            final int nextCellIndex = cellToMatch.nextPosition;
            bucketCell.firstPosition = nextCellIndex;
          } else {
            if(bucketCell.value == null){
              assert index > -1 && index < cells.length;
              cells[index] = null;
            }else{
//              bucketCell.size = 0;
              bucketCell.firstPosition = END_OF_CHAIN;
            }
          }
        }

        if(cellToMatch.size == 0){
          assert cellIndex > -1 && cellIndex < cells.length;
          cells[cellIndex] = null;
        }else{
          cellToMatch.value = null;
          cellToMatch.nextPosition = END_OF_CHAIN;
        }

        bucketCell.size--;

        size--;
        return true;
      }

      prevCellIndex = cellIndex;
      cellIndex = cellToMatch.nextPosition;
    }

    while (cellToMatch.nextPosition != END_OF_CHAIN);

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
    assert bucketCellIndex > -1 && bucketCellIndex < cells.length;
    final Cell bucketCell = cells[bucketCellIndex];
    int cellIndex = bucketCell.firstPosition;
    assert cellIndex > -1 && cellIndex < cells.length;
    Cell cell = cells[cellIndex];
    while (cell.nextPosition != END_OF_CHAIN) {
      cellIndex = cell.nextPosition;
      assert cellIndex > -1 && cellIndex < cells.length;
      cell = cells[cellIndex];
    }

    return cellIndex;
  }

//  private static int calculateNextCellDistance(final int bucketIndex, final int lastCellIndex,
//                                               final int cellToAddIndex, final int tableSize) {
//    if(cellToAddIndex < bucketIndex && lastCellIndex > bucketIndex || cellToAddIndex > bucketIndex && lastCellIndex < bucketIndex)
//      return tableSize - lastCellIndex + cellToAddIndex;
//    return cellToAddIndex - lastCellIndex;
//  }

//  private static int normalizeCellIndex(final int cellIndex, final int tableSize) {
//    if(cellIndex < 0)
//      return cellIndex + tableSize;
//
//    if(cellIndex >= tableSize)
//      return cellIndex - tableSize;
//
//    return cellIndex;
//  }
  
  private static int cellIndex(final int hash, final int tableSize) {
    return (tableSize - 1) & hash;
  }

  //проработать внимательнее
  private void moveValues(final int bucketCellIndex, final int fromCellIndex, final int toCellIndex) {
    assert bucketCellIndex > -1 && bucketCellIndex < cells.length;
    final Cell bucketCell = cells[bucketCellIndex];
    assert fromCellIndex > -1 && fromCellIndex < cells.length;
    final Cell fromCell = cells[fromCellIndex];
    assert toCellIndex > -1 && toCellIndex < cells.length;
    Cell toCell = cells[toCellIndex];

    if (toCell == null) {
      toCell = new Cell();
      assert toCellIndex > -1 && toCellIndex < cells.length;
      cells[toCellIndex] = toCell;
    }

    toCell.value = fromCell.value;
    fromCell.value = null;

     if (bucketCell.firstPosition == fromCellIndex)
      bucketCell.firstPosition = toCellIndex;
    else {
      final int lastCellIndex = findPrevBucketCell(bucketCell.firstPosition, fromCellIndex);
      final Cell lastCell = cells[lastCellIndex];

      lastCell.nextPosition = toCellIndex;
    }

//    уточнить так ли это
    if (fromCell.size == 0)
         cells[fromCellIndex] = null;
  }

    private int findPrevBucketCell(int firstCellIndex, int currentCellIndex){

        if (firstCellIndex == currentCellIndex){
            throw new RuntimeException("bucket cell does not have previous cell");
        }

      assert firstCellIndex > -1 && firstCellIndex < cells.length;
      int cellIndex = firstCellIndex;
      assert cellIndex > -1 && cellIndex < cells.length;
        Cell cell = cells[cellIndex];
        while ((cell.nextPosition != currentCellIndex) && (cell.nextPosition!= END_OF_CHAIN)) {
            cellIndex = cell.nextPosition;
          assert cellIndex > -1 && cellIndex < cells.length;
            cell = cells[cellIndex];
        }

        return cellIndex;
    }

  //review and optimize
  private int[] findClosestCellToMove(final int index) {
    int beginWith = index - HOPSCOTCH_DISTANCE + 1;
    if (beginWith >= 0) {
      for (int i = beginWith; i < index; i++) {
        assert i > -1 && i < cells.length;
        final Cell cell = cells[i];
        assert cell != null;
        //first position or size to determine bucket
        if (cell.size > 0) {
          final int closestIndex = cell.firstPosition;
          if (closestIndex < index)
            return new int[]{i, closestIndex};
        }
      }

      return null;
    } else {
      for (int i = cells.length + beginWith; i < cells.length; i++) {
        assert i > -1 && i < cells.length;
        final Cell cell = cells[i];
        assert cell != null;
        if (cell.size > 0) {
          final int closestIndex = cell.firstPosition;
          if (closestIndex < index)
            return new int[]{i, closestIndex};

//          if (firstPosition - cells.length < beginWith)
//            return new int[]{i, firstPosition};
        }
      }

      for (int i = 0; i < index; i++) {
        assert i > -1 && i < cells.length;
        final Cell cell = cells[i];
        assert cell != null;
        if (cell.size > 0) {
          final int closestIndex = cell.firstPosition;
          if (closestIndex < index)
            //why this values are swapped?
            return new int[]{i, closestIndex};
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
    assert index > -1 && index < cells.length;
    final Cell cell = cells[index];
    return cell == null || cell.value == null;
  }

  private static int distance(final int fromIndex, final int toIndex, final int tableSize) {
    if (toIndex >= fromIndex)
      return toIndex - fromIndex;
    return toIndex - fromIndex + tableSize;
  }

  private void rehash() {
    final Cell[] oldCells = cells;
    assert oldCells.length << 1 < Integer.MAX_VALUE;
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
        assert currentIndex > -1 && currentIndex < cells.length;
        while (currentIndex < cells.length && (cells[currentIndex] == null || cells[currentIndex].value == null))
          currentIndex++;

        assert currentIndex > -1 && currentIndex < cells.length;
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



//  public void correctBucket(int bucketCellIndex){
//    final Cell bucketCell = cells[bucketCellIndex];
//
//    Cell currentCell;
//    int currentCellIndex = normalizeCellIndex( bucketCell.firstDistance + bucketCellIndex, cells.length );
//
//    currentCell = cells[currentCellIndex];
//
//    while (currentCell.nextDistance != 0){
////      int prevCellIndex = currentCellIndex;
//      currentCellIndex = normalizeCellIndex( currentCellIndex + currentCell.nextDistance, cells.length );
//      currentCell = cells[currentCellIndex];
//      return;
//    }
//    return;
////    throw new RuntimeException( "bucket is incorrect" );
//  }
}