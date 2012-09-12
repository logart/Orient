package com.orientechnologies.orient.core.type.tree;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author <a href='mailto:enisher@gmail.com'>Artem Orobets</a>
 */
public class BitMapTest {
  @Test
  public void testMark() {
    final ODougLeaAllocator.BitMap bitMap = new ODougLeaAllocator.BitMap(0l);

    Assert.assertEquals(bitMap.getSet(), 0L);

    bitMap.mark(3);

    Assert.assertEquals(bitMap.getSet(), 8L);

    bitMap.mark(40);

    Assert.assertEquals(bitMap.getSet(), 1099511627784L);
  }

  @Test
  public void testClear() {

    final ODougLeaAllocator.BitMap bitMap = new ODougLeaAllocator.BitMap(1099511627784L);

    Assert.assertEquals(bitMap.getSet(), 1099511627784L);

    bitMap.clear(40);

    Assert.assertEquals(bitMap.getSet(), 8L);

    bitMap.clear(3);

    Assert.assertEquals(bitMap.getSet(), 0L);
  }

  @Test
  public void testIsMarked() {

    final ODougLeaAllocator.BitMap bitMap = new ODougLeaAllocator.BitMap(1099511627784L);

    Assert.assertEquals(bitMap.getSet(), 1099511627784L);

    Assert.assertTrue(bitMap.isMarked(40));
    Assert.assertTrue(bitMap.isMarked(3));

    Assert.assertEquals(bitMap.getSet(), 1099511627784L);
  }
}
