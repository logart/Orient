package com.orientechnologies.orient.core.type.tree;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Unit test for {@link ODougLeaAllocator}.
 *
 * @author <a href='mailto:enisher@gmail.com'>Artem Orobets</a>
 */
public class ODougLeaAllocatorTest {

  public static final int _200_BYTES = 200;

  @Test
  public void testAllocatorInitialization() {
    final int memorySize = 1024 + 256 + 32 + 16;
    final ODougLeaAllocator memory = new ODougLeaAllocator(memorySize);
    final int availableSpace = memory.checkStructure();
    assertEquals(memorySize, availableSpace);
  }

  @Test
  public void allocatedPointerNotNull() throws Exception {
    final ODougLeaAllocator memory = new ODougLeaAllocator(2048);
    final int pointer = memory.allocate(_200_BYTES);
    assertTrue(pointer != OMemory.NULL_POINTER);
  }

  @Test
  public void allocatedPointersAreDifferent() {
    final ODougLeaAllocator memory = new ODougLeaAllocator(2048);

    Set<Integer> pointers = new HashSet<Integer>();
    while (memory.freeSpace() > _200_BYTES) {
      final int p = memory.allocate(_200_BYTES);
      assertFalse(pointers.contains(p));
      pointers.add(p);
    }
  }
}
