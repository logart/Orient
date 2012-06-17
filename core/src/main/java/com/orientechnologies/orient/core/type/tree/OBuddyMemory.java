package com.orientechnologies.orient.core.type.tree;

import java.util.Arrays;

/**
 * Buddy memory allocation algorithm.
 * <p/>
 * NOTE: Not multi-threaded implementation.
 *
 * @author Artem Orobets
 * @since 10.06.12
 */
public class OBuddyMemory implements OMemory {
	public static final int SYSTEM_INFO_SIZE = 6;

	private static final int TAG_OFFSET = 0;
	public static final int SIZE_OFFSET = 1;
	public static final int NEXT_OFFSET = 2;
	public static final int PREVIOUS_OFFSET = 6;

	public static final byte TAG_FREE = 0;
	public static final byte TAG_ALLOCATED = 1;

	private final byte[] buffer;

	private final int minChunkSize;
	private final int[] freeListHeader;
	private final int[] freeListTail;
	private final int maxLevel;

	/**
	 *
	 * @param capacity
	 * @param minChunkSize - size of chunks on level 0. Should be power of 2.
	 */
	public OBuddyMemory(int capacity, int minChunkSize) {
		this.minChunkSize = minChunkSize;

		capacity = (int) Math.pow(2, Math.floor(Math.log(capacity) / Math.log(2)));
		maxLevel = (int) (Math.log((double) capacity / minChunkSize) / Math.log(2));

		freeListHeader = new int[maxLevel + 1];
		freeListTail = new int[maxLevel + 1];
		buffer = new byte[capacity];

		initMemory();
	}

	private void initMemory() {
		Arrays.fill(freeListHeader, 0, maxLevel + 1, NULL_POINTER);
		Arrays.fill(freeListTail, 0, maxLevel + 1, NULL_POINTER);

		buffer[0] = TAG_FREE;
		buffer[1] = (byte) maxLevel;
		addNodeToTail(maxLevel, 0);
	}

	public int allocate(byte[] bytes) {
		int pointer = allocate(bytes.length);
		set(pointer, 0, bytes.length, bytes);
		return pointer;
	}

	public int allocate(final int size) {
		final int level;
		if (size + SYSTEM_INFO_SIZE > minChunkSize) {
			level = (int) Math.ceil(Math.log((double)(size + SYSTEM_INFO_SIZE) / minChunkSize) / Math.log(2));
		} else {
			level = 0;
		}

		if (level > maxLevel) {
			//We have no free space
			return NULL_POINTER;
		}

		int pointer = freeListHeader[level];
		if (pointer != NULL_POINTER) {
			removeNodeFromHead(level);
			buffer[pointer] = TAG_ALLOCATED;
		} else {
			int currentLevel = level + 1;
			while (freeListHeader[currentLevel] == NULL_POINTER) {
				currentLevel++;

				if (currentLevel > maxLevel) {
					//We have no free space
					return NULL_POINTER;
				}
			}

			pointer = removeNodeFromHead(currentLevel);

			do {
				pointer = split(pointer);
				currentLevel--;
				buffer[pointer + TAG_OFFSET] = (currentLevel == level) ? TAG_ALLOCATED : TAG_FREE;
				buffer[pointer + SIZE_OFFSET] = (byte) (currentLevel & 0xFF);
			} while (currentLevel > level);
		}

		return pointer;
	}

	private int split(int pointer) {
		int level = --buffer[pointer + SIZE_OFFSET];
		addNodeToTail(level, pointer);

		return buddy(pointer, level);
	}

	public void free(int pointer) {
		int level = buffer[pointer + SIZE_OFFSET];
		int buddy = buddy(pointer, level);
		while (level < maxLevel && buffer[buddy + TAG_OFFSET] == TAG_FREE && buffer[buddy + SIZE_OFFSET] == level) {
			removeFromFreeList(level, buddy);

			if (buddy < pointer) {
				pointer = buddy;
			}
			level++;
			buddy = buddy(pointer, level);
		}

		buffer[pointer + TAG_OFFSET] = TAG_FREE;
		buffer[pointer + SIZE_OFFSET] = (byte) (level & 0xFF);
		addNodeToTail(level, pointer);
	}

	private void removeFromFreeList(int level, int pointer) {
		final int next = next(pointer);
		final int previous = previous(pointer);

		if (freeListHeader[level] == pointer) {
			freeListHeader[level] = next;
		} else {
			next(previous, next);
		}

		if (freeListTail[level] == pointer) {
			freeListTail[level] = previous;
		} else {
			previous(next, previous);
		}
	}

	public byte[] get(int pointer, int offset, final int length) {
		final int newLength;
		if (length > 0) {
			newLength = Math.min(length, size(pointer) - offset);
		} else {
			newLength = size(pointer) - offset;
		}

		try {
			byte[] dest = new byte[newLength];
			System.arraycopy(buffer, pointer + SYSTEM_INFO_SIZE + offset, dest, 0, newLength);
			return dest;
		} catch (RuntimeException e) {
			System.out.println("pointer=" + pointer + ", offset=" + offset + ", length=" + length + ", " + (buffer.length < pointer + SYSTEM_INFO_SIZE + offset + length));
			throw e;
		}
	}

	private int size(int pointer) {
//		return readInt(pointer, ACTUAL_SIZE_OFFSET);
		return (1 << buffer[pointer + SIZE_OFFSET]) * minChunkSize - SYSTEM_INFO_SIZE;
	}


	public void set(int pointer, int offset, int length, byte[] content) {
		System.arraycopy(content, 0, buffer, pointer + SYSTEM_INFO_SIZE + offset, length);
	}

	public int capacity() {
		return buffer.length;
	}

	public int freeSpace() {
		int freeSpace = 0;
		for (int level = 0; level <= maxLevel; level++) {
			int count = 0;
			int pointer = freeListHeader[level];
			while (pointer != NULL_POINTER) {
				count++;
				pointer = next(pointer);
			}

			freeSpace += (1 << level) * count;
		}
		freeSpace *= minChunkSize;

		return freeSpace;
	}

	public void clear() {
		initMemory();
	}

	public void setInt(int pointer, int offset, int value) {
		writeInt(pointer, offset + SYSTEM_INFO_SIZE, value);
	}

	public int getInt(int pointer, int offset) {
		return readInt(pointer, offset + SYSTEM_INFO_SIZE);
	}

	private int buddy(int pointer, int level) {
		//TODO optimize
		return pointer ^ ((1 << level) * minChunkSize);
	}

	private int removeNodeFromHead(int level) {
		int pointer = freeListHeader[level];

		freeListHeader[level] = next(pointer);
		if (freeListHeader[level] != NULL_POINTER) {
			previous(freeListHeader[level], NULL_POINTER);
		} else {
			freeListTail[level] = NULL_POINTER;
		}
		return pointer;
	}

	private void addNodeToTail(int level, int pointer) {
		next(pointer, NULL_POINTER);
		previous(pointer, freeListTail[level]);
		if (freeListTail[level] == NULL_POINTER) {
			freeListHeader[level] = pointer;
		} else {
			next(freeListTail[level], pointer);
		}
		freeListTail[level] = pointer;
	}

	private int next(int pointer) {
		return readInt(pointer, NEXT_OFFSET);
	}

	private void next(int pointer, int next) {
		writeInt(pointer, NEXT_OFFSET, next);
	}

	private int previous(int pointer) {
		return readInt(pointer, PREVIOUS_OFFSET);
	}

	private void previous(int pointer, int next) {
		writeInt(pointer, PREVIOUS_OFFSET, next);
	}

	private void writeInt(int pointer, int offset, int value) {
		final int position = pointer + offset;

		buffer[position + 0] = (byte) ((value >>> 24) & 0xFF);
		buffer[position + 1] = (byte) ((value >>> 16) & 0xFF);
		buffer[position + 2] = (byte) ((value >>> 8) & 0xFF);
		buffer[position + 3] = (byte) ((value >>> 0) & 0xFF);
	}

	private int readInt(int pointer, int offset) {
		final int position = pointer + offset;

		final int v1 = buffer[position + 0] & 0xFF;
		final int v2 = buffer[position + 1] & 0xFF;
		final int v3 = buffer[position + 2] & 0xFF;
		final int v4 = buffer[position + 3] & 0xFF;

		return ((v1 << 24) + (v2 << 16) + (v3 << 8) + (v4 << 0));
	}
}
