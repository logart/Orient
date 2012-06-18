package com.orientechnologies.common.types;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.06.12
 */
public class OUnsafeBinaryConverter implements OBinaryConverter {
  private static final Unsafe theUnsafe;
  private static final long  BYTE_ARRAY_OFFSET;

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (NoSuchFieldException e) {
          throw new Error();
        } catch (IllegalAccessException e) {
          throw new Error();
        }
      }
    });
    BYTE_ARRAY_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
  }

	public void putShort(byte[] buffer, int index, short value) {
		theUnsafe.putShort(buffer, index + BYTE_ARRAY_OFFSET, value);
	}

	public short getShort(byte[] buffer, int index) {
		return theUnsafe.getShort(buffer,index + BYTE_ARRAY_OFFSET);
	}

	public void putInt(byte[] buffer, int pointer, int offset, int value) {
    final long position = pointer + offset + BYTE_ARRAY_OFFSET;
    theUnsafe.putInt(buffer, position, value);
  }

  public int getInt(byte[] buffer, int pointer, int offset) {
    final long position = pointer + offset + BYTE_ARRAY_OFFSET;
    return theUnsafe.getInt(buffer, position);
  }

	public void putLong(byte[] buffer, int index, long value) {
		theUnsafe.putLong(buffer, index + BYTE_ARRAY_OFFSET, value);
	}

	public long getLong(byte[] buffer, int index) {
		return theUnsafe.getLong(buffer, index + BYTE_ARRAY_OFFSET);
	}

	public void putChar(byte[] buffer, int index, char character) {
		theUnsafe.putChar(index + BYTE_ARRAY_OFFSET, character);
	}

	public char getChar(byte[] buffer, int index) {
		return theUnsafe.getChar(buffer, index + BYTE_ARRAY_OFFSET);
	}
}
