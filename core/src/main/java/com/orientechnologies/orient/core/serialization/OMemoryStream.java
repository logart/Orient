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
package com.orientechnologies.orient.core.serialization;

import java.io.IOException;
import java.io.OutputStream;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.OConstants;

/**
 * Class to parse and write buffers in very fast way.
 * 
 * @author Luca Garulli
 * 
 */
public class OMemoryStream extends OutputStream {
	private byte[]						buffer;
	private int								position;

	private static final int	NATIVE_COPY_THRESHOLD	= 9;
	private static final int	DEF_SIZE							= 1024;

	// private int fixedSize = 0;

	public OMemoryStream() {
		this(DEF_SIZE);
	}

	/**
	 * Callee takes ownership of 'buf'.
	 */
	public OMemoryStream(final int initialCapacity) {
		buffer = new byte[initialCapacity];
	}

	public OMemoryStream(byte[] stream) {
		buffer = stream;
	}

	public final void writeTo(final OutputStream out) throws IOException {
		out.write(buffer, 0, position);
	}

	public final byte[] getByteArray() {
		position = 0;
		return buffer;
	}

	/**
	 * 
	 * @return [result.length = size()]
	 */
	public final byte[] toByteArray() {
		final int pos = position;

		final byte[] result = new byte[pos];
		final byte[] mbuf = buffer;

		if (pos < NATIVE_COPY_THRESHOLD)
			for (int i = 0; i < pos; ++i)
				result[i] = mbuf[i];
		else
			System.arraycopy(mbuf, 0, result, 0, pos);

		return result;
	}

	/**
	 * Does not reduce the current capacity.
	 */
	public final void reset() {
		position = 0;
	}

	// OutputStream:

	@Override
	public final void write(final int b) {
		assureSpaceFor(OConstants.SIZE_BYTE);
		buffer[position++] = (byte) b;
	}

	@Override
	public final void write(final byte[] iBuffer, final int iOffset, final int iLength) {
		final int pos = position;
		final int tot = pos + iLength;

		assureSpaceFor(iLength);

		byte[] mbuf = buffer;

		if (iLength < NATIVE_COPY_THRESHOLD)
			for (int i = 0; i < iLength; ++i)
				mbuf[pos + i] = iBuffer[iOffset + i];
		else
			System.arraycopy(iBuffer, iOffset, mbuf, pos, iLength);

		position = tot;
	}

	/**
	 * Equivalent to {@link #reset()}.
	 */
	@Override
	public final void close() {
		reset();
	}

	public final void addAsFixed(final byte[] iContent) {
		if (iContent == null)
			return;
		write(iContent, 0, iContent.length);
	}

	/**
	 * Append byte[] to the stream.
	 * 
	 * @param iContent
	 * @return The begin offset of the appended content
	 * @throws IOException
	 */
	public int add(final byte[] iContent) {
		if (iContent == null)
			return -1;

		final int begin = position;

		assureSpaceFor(OConstants.SIZE_INT + iContent.length);

		OBinaryProtocol.int2bytes(iContent.length, buffer, position);
		position += OConstants.SIZE_INT;
		write(iContent, 0, iContent.length);

		return begin;
	}

	public void remove(final int iBegin, final int iEnd) {
		if (iBegin > iEnd)
			throw new IllegalArgumentException("Begin is bigger than end");

		if (iEnd > buffer.length)
			throw new IndexOutOfBoundsException("Position " + iEnd + " is out of buffer length (" + buffer.length + ")");

		System.arraycopy(buffer, iEnd, buffer, iBegin, buffer.length - iEnd);
	}

	public void add(final byte iContent) {
		write(iContent);
	}

	public final int add(final String iContent) {
		return add(OBinaryProtocol.string2bytes(iContent));
	}

	public void add(final boolean iContent) {
		write(iContent ? 1 : 0);
	}

	public void add(final char iContent) {
		assureSpaceFor(OConstants.SIZE_CHAR);
		OBinaryProtocol.char2bytes(iContent, buffer, position);
		position += OConstants.SIZE_CHAR;
	}

	public void add(final int iContent) {
		assureSpaceFor(OConstants.SIZE_INT);
		OBinaryProtocol.int2bytes(iContent, buffer, position);
		position += OConstants.SIZE_INT;
	}

	public int add(final long iContent) {
		assureSpaceFor(OConstants.SIZE_LONG);
		final int begin = position;
		OBinaryProtocol.long2bytes(iContent, buffer, position);
		position += OConstants.SIZE_LONG;
		return begin;
	}

	public int add(final short iContent) {
		assureSpaceFor(OConstants.SIZE_SHORT);
		final int begin = position;
		OBinaryProtocol.short2bytes(iContent, buffer, position);
		position += OConstants.SIZE_SHORT;
		return begin;
	}

	public int getPosition() {
		return position;
	}

	private void assureSpaceFor(final int iLength) {
		final byte[] mbuf = buffer;
		final int pos = position;
		final int capacity = position + iLength;

		final int mbuflen = mbuf.length;

		if (mbuflen <= capacity) {
			OProfiler.getInstance().updateCounter("OMemOutStream.resize", +1);

			final byte[] newbuf = new byte[Math.max(mbuflen << 1, capacity)];

			if (pos < NATIVE_COPY_THRESHOLD)
				for (int i = 0; i < pos; ++i)
					newbuf[i] = mbuf[i];
			else
				System.arraycopy(mbuf, 0, newbuf, 0, pos);

			buffer = newbuf;
		}
	}

	/**
	 * Jumps bytes positioning forward of passed bytes.
	 * 
	 * @param iLength
	 *          Bytes to jump
	 */
	public void fill(final int iLength) {
		assureSpaceFor(iLength);
		position += iLength;
	}

	public OMemoryStream jump(final int iOffset) {
		if (iOffset > buffer.length)
			throw new IndexOutOfBoundsException("Offset " + iOffset + " is out of bound of buffer size " + buffer.length);
		position = iOffset;
		return this;
	}

	public byte[] getAsByteArrayFixed(final int iSize) {
		if (position >= buffer.length)
			return null;

		final byte[] portion = OArrays.copyOfRange(buffer, position, position + iSize);
		position += iSize;

		return portion;
	}

	/**
	 * Browse the stream but just return the begin of the byte array. This is used to lazy load the information only when needed.
	 * 
	 */
	public int getAsByteArrayOffset() {
		if (position >= buffer.length)
			return -1;

		final int begin = position;

		final int size = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT + size;

		return begin;
	}

	public int read() {
		return buffer[position++];
	}

	public int read(final byte[] b) {
		return read(b, 0, b.length);
	}

	public int read(final byte[] b, final int off, final int len) {
		if (position >= buffer.length)
			return 0;

		System.arraycopy(buffer, position, b, off, len);
		position += len;

		return len;
	}

	public byte[] getAsByteArray(int iOffset) {
		if (buffer == null || iOffset >= buffer.length)
			return null;

		final int size = OBinaryProtocol.bytes2int(buffer, iOffset);

		if (size == 0)
			return null;

		iOffset += OConstants.SIZE_INT;

		return OArrays.copyOfRange(buffer, iOffset, iOffset + size);
	}

	public byte[] getAsByteArray() {
		if (position >= buffer.length)
			return null;

		final int size = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT;

		final byte[] portion = OArrays.copyOfRange(buffer, position, position + size);
		position += size;

		return portion;
	}

	public String getAsString() {
		final int size = getVariableSize();
		if (size < 0)
			return null;
		return OBinaryProtocol.bytes2string(this, size);
	}

	public boolean getAsBoolean() {
		return buffer[position++] == 1;
	}

	public char getAsChar() {
		final char value = OBinaryProtocol.bytes2char(buffer, position);
		position += OConstants.SIZE_CHAR;
		return value;
	}

	public byte getAsByte() {
		return buffer[position++];
	}

	public long getAsLong() {
		final long value = OBinaryProtocol.bytes2long(buffer, position);
		position += OConstants.SIZE_LONG;
		return value;
	}

	public int getAsInteger() {
		final int value = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT;
		return value;
	}

	public short getAsShort() {
		final short value = OBinaryProtocol.bytes2short(buffer, position);
		position += OConstants.SIZE_SHORT;
		return value;
	}

	public byte peek() {
		return buffer[position];
	}

	public void setSource(final byte[] iBuffer) {
		buffer = iBuffer;
		position = 0;
	}

	public byte[] copy() {
		if (buffer == null)
			return null;

		final int size = position > 0 ? position : buffer.length;

		final byte[] copy = new byte[size];
		System.arraycopy(buffer, 0, copy, 0, size);
		return copy;
	}

	public int getVariableSize() {
		if (position >= buffer.length)
			return -1;

		final int size = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT;

		return size;
	}

	public int getSize() {
		return buffer.length;
	}

	public final int size() {
		return position;
	}
}
