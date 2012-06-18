package com.orientechnologies.common.types;

/**
* @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
* @since 18.06.12
*/
public interface OBinaryConverter {
	void putInt(byte[] buffer, int pointer, int offset, int value);

	int getInt(byte[] buffer, int pointer, int offset);

	void putShort(byte[] buffer, int index, short value);

	short getShort(byte[] buffer, int index);

	void putLong(byte[] buffer, int index, long value);

	long getLong(byte[] buffer, int index);

	void putChar(byte[] buffer, int index, char character);

	char getChar(byte[] buffer, int index);
}