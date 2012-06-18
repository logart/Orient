/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.common.types.OBinaryConverter;
import com.orientechnologies.common.types.OBinaryConverterFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#FLOAT}
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OFloatSerializer implements OBinarySerializer<Float> {
	private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();

	public static OFloatSerializer INSTANCE = new OFloatSerializer();
	public static final byte ID = 7;

	/**
	 * size of float value in bytes
	 */
	public static final int FLOAT_SIZE = 4;

	public int getObjectSize(Float object) {
		return FLOAT_SIZE;
	}

	public void serialize(Float object, byte[] stream, int startPosition) {
		int2bytes(Float.floatToIntBits(object), stream, startPosition);
	}

	public Float deserialize(byte[] stream, int startPosition) {
		return Float.intBitsToFloat(bytes2int(stream, startPosition));
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return FLOAT_SIZE;
	}

	public byte getId() {
		return ID;
	}

	public int getObjectSizeNative(byte[] stream, int startPosition) {
		return FLOAT_SIZE;
	}

	public void serializeNative(Float object, byte[] stream, int startPosition) {
		CONVERTER.putInt(stream, startPosition,0, Float.floatToIntBits(object));
	}

	public Float deserializeNative(byte[] stream, int startPosition) {
		return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, 0));
	}
}


