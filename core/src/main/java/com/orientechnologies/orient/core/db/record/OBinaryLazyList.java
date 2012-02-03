package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Represent list that load its elements lazily
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinaryLazyList<T> extends AbstractList<T> {

    /**
     * Holder for the elements. Contains its position ad size in the binary source
     * @param <T>
     */
    protected class LazyEntry<T> {

        private static final int DIRTY = -1;
        private int sourcePosition;
        private int size;
        private T value;

        protected LazyEntry(final int sourcePosition, final int size) {
            this(sourcePosition, size, null);
        }

        protected LazyEntry(final T value) {
            this(DIRTY, DIRTY, value);
        }

        protected LazyEntry(final int sourcePosition, final int size, final T value) {
            this.sourcePosition = sourcePosition;
            this.size = size;
            this.value = value;
        }

        public boolean isDirty() {
            return sourcePosition == DIRTY;
        }

        public void init() {
            final ObjectSerializer<T> objectSerializer = ObjectSerializerFactory.INSTANCE.getObjectSerializer(source[sourcePosition]);
            this.value = objectSerializer.deserialize(source, sourcePosition + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE);
        }

        public void set(T value) {
            this.sourcePosition = DIRTY;
            this.value = value;
        }

        public T get() {
            if (sourcePosition != DIRTY && value == null) {
                init();
            }
            return value;
        }

        public T getForUpdate() {
            final T value = get();
            sourcePosition = DIRTY;
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final T thisValue = get();
            final T otherValue = ((LazyEntry<T>) o).get();

            if (thisValue == null) {
                return otherValue == null;
            } else {
                return thisValue.equals(otherValue);
            }
        }

        @Override
        public int hashCode() {
            final T thisValue = get();
            return thisValue != null ? thisValue.hashCode() : 0;
        }
    }

    private boolean isDirty;
    private final int sourceOffset;
    private final byte[] source;
    private final ArrayList<LazyEntry<T>> data = new ArrayList<LazyEntry<T>>();

    public OBinaryLazyList() {
        this(null, -1);
    }

    /**
     * Initialize list
     *
     * @param source records source
     * @param sourceOffset offset of the list in the source
     */
    public OBinaryLazyList(final byte[] source, final int sourceOffset) {
        this.sourceOffset = sourceOffset;
        this.source = source;
        if (source != null) {
            final ObjectSerializer<Integer> indexSerializer = ObjectSerializerFactory.INSTANCE.getIndexSerializer();
            final int initialSize = indexSerializer.deserialize(source, sourceOffset);
            final int indexSize = indexSerializer.getFieldSize(null);
            int indexOffset = sourceOffset + indexSize;
            int dataOffset = indexOffset + indexSize * initialSize;
            for(int i=0; i<initialSize; i++){
                final int size = indexSerializer.deserialize(source, indexOffset);
                data.add(new LazyEntry<T>(dataOffset, size));
                indexOffset += indexSize;
                dataOffset += size;
            }
            this.isDirty = false;
        } else {
            this.isDirty = true;
        }
    }

    /**
     * Serialize list into the stream
     *
     * @param stream to serialize list
     * @param streamOffset position to start serialization from
     */
    public void toStream(final byte[] stream, final int streamOffset) {
        if (isDirty) {
            final ObjectSerializer<Integer> indexSerializer = ObjectSerializerFactory.INSTANCE.getIndexSerializer();
            final int indexSize = indexSerializer.getFieldSize(null);
            final int size = data.size();
            indexSerializer.serialize(size, stream, streamOffset);
            int indexOffset = streamOffset + indexSize;
            int dataOffset = indexOffset + size * indexSize;
            for (int i = 0; i < size; i++) {
                final int objSize = getObjectSize(i);
                indexSerializer.serialize(objSize, stream, indexOffset);
                indexOffset += indexSize;
                if (!data.get(i).isDirty()) {
                    System.arraycopy(source,data.get(i).sourcePosition,stream,dataOffset,data.get(i).size);
                } else {
                    final T obj = data.get(i).get();
                    final OType type = OType.getTypeByClass(obj.getClass());
                    stream[dataOffset] = type.getByteId();
                    ObjectSerializerFactory.INSTANCE.getObjectSerializer(type).serialize(data.get(i).get(), stream, dataOffset + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE);
                }
                dataOffset += objSize;
            }
        } else {
            //if list has no changes just copy it from the source
            final int sizeToCopy = getStreamSize();
            System.arraycopy(source, sourceOffset, stream, streamOffset, sizeToCopy);
        }
    }

    /**
     * Calculates amount of bytes that is required to serialize this list
     *
     * @return required amount of bytes to serialize list
     */
    public int getStreamSize() {
        final int indexSize = ObjectSerializerFactory.INSTANCE.getIndexSerializer().getFieldSize(null);
        int size = indexSize * (data.size() + 1);
        for(int i=0; i<data.size(); i++){
            size += getObjectSize(i);
        }
        return size;
    }

    /**
     * Obtains size of the object
     *
     * @param index of the object in the list
     * @return
     */
    protected int getObjectSize(final int index) {
        final LazyEntry<T> entry = data.get(index);
        if(!entry.isDirty()){
            return entry.size;
        }else{
            final T obj = entry.get();
            return ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE +
                    ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.getTypeByClass(obj.getClass())).getFieldSize(obj);
        }
    }

    /**
     * Determines weather list modified or not
     *
     * @return {@code true} if list has been modified or {@code false} otherwise
     */
    public boolean isDirty() {
        return isDirty;
    }

    @Override
    public boolean add(T t) {
        isDirty = true;
        return data.add(new LazyEntry<T>(t));
    }

    @Override
    public T set(int index, T element) {
        isDirty = true;
        final T toReturn = get(index);
        data.set(index, new LazyEntry<T>(element));
        return toReturn;
    }

    @Override
    public void add(int index, T element) {
        isDirty = true;
        data.add(index, new LazyEntry<T>(element));
    }

    @Override
    public T remove(int index) {
        isDirty = true;
        final T toReturn = get(index);
        data.remove(index);
        return toReturn;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {

            private final Iterator<LazyEntry<T>> iterator = data.iterator();

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public T next() {
                return iterator.next().get();
            }

            public void remove() {
                isDirty = true;
                iterator.remove();
            }
        };
    }

    @Override
    public T get(int index) {
        return data.get(index).get();
    }

    /**
     * Obtain mutable record for its further modification
     *
     * @param index of the element to return
     * @return element for further modification
     */
    public T getForUpdate(int index) {
        isDirty = true;
        return data.get(index).getForUpdate();
    }

    @Override
    public int size() {
        return data.size();
    }
}
