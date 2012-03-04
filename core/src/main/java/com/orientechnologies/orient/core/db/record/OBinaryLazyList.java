package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializable;
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
public class OBinaryLazyList<T> extends AbstractList<T> implements OBinarySerializable, ORecordElement {

    /**
     * Holder for the elements. Contains its position ad size in the binary source
     *
     * @param <T>
     */
    protected class LazyEntry<T> {

        protected static final int DIRTY = -1;
        protected int sourcePosition;
        protected int size;
        protected T value;

        /**
         * Do not create entries directly. Use {@link OBinaryLazyList#createEntry(int, int)}
         */
        protected LazyEntry(final int sourcePosition, final int size) {
            this(sourcePosition, size, null);
        }

        /**
         * Do not create entries directly. Use {@link OBinaryLazyList#createEntry(T)}
         */
        protected LazyEntry(final T value) {
            this(DIRTY, DIRTY, value);
        }

        /**
         * Do not create entries directly
         */
        private LazyEntry(final int sourcePosition, final int size, final T value) {
            this.sourcePosition = sourcePosition;
            this.size = size;
            this.value = value;
        }

        public boolean isDirty() {
            return sourcePosition == DIRTY;
        }

        /**
         * Init record if it is still not initialized
         */
        public void init() {
            if (sourcePosition != DIRTY && value == null) {
                OBinaryLazyList.this.status = STATUS.UNMARSHALLING;
                final ObjectSerializer<T> objectSerializer = ObjectSerializerFactory.INSTANCE.getObjectSerializer(source[sourcePosition]);
                this.value = objectSerializer.deserialize(source, sourcePosition + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE);
            }
            OBinaryLazyList.this.status = STATUS.LOADED;
        }

        public void set(T value) {
            this.sourcePosition = DIRTY;
            this.value = value;
        }

        public T get() {
            init();
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

    private STATUS status;
    /**
     * List owner
     */
    private ORecord<?> sourceRecord;
    private int sourceOffset;
    private byte[] source;
    private boolean hasChanges;
    protected final ArrayList<LazyEntry<T>> data = new ArrayList<LazyEntry<T>>();

    /**
     * Default constructor
     */
    public OBinaryLazyList() {
        this.status = STATUS.NOT_LOADED;
        this.sourceRecord = null;
        this.source = null;
        this.sourceOffset = -1;
        this.hasChanges = true;
    }

    /**
     * Setter for the list owner. Assumed that this will be called on deserialization hook
     *
     * @param sourceRecord list owner
     */
    public void setSourceRecord(ORecord<?> sourceRecord) {
        this.sourceRecord = sourceRecord;
    }

    /**
     * Serialize list into the stream
     *
     * @param stream       to serialize list
     * @param streamOffset position to start serialization from
     */
    public void serialize(final byte[] stream, final int streamOffset) {
        this.status = STATUS.MARSHALLING;
        if (hasChanges) {
            final ObjectSerializer<Integer> indexSerializer = ObjectSerializerFactory.INSTANCE.getIndexSerializer();
            final int indexSize = ObjectSerializerFactory.INSTANCE.getIndexSize();
            final int size = data.size();
            indexSerializer.serialize(size, stream, streamOffset);
            int indexOffset = streamOffset + indexSize;
            int dataOffset = indexOffset + size * indexSize;
            for (int i = 0; i < size; i++) {
                final int objSize = getObjectSize(i);
                indexSerializer.serialize(objSize, stream, indexOffset);
                indexOffset += indexSize;
                if (!data.get(i).isDirty()) {
                    System.arraycopy(source, data.get(i).sourcePosition, stream, dataOffset, data.get(i).size);
                } else {
                    final T obj = data.get(i).get();
                    final OType type = OType.getTypeByClass(obj.getClass());
                    stream[dataOffset] = type.getByteId();
                    ObjectSerializerFactory.INSTANCE.getObjectSerializer(type, obj).serialize(obj, stream, dataOffset + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE);
                }
                dataOffset += objSize;
            }
        } else {
            //if list has no changes just copy it from the source
            final int sizeToCopy = getBinarySize();
            System.arraycopy(source, sourceOffset, stream, streamOffset, sizeToCopy);
        }
        this.status = STATUS.LOADED;
        this.hasChanges = false;
    }

    /**
     * Deserialize from the stream
     */
    public void deserialize(byte[] stream, int startPosition) {
        this.status = STATUS.UNMARSHALLING;
        this.sourceOffset = startPosition;
        this.source = stream;
        this.data.clear();
        if (source != null) {
            final ObjectSerializer<Integer> indexSerializer = ObjectSerializerFactory.INSTANCE.getIndexSerializer();
            final int indexSize = ObjectSerializerFactory.INSTANCE.getIndexSize();
            final int initialSize = indexSerializer.deserialize(source, sourceOffset);
            int indexOffset = sourceOffset + indexSize;
            int dataOffset = indexOffset + indexSize * initialSize;
            for (int i = 0; i < initialSize; i++) {
                final int size = indexSerializer.deserialize(source, indexOffset);
                data.add(createEntry(dataOffset, size));
                indexOffset += indexSize;
                dataOffset += size;
            }
        }
        this.status = STATUS.LOADED;
        this.hasChanges = false;
    }

    /**
     * Calculates amount of bytes that is required to serialize this list
     *
     * @return required amount of bytes to serialize list
     */
    public int getBinarySize() {
        final int indexSize = ObjectSerializerFactory.INSTANCE.getIndexSize();
        int size = indexSize * (data.size() + 1);//+1 is stands for the list size
        for (int i = 0; i < data.size(); i++) {
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
        if (!entry.isDirty()) {
            return entry.size;
        } else {
            final T obj = entry.get();
            final OType type = OType.getTypeByClass(obj.getClass());
            return ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE +
                    ObjectSerializerFactory.INSTANCE.getObjectSerializer(type, obj).getObjectSize(obj);
        }
    }

    @Override
    public boolean add(T t) {
        setDirty();
        return data.add(createEntry(t));
    }

    @Override
    public T set(int index, T element) {
        setDirty();
        final T toReturn = get(index);
        data.set(index, createEntry(element));
        return toReturn;
    }

    @Override
    public void add(int index, T element) {
        setDirty();
        data.add(index, createEntry(element));
    }

    @Override
    public T remove(int index) {
        setDirty();
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
                throw new UnsupportedOperationException();
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
        setDirty();
        return data.get(index).getForUpdate();
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public STATUS getInternalStatus() {
        return status;
    }

    @Override
    public void setInternalStatus(STATUS iStatus) {
        this.status = iStatus;
    }

    @Override
    public <RET> RET setDirty() {
        if (status != STATUS.UNMARSHALLING && sourceRecord != null && !sourceRecord.isDirty()) {
            sourceRecord.setDirty();
        }
        hasChanges = true;
        return (RET) this;
    }

    /**
     * Determines weather source record modified or not
     *
     * @return {@code true} if source record has been modified or {@code false} otherwise
     */
    public boolean isDirty() {
        return sourceRecord == null || sourceRecord.isDirty();
    }

    /**
     * Determine weather list modified or not
     *
     * @return {@code true} if list has been modified or {@code false} otherwise
     */
    public boolean hasChanges() {
        return hasChanges;
    }

    @Override
    public void onBeforeIdentityChanged(ORID iRID) {
        //do nothing
    }

    @Override
    public void onAfterIdentityChanged(ORecord<?> iRecord) {
        //do nothing
    }

    protected LazyEntry<T> createEntry(int sourcePosition, int size) {
        return new LazyEntry<T>(sourcePosition, size);
    }

    protected LazyEntry<T> createEntry(T value) {
        return new LazyEntry<T>(value);
    }
}
