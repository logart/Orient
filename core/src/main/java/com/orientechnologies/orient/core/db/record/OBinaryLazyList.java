package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinaryLazyList<T> extends AbstractList<T> {

    protected class LazyEntry<T> {

        private static final int DIRTY = -1;
        private int sourcePosition;
        private T value;

        public LazyEntry(int sourcePosition) {
            this(sourcePosition, null);
        }

        public LazyEntry(T value) {
            this(DIRTY, value);
        }

        public LazyEntry(int sourcePosition, T value) {
            this.sourcePosition = sourcePosition;
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
            if(sourcePosition != DIRTY && value == null){
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

            if(thisValue == null){
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
    private final byte[] source;
    private final ArrayList<LazyEntry<T>> data = new ArrayList<LazyEntry<T>>();

    public OBinaryLazyList() {
        this(null);
    }

    public OBinaryLazyList(byte[] source) {
        this.source = source;
        if(source != null){
            final ObjectSerializer<Integer> indexSerializer = ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.INTEGER);
            final int initialSize = indexSerializer.deserialize(source, 0);
            int position = indexSerializer.getFieldSize(null);
            for (int i=0; i<initialSize; i++){
                data.add(new LazyEntry<T>(indexSerializer.deserialize(source, position)));
            }
        }
    }

    public void toStream(final byte[] stream, final int offset) {
        if(isDirty){
            final ObjectSerializer<Integer> indexSerializer = ObjectSerializerFactory.INSTANCE.getIndexSerializer();
            final int indexSize = indexSerializer.getFieldSize(null);
            final int size = data.size();
            final int[] offsets = new int[size];
            int position = 0;
            for(int i=0; i<size; i++) {
                offsets[i] = position;
                position += ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE +
                        ObjectSerializerFactory.INSTANCE.getObjectSerializer((byte)0).getFieldSize(data.get(i).get());
                        //TODO obtain serializer by type, and not trigger lazy initialization;
            }

            final int offsetsSize = indexSize * (data.size() + 1);
            indexSerializer.serialize(size, stream, 0);
            position = indexSize;
            for(int i=0; i<size; i++) {
                indexSerializer.serialize(offsets[i], stream, position);
                position += indexSize;
                if(data.get(i).isDirty()){
                    ObjectSerializerFactory.INSTANCE.getObjectSerializer((byte)0).serialize(data.get(i).get(), stream, offsetsSize+offsets[i]);
                }else{
    //                System.arraycopy();TODO copy from the source to new stream
                }
            }
        } else {
//            System.arraycopy();TODO copy all
        }
    }

    public int getStreamSize() {
        final int indexSize = ObjectSerializerFactory.INSTANCE.getIndexSerializer().getFieldSize(null);
        int size = indexSize * (data.size() + 1);
        for(final LazyEntry<T> entry : data) {
            size += ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE +
                    ObjectSerializerFactory.INSTANCE.getObjectSerializer((byte)0).getFieldSize(entry.get());
                    //TODO obtain serializer by type, and not trigger lazy initialization
        }
        return 0;
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
                iterator.remove();
            }
        };
    }

    @Override
    public T get(int index) {
        return data.get(index).get();
    }

    public T getForUpdate(int index) {
        isDirty = true;
        return data.get(index).getForUpdate();
    }

    @Override
    public int size() {
        return data.size();
    }
}
