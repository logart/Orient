package com.orientechnologies.orient.core.db.record;

import java.util.Iterator;

/**
 * Represent list of links that load its elements lazily
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinaryLinkLazyList extends OBinaryLazyList<OIdentifiable> implements ORecordLazyMultiValue {

    protected boolean autoConvertToRecord = true;

    protected class LazyLinkEntry extends LazyEntry<OIdentifiable> {

        protected boolean isLink = true;

        /**
         * Do not create entries directly. Use {@link OBinaryLinkLazyList#createEntry(int, int)}
         */
        protected LazyLinkEntry(int sourcePosition, int size) {
            super(sourcePosition, size);
        }

        /**
         * Do not create entries directly. Use {@link OBinaryLinkLazyList#createEntry(OIdentifiable)}
         */
        protected LazyLinkEntry(OIdentifiable value) {
            super(value);
        }

        public OIdentifiable rawGet() {
            return super.get();
        }
        
        @Override
        public OIdentifiable get() {
            init();
            if(autoConvertToRecord && isLink){
                convertLink2Record();
            }
            return value;
        }

        public boolean convertRecord2Link() {
            init();
            if(!isLink){
                value = value.getIdentity();//this will lead to loose changes if not saved
                isLink = true;
            }
            return true;
        }

        public void convertLink2Record() {
            init();
            if(isLink){
                status = STATUS.UNMARSHALLING;
                value = value.getRecord();
                status = STATUS.LOADED;
                isLink = false;
            }
        }
    }


    public OIdentifiable rawGet(int index) {
        return ((LazyLinkEntry) data.get(index)).rawGet();
    }

    @Override
    public Iterator<OIdentifiable> rawIterator() {
        return new Iterator<OIdentifiable>() {

            private final Iterator<LazyEntry<OIdentifiable>> iterator = data.iterator();

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public OIdentifiable next() {
                return ((LazyLinkEntry) iterator.next()).rawGet();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void convertLinks2Records() {
        if(!autoConvertToRecord){
            return;
        }
        final int size = size();
        for(int i=0; i<size; i++){
            ((LazyLinkEntry) data.get(i)).convertLink2Record();
        }
    }

    @Override
    public boolean convertRecords2Links() {
        boolean allConverted = true;
        final int size = size();
        for(int i=0; i<size; i++){
            allConverted &= ((LazyLinkEntry) data.get(i)).convertRecord2Link();
        }
        return allConverted;
    }

    @Override
    public boolean isAutoConvertToRecord() {
        return autoConvertToRecord;
    }

    @Override
    public void setAutoConvertToRecord(boolean convertToRecord) {
        this.autoConvertToRecord = convertToRecord;
    }

    @Override
    public boolean detach() {
        return convertRecords2Links();
    }

    @Override
    protected LazyEntry<OIdentifiable> createEntry(int sourcePosition, int size) {
        return new LazyLinkEntry(sourcePosition, size);
    }

    @Override
    protected LazyEntry<OIdentifiable> createEntry(OIdentifiable value) {
        return new LazyLinkEntry(value);
    }
}
