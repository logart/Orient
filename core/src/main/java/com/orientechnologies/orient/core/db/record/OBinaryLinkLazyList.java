package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Iterator;

/**
 * Represent list of links that load its elements lazily
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinaryLinkLazyList extends OBinaryLazyList<OIdentifiable> implements ORecordLazyMultiValue {

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
            if (autoConvertToRecord && isLink) {
                convertLink2Record();
            }
            return value;
        }

        public boolean convertRecord2Link() {
            init();
            if (!isLink) {
                if (((ORecord<?>) value).isDirty()
                        || isDirty()) {//TODO how to store if new or changed record (link/document)
                    return false;
                }
                setInternalStatus(STATUS.UNMARSHALLING);
                value = value.getIdentity();
                setInternalStatus(STATUS.LOADED);
                isLink = true;
            }
            return true;
        }

        public void convertLink2Record() {
            init();
            if (isLink) {
                setInternalStatus(STATUS.UNMARSHALLING);
                value = value.getRecord();
                setInternalStatus(STATUS.LOADED);
                isLink = false;
            }
        }
    }

    private boolean autoConvertToRecord = true;
    private boolean ridOnly = false;
    private ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE contentType = ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE.EMPTY;

    public OIdentifiable rawGet(int index) {
        return ((LazyLinkEntry) data.get(index)).rawGet();
    }

    private boolean addAsIdentity(OIdentifiable o) {
        return (ridOnly || contentType == ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE.ALL_RIDS
                || OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.getValueAsBoolean())

                && !o.getIdentity().isNew() && (o instanceof ODocument && !((ODocument) o).isDirty());
    }

    private OIdentifiable convertBeforeStore(OIdentifiable o) {
        if (addAsIdentity(o)) {
            return o.getIdentity();
        } else {
            contentType = ORecordMultiValueHelper.updateContentType(contentType, o);
            return o;
        }
    }

    @Override
    public OIdentifiable set(int index, OIdentifiable element) {
        return super.set(index, convertBeforeStore(element));
    }

    @Override
    public boolean add(OIdentifiable oIdentifiable) {
        return super.add(convertBeforeStore(oIdentifiable));
    }

    @Override
    public void add(int index, OIdentifiable element) {
        super.add(index, convertBeforeStore(element));
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
        if (contentType == ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE.ALL_RECORDS || !autoConvertToRecord) {
            return;
        }
        final int size = size();
        for (int i = 0; i < size; i++) {
            ((LazyLinkEntry) data.get(i)).convertLink2Record();
        }
        contentType = ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE.ALL_RECORDS;
    }

    @Override
    public boolean convertRecords2Links() {
        if (contentType == ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE.ALL_RIDS) {
            return true;
        }
        boolean allConverted = true;
        final int size = size();
        for (int i = 0; i < size; i++) {
            allConverted &= ((LazyLinkEntry) data.get(i)).convertRecord2Link();
        }
        if (allConverted) {
            contentType = ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE.ALL_RIDS;
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

    /**
     * @return {@code true} if list have to contain only ORID, {@code false} otherwise
     */
    public boolean isRidOnly() {
        return ridOnly;
    }

    /**
     * Permit/deny for list to contain records
     *
     * @param ridOnly {@code true} if list have to contain only ORID, {@code false} otherwise
     * @return this list
     */
    public OBinaryLinkLazyList setRidOnly(boolean ridOnly) {
        this.ridOnly = ridOnly;
        return this;
    }
}
