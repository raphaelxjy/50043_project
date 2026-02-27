package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    public TDItem[] TDItemAr;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {

        return new Iterator<TDItem>() {

            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < numFields();
            }

            @Override
            public TDItem next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } 
                return TDItemAr[i++];
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException {
        if (typeAr == null || typeAr.length == 0) {
                throw new IllegalArgumentException("typeAr must contain at least one entry");
        } else if (fieldAr == null) {
            throw new IllegalArgumentException("fieldAr must be an array");
        } else if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("typeAr and fieldAr must have the same length");
        } else {
            int l = typeAr.length;
            TDItemAr = new TDItem[l];
            for (int i = 0; i < l; i++) {
                TDItemAr[i] = new TDItem(typeAr[i], fieldAr[i]);
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr == null || typeAr.length == 0) {
                throw new IllegalArgumentException("typeAr must contain at least one entry");
        } else {
            int l = typeAr.length;
            TDItemAr = new TDItem[l];
            for (int i = 0; i < l; i++) {
                TDItemAr[i] = new TDItem(typeAr[i], null);
            }
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.TDItemAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        try {
            return this.TDItemAr[i].fieldName;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        try {
            return this.TDItemAr[i].fieldType;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name != null) {
            final int length = numFields();
            for (int i = 0; i < length; i++) {
                String fname = getFieldName(i);
                if (fname != null && getFieldName(i).equals(name)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException("No field with a matching name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int c = numFields();
        int size = 0;
        for (int i = 0; i < c; i++) {
            size += this.TDItemAr[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int td1Length = td1.numFields();
        int td2Length = td2.numFields();
        Type[] typeArNew = new Type[td1Length + td2Length];
        String[] fieldArNew = new String[td1Length + td2Length];

        for (int i = 0; i < td1Length; i++) {
            typeArNew[i] = td1.getFieldType(i);
            fieldArNew[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < td2Length; i++) {
            typeArNew[i + td1Length] = td2.getFieldType(i);
            fieldArNew[i + td1Length] = td2.getFieldName(i);
        }

        return new TupleDesc(typeArNew, fieldArNew);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc ob = (TupleDesc) o;
        int numFields = numFields();
        if (numFields == ob.numFields()) {
            for (int i = 0; i < numFields; i++) {
                if (getFieldType(i) != ob.getFieldType(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (TDItem t : TDItemAr) {
            sb.append(t.toString());
        }
        return sb.toString();
    }
}
