package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.storage.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId recordId;
    private Field[] fieldAr;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td == null) throw new IllegalArgumentException("td must be a valid TupleDesc instance");
        int n = td.numFields();
        if (n == 0) throw new IllegalArgumentException("td must have at least one field");
        this.fieldAr = new Field[n];
        this.td = td;
        this.recordId = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        try {
            this.fieldAr[i] = f;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException("i is not a valid index");
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        try {
            return this.fieldAr[i];
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException("i is not a valid index");
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int l = fieldAr.length;
        for (int i = 0; i < l; i++) {
            sb.append(fieldAr[i].toString() + " ");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return new Iterator<Field>() {

            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < fieldAr.length;
            }

            @Override
            public Field next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } 
                return fieldAr[i++];
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = null;
    }
}
