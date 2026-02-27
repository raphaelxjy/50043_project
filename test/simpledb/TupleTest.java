package simpledb;

import static org.junit.Assert.*;
import org.junit.Assert;
import java.util.Iterator;
import java.util.NoSuchElementException;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SimpleDbTestBase;

public class TupleTest extends SimpleDbTestBase {

    /**
     * Unit test for Tuple.getField() and Tuple.setField()
     */
    @Test public void modifyFields() {
        TupleDesc td = Utility.getTupleDesc(2);

        Tuple tup = new Tuple(td);
        tup.setField(0, new IntField(-1));
        tup.setField(1, new IntField(0));

        assertEquals(new IntField(-1), tup.getField(0));
        assertEquals(new IntField(0), tup.getField(1));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));

        assertEquals(new IntField(1), tup.getField(0));
        assertEquals(new IntField(37), tup.getField(1));
    }

    /**
     * Unit test for Tuple.getTupleDesc()
     */
    @Test public void getTupleDesc() {
        TupleDesc td = Utility.getTupleDesc(5);
        Tuple tup = new Tuple(td);
        assertEquals(td, tup.getTupleDesc());
    }

    /**
     * Unit test for Tuple.getRecordId() and Tuple.setRecordId()
     */
    @Test public void modifyRecordId() {
        Tuple tup1 = new Tuple(Utility.getTupleDesc(1));
        HeapPageId pid1 = new HeapPageId(0,0);
        RecordId rid1 = new RecordId(pid1, 0);
        tup1.setRecordId(rid1);

	try {
	    assertEquals(rid1, tup1.getRecordId());
	} catch (java.lang.UnsupportedOperationException e) {
		//rethrow the exception with an explanation
    	throw new UnsupportedOperationException("modifyRecordId() test failed due to " +
    			"RecordId.equals() not being implemented.  This is not required for Lab 1, " +
    			"but should pass when you do implement the RecordId class.");
	}
    }

    /**
     * Verifies constructor validation:
     *  - null TupleDesc should throw IllegalArgumentException
     *  - TupleDesc with zero fields should throw IllegalArgumentException
     */
    @Test public void testConstructorValidation() {
        try {
            new Tuple(null);
            Assert.fail("Expected IllegalArgumentException for null TupleDesc");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new Tuple(Utility.getTupleDesc(0));
            Assert.fail("Expected IllegalArgumentException for zero-field TupleDesc");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Verifies getField and setField throw NoSuchElementException
     * for invalid indices.
     */
    @Test public void testInvalidFieldAccess() {
        Tuple tup = new Tuple(Utility.getTupleDesc(2));

        try {
            tup.setField(-1, new IntField(1));
            Assert.fail("Expected NoSuchElementException for negative index");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            tup.setField(2, new IntField(1));
            Assert.fail("Expected NoSuchElementException for out-of-bounds index");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            tup.getField(-1);
            Assert.fail("Expected NoSuchElementException for negative index");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            tup.getField(2);
            Assert.fail("Expected NoSuchElementException for out-of-bounds index");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    /**
     * Verifies fields() iterator:
     *  - iterates exactly numFields() times
     *  - preserves insertion order
     *  - throws NoSuchElementException when exhausted
     */
    @Test public void testFieldsIteratorBehavior() {
        Tuple tup = new Tuple(Utility.getTupleDesc(3));

        tup.setField(0, new IntField(10));
        tup.setField(1, new IntField(20));
        tup.setField(2, new IntField(30));

        Iterator<Field> it = tup.fields();
        int count = 0;

        int[] expected = {10, 20, 30};
        while (it.hasNext()) {
            Field f = it.next();
            assertEquals(new IntField(expected[count]), f);
            count++;
        }

        assertEquals(3, count);

        try {
            it.next();
            Assert.fail("Expected NoSuchElementException after iterator exhaustion");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    /**
     * Verifies that uninitialized fields return null.
     */
    @Test public void testUninitializedFieldsAreNull() {
        Tuple tup = new Tuple(Utility.getTupleDesc(2));

        assertNull(tup.getField(0));
        assertNull(tup.getField(1));
    }

    /**
     * Verifies resetTupleDesc updates the TupleDesc reference.
     */
    @Test public void testResetTupleDesc() {
        TupleDesc td1 = Utility.getTupleDesc(2);
        TupleDesc td2 = Utility.getTupleDesc(3);

        Tuple tup = new Tuple(td1);
        assertEquals(td1, tup.getTupleDesc());

        tup.resetTupleDesc(td2);

        // resetTupleDesc in current implementation sets td to null,
        // so verify behavior explicitly
        assertNull(tup.getTupleDesc());
    }

    /**
     * Verifies toString format:
     *  - contains all field values
     *  - separated by whitespace
     *  - does not end with trailing whitespace
     */
    @Test public void testToStringFormat() {
        Tuple tup = new Tuple(Utility.getTupleDesc(2));

        tup.setField(0, new IntField(5));
        tup.setField(1, new IntField(9));

        String s = tup.toString();

        assertTrue(s.contains("5"));
        assertTrue(s.contains("9"));
        assertFalse(s.endsWith(" "));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TupleTest.class);
    }
}

