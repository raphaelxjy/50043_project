package simpledb;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.storage.DbFile;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class CatalogTest extends SimpleDbTestBase {
	private static final Random r = new Random();
    private static final String name = SystemTestUtil.getUUID();
    private static final int id1 = r.nextInt();
    private static final int id2 = r.nextInt();
	private String nameThisTestRun;
    
    @Before public void addTables() {
        Database.getCatalog().clear();
		nameThisTestRun = SystemTestUtil.getUUID();
        Database.getCatalog().addTable(new SkeletonFile(id1, Utility.getTupleDesc(2)), nameThisTestRun);
        Database.getCatalog().addTable(new SkeletonFile(id2, Utility.getTupleDesc(2)), name);
    }

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test public void getTupleDesc() {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(id1);

        assertEquals(expected, actual);
    }

    /**
     * Unit test for Catalog.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(id2, Database.getCatalog().getTableId(name));
        assertEquals(id1, Database.getCatalog().getTableId(nameThisTestRun));
        
        try {
            Database.getCatalog().getTableId(null);
            Assert.fail("Should not find table with null name");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
        
        try {
            Database.getCatalog().getTableId("foo");
            Assert.fail("Should not find table with name foo");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    /**
     * Unit test for Catalog.getDatabaseFile()
     */

    @Test public void getDatabaseFile() {
        DbFile f = Database.getCatalog().getDatabaseFile(id1);

        // NOTE(ghuo): we try not to dig too deeply into the DbFile API here; we
        // rely on HeapFileTest for that. perform some basic checks.
        assertEquals(id1, f.getId());
    }
    
    /**
     * Check that duplicate names are handled correctly
     */
    @Test public void handleDuplicateNames() {
    	int id3 = r.nextInt();
    	Database.getCatalog().addTable(new SkeletonFile(id3, Utility.getTupleDesc(2)), name);
    	assertEquals(id3, Database.getCatalog().getTableId(name));
    }
    
    /**
     * Check that duplicate file ids are handled correctly
     */
    @Test public void handleDuplicateIds() {
    	String newName = SystemTestUtil.getUUID();
    	DbFile f = new SkeletonFile(id2, Utility.getTupleDesc(2));
    	Database.getCatalog().addTable(f, newName);
    	assertEquals(newName, Database.getCatalog().getTableName(id2));
    	assertEquals(f, Database.getCatalog().getDatabaseFile(id2));
    }


    /**
     * Unit test for Catalog.getPrimaryKey().
     * Ensures primary key field is stored and returned correctly.
     */
    @Test public void getPrimaryKey() {
        String pk = "id";
        DbFile f = new SkeletonFile(r.nextInt(), Utility.getTupleDesc(1));
        Database.getCatalog().addTable(f, SystemTestUtil.getUUID(), pk);

        assertEquals(pk, Database.getCatalog().getPrimaryKey(f.getId()));
    }

    /**
     * Ensures Catalog.getPrimaryKey() throws for unknown table ids.
     */
    @Test public void getPrimaryKeyUnknownId() {
        try {
            Database.getCatalog().getPrimaryKey(Integer.MIN_VALUE);
            Assert.fail("Expected NoSuchElementException for unknown table id");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    /**
     * Ensures Catalog.getTupleDesc() and Catalog.getDatabaseFile() throw for unknown table ids.
     */
    @Test public void unknownTableIdThrows() {
        int missingId = Integer.MAX_VALUE;

        try {
            Database.getCatalog().getTupleDesc(missingId);
            Assert.fail("Expected NoSuchElementException for unknown table id in getTupleDesc");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            Database.getCatalog().getDatabaseFile(missingId);
            Assert.fail("Expected NoSuchElementException for unknown table id in getDatabaseFile");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    /**
     * Verifies addTable allows an empty table name and that it can be retrieved by name and id.
     */
    @Test public void addTableWithEmptyName() {
        int id = r.nextInt();
        String emptyName = "";
        DbFile f = new SkeletonFile(id, Utility.getTupleDesc(2));

        Database.getCatalog().addTable(f, emptyName);

        assertEquals(id, Database.getCatalog().getTableId(emptyName));
        assertEquals(emptyName, Database.getCatalog().getTableName(id));
        assertEquals(f, Database.getCatalog().getDatabaseFile(id));
    }

    /**
     * Verifies the overload Catalog.addTable(DbFile) assigns a non-null name that can be
     * retrieved via getTableName and used to look up the table id.
     */
    @Test public void addTableAutoNameIsQueryable() {
        int id = r.nextInt();
        DbFile f = new SkeletonFile(id, Utility.getTupleDesc(1));

        Database.getCatalog().addTable(f);

        String autoName = Database.getCatalog().getTableName(id);
        assertNotNull("Auto-generated table name should not be null", autoName);
        assertFalse("Auto-generated table name should not be empty", autoName.isEmpty());

        assertEquals(id, Database.getCatalog().getTableId(autoName));
    }

    /**
     * Unit test for Catalog.tableIdIterator().
     * Ensures iterator returns a snapshot of ids present when created.
     */
    @Test public void tableIdIteratorSnapshot() {

        // Snapshot at iterator creation time should include current ids.
        Iterator<Integer> it = Database.getCatalog().tableIdIterator();
        Set<Integer> ids = new HashSet<>();
        while (it.hasNext()) {
            ids.add(it.next());
        }

        assertTrue(ids.contains(id1));
        assertTrue(ids.contains(id2));

        // Mutate catalog after iterator creation; collected ids should remain unchanged.
        int id3 = r.nextInt();
        Database.getCatalog().addTable(new SkeletonFile(id3, Utility.getTupleDesc(1)), SystemTestUtil.getUUID());

        assertFalse("Iterator snapshot should not include tables added after iterator creation", ids.contains(id3));
    }

    /**
     * Ensures tableIdIterator() returns an empty iterator after clear().
     */
    @Test public void tableIdIteratorAfterClear() {
        Database.getCatalog().clear();

        Iterator<Integer> it = Database.getCatalog().tableIdIterator();
        assertFalse("Expected no table ids after clear()", it.hasNext());
    }

    /**
     * Unit test for Catalog.clear().
     * Ensures all lookup paths are reset.
     */
    @Test public void clearResetsLookups() {
        Database.getCatalog().clear();;
        

        try {
            Database.getCatalog().getTableId(name);
            Assert.fail("Expected NoSuchElementException after clear()");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            Database.getCatalog().getTupleDesc(id1);
            Assert.fail("Expected NoSuchElementException after clear()");
        } catch (NoSuchElementException e) {
            // expected
        }

        try {
            Database.getCatalog().getDatabaseFile(id2);
            Assert.fail("Expected NoSuchElementException after clear()");
        } catch (NoSuchElementException e) {
            // expected
        }

        assertNull("Expected null table name lookup after clear()", Database.getCatalog().getTableName(id1));
    }


    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(CatalogTest.class);
    }
}

