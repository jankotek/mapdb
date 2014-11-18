package org.mapdb;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.Volume.MappedFileVol;

import java.io.*;
import java.util.Arrays;

public class BrokenDBTest {
    File index;
    File log;

    @Before
    public void before() throws IOException {
        index = UtilsTest.tempDbFile();
        log = new File(index.getPath() + StoreWAL.TRANS_LOG_FILE_EXT);
    }

    /**
     * Verify that DB files are properly closed when opening the database fails, allowing an
     * application to recover by purging the database and starting over.
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void canDeleteDBOnBrokenIndex() throws FileNotFoundException, IOException {
        for (final File f : Arrays.asList(index, log)) {
            final FileOutputStream fos = new FileOutputStream(f);
            fos.write("Some Junk".getBytes());
            fos.close();
        }

        try {
            DBMaker.newFileDB(index).make();
            Assert.fail("Expected exception not thrown");
        } catch (final IOError e) {
            // will fail!
            Assert.assertTrue("Wrong message", e.getMessage().contains("storage has invalid header"));
        }

        index.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete log", log.exists());
    }

    /**
     * Verify that DB files are properly closed when opening the database fails, allowing an
     * application to recover by purging the database and starting over.
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void canDeleteDBOnBrokenLog() throws IOException {
        // init empty, but valid DB
        DBMaker.newFileDB(index).make().close();

        // corrupt file
        MappedFileVol physVol = new Volume.MappedFileVol(index, false, CC.VOLUME_PAGE_SHIFT,0);
        physVol.ensureAvailable(32);
        //TODO corrupt file somehow
//        physVol.putInt(0, StoreDirect.HEADER);
//        physVol.putUnsignedShort(4, StoreDirect.STORE_VERSION);
//        physVol.putLong(8, StoreWAL.LOG_SEAL);
        physVol.putLong(16, 123456789L);
        physVol.sync();
        physVol.close();

        try {
            DBMaker.newFileDB(index).make();
            Assert.fail("Expected exception not thrown");
        } catch (final Exception e) {
            // will fail!
            Assert.assertTrue("Wrong message", e.getMessage().contains("Error while opening"));
        }

        index.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete log", log.exists());
    }

    @After
    public void after() throws IOException {
        if (index != null)
            index.deleteOnExit();
        if (log != null)
            log.deleteOnExit();
    }



    public static class SomeDataObject implements Serializable {
        private static final long serialVersionUID = 1L;
        public int someField = 42;
    }

    /**
     * Verify that DB files are properly closed when opening the database fails, allowing an
     * application to recover by purging the database and starting over.
     *
     * @throws FileNotFoundException
     * @throws IOException
     *
     *
     */
    @Test
    public void canDeleteDBOnBrokenContent() throws IOException {
        // init empty, but valid DB
        DB db = DBMaker.newFileDB(index).make();
        db.getHashMap("foo").put("foo", new SomeDataObject());
        db.commit();
        db.close();

        // Fudge the content so that the data refers to an undefined field in SomeDataObject.
        RandomAccessFile dataFile = new RandomAccessFile(index, "rw");
        byte grep[] = "someField".getBytes();
        int p = 0, read;
        while ((read = dataFile.read()) >= 0)
            if (((byte) read) == grep[p]) {
                if (++p == grep.length) {
                    dataFile.seek(dataFile.getFilePointer() - grep.length);
                    dataFile.write("xxxxField".getBytes());
                    break;
                }
            } else
                p = 0;
        dataFile.close();

        try {
            DBMaker.newFileDB(index).make();
            Assert.fail("Expected exception not thrown");
        } catch (final RuntimeException e) {
            // will fail!
            Assert.assertTrue("Wrong message", e.getMessage().contains("Could not set field value"));
        }

        index.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete log", log.exists());
    }
}