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
    File data;
    File log;

    @Before
    public void before() throws IOException {
        index = UtilsTest.tempDbFile();
        data = new File(index.getPath() + StoreDirect.DATA_FILE_EXT);
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
        for (final File f : Arrays.asList(index, data, log)) {
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
        data.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete data", data.exists());
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

        // trash the log
        MappedFileVol logVol = new Volume.MappedFileVol(log, false, 0,CC.VOLUME_CHUNK_SHIFT,0);
        logVol.ensureAvailable(32);
        logVol.putInt(0, StoreWAL.HEADER);
        logVol.putUnsignedShort(4, StoreWAL.STORE_VERSION);
        logVol.putLong(8, StoreWAL.LOG_SEAL);
        logVol.putLong(16, 123456789L);
        logVol.sync();
        logVol.close();

        try {
            DBMaker.newFileDB(index).make();
            Assert.fail("Expected exception not thrown");
        } catch (final Error e) {
            // will fail!
            Assert.assertTrue("Wrong message", e.getMessage().contains("unknown trans log instruction"));
        }

        index.delete();
        data.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete data", data.exists());
        Assert.assertFalse("Can't delete log", log.exists());
    }

    @After
    public void after() throws IOException {
        if (index != null)
            index.deleteOnExit();
        if (data != null)
            data.deleteOnExit();
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
        RandomAccessFile dataFile = new RandomAccessFile(data, "rw");
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
        data.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete data", data.exists());
        Assert.assertFalse("Can't delete log", log.exists());
    }
}