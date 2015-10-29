package org.mapdb;

import org.junit.*;
import org.mapdb.Volume.MappedFileVol;

import java.io.*;
import java.util.Arrays;

public class BrokenDBTest {
    File index;
    File log;

    @Before
    public void before() throws IOException {
        index = TT.tempDbFile();
        log = new File(index.getPath() + "wal.0");
    }

    /*
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
            DBMaker.fileDB(index).make();
            Assert.fail("Expected exception not thrown");
        } catch (final DBException.WrongConfig e) {
            // will fail!
            Assert.assertTrue("Wrong message", e.getMessage().contains("This is not MapDB file"));
        }

        index.delete();
        log.delete();

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index.exists());
        Assert.assertFalse("Can't delete log", log.exists());
    }

    /*
     * Verify that DB files are properly closed when opening the database fails, allowing an
     * application to recover by purging the database and starting over.
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    public void canDeleteDBOnBrokenLog() throws IOException {
        // init empty, but valid DB
        DBMaker.fileDB(index).make().close();

        // corrupt file
        Volume physVol = new Volume.RandomAccessFileVol(index, false, false, 0L);
        physVol.ensureAvailable(32);
        //TODO corrupt file somehow
//        physVol.putInt(0, StoreDirect.HEADER);
//        physVol.putUnsignedShort(4, StoreDirect.STORE_VERSION);
//        physVol.putLong(8, StoreWAL.LOG_SEAL);
        physVol.putLong(16, 123456789L);
        physVol.sync();
        physVol.close();

        try {
            DBMaker.fileDB(index).make();
            Assert.fail("Expected exception not thrown");
        } catch (final DBException.HeadChecksumBroken e) {
            // expected
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



}