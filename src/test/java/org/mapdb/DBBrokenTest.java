package org.mapdb;

import org.junit.*;
import org.mapdb.volume.RandomAccessFileVol;
import org.mapdb.volume.Volume;

import java.io.*;
import java.util.Arrays;


public class DBBrokenTest {
    File index;
    File log;

    @Before
    public void before() throws IOException {
        index = TT.tempFile();
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
    @Ignore //TODO index checksum
    public void canDeleteDBOnBrokenIndex() throws IOException {
        for (final File f : Arrays.asList(index, log)) {
            final FileOutputStream fos = new FileOutputStream(f);
            fos.write("Some Junk".getBytes());
            fos.close();
        }

        try {
            DBMaker.fileDB(index.getPath()).make();
            Assert.fail("Expected exception not thrown");
        } catch (final DBException.WrongConfiguration e) {
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
    @Ignore //TODO index checksum
    public void canDeleteDBOnBrokenLog() throws IOException {
        // init empty, but valid DB
        DBMaker.fileDB(index.getPath()).make().close();

        // corrupt file
        Volume physVol = new RandomAccessFileVol(index, false, false, 0L);
        physVol.ensureAvailable(32);
        physVol.putLong(16, 123456789L);
        physVol.sync();
        physVol.close();

        try {
            DBMaker.fileDB(index.getPath()).make();
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