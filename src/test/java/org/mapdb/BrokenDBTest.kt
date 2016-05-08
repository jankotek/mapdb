package org.mapdb

import org.junit.*
import org.mapdb.volume.RandomAccessFileVol
import org.mapdb.volume.Volume

import java.io.*
import java.util.Arrays

class BrokenDBTest {
    internal var index: File? = null
    internal var log: File? = null

    @Before
    @Throws(IOException::class)
    fun before() {
        index = TT.tempFile()
        log = File(index!!.path + "wal.0")
    }

    /*
     * Verify that DB files are properly closed when opening the database fails, allowing an
     * application to recover by purging the database and starting over.
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Test
    @Throws(FileNotFoundException::class, IOException::class)
    fun canDeleteDBOnBrokenIndex() {
        for (f in Arrays.asList<File>(index, log)) {
            val fos = FileOutputStream(f)
            fos.write("Some Junk".toByteArray())
            fos.close()
        }

        try {
            DBMaker.fileDB(index!!).make()
            Assert.fail("Expected exception not thrown")
        } catch (e: DBException.WrongFormat) {
            // will fail!
            Assert.assertTrue("Wrong message", e.message!!.contains("Wrong file header, not MapDB file"))
        }

        index!!.delete()
        log!!.delete()

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index!!.exists())
        Assert.assertFalse("Can't delete log", log!!.exists())
    }

    /*
     * Verify that DB files are properly closed when opening the database fails, allowing an
     * application to recover by purging the database and starting over.
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Ignore //TODO header checksums @Test
    @Throws(IOException::class)
    fun canDeleteDBOnBrokenLog() {
        // init empty, but valid DB
        DBMaker.fileDB(index!!).make().close()

        // corrupt file
        val physVol = RandomAccessFileVol(index, false, 0L, 0L)
        physVol.ensureAvailable(32)
        physVol.putLong(16, 123456789L)
        physVol.sync()
        physVol.close()

        try {
            DBMaker.fileDB(index!!).make()
            Assert.fail("Expected exception not thrown")
        } catch (e: DBException.WrongFormat) {
            // expected
        }

        index!!.delete()
        log!!.delete()

        // assert that we can delete the db files
        Assert.assertFalse("Can't delete index", index!!.exists())
        Assert.assertFalse("Can't delete log", log!!.exists())
    }

    @After
    @Throws(IOException::class)
    fun after() {
        if (index != null)
            index!!.deleteOnExit()
        if (log != null)
            log!!.deleteOnExit()
    }


}