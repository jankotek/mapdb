package org.mapdb.issues

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.DBMaker
import org.mapdb.Serializer
import org.mapdb.TestWithTempDir

class Issue819: TestWithTempDir() {

    @Test fun issue819(){
        val f = tempFile()
        var db = DBMaker.fileDB(f).make()
        var m = db.hashMap("test", Serializer.INTEGER, Serializer.INTEGER)
                .layout(128, 10, 3).createOrOpen()
        for(i in 0 until 1000)
            m.put(i, i)
        for(i in 0 until 1000)
            assertEquals(i, m.get(i))
        assertEquals(1000, m.size)

        db.close()
        db = DBMaker.fileDB(f).make()
        m = db.hashMap("test", Serializer.INTEGER, Serializer.INTEGER)
                .open()

        for(i in 0 until 1000)
           assertEquals(i, m.get(i))
        assertEquals(1000, m.size)
    }

    @Test fun issue819_counter(){
        val f = tempFile()
        var db = DBMaker.fileDB(f).make()
        var m = db.hashMap("test", Serializer.INTEGER, Serializer.INTEGER)
                .counterEnable()
                .layout(128, 10, 3).createOrOpen()
        for(i in 0 until 1000)
            m.put(i, i)
        for(i in 0 until 1000)
            assertEquals(i, m.get(i))
        assertEquals(1000, m.size)

        db.close()
        db = DBMaker.fileDB(f).make()
        m = db.hashMap("test", Serializer.INTEGER, Serializer.INTEGER)
                .counterEnable()
                .open()

        for(i in 0 until 1000)
            assertEquals(i, m.get(i))
        assertEquals(1000, m.size)
    }

}
