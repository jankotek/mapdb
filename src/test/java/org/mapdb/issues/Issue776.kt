package org.mapdb.issues

import org.junit.Test
import org.mapdb.DBMaker
import org.mapdb.TT

class Issue776{

    @Test fun stack_overflow_in_shutdown_hook(){

        val f = TT.tempFile()
        val db = DBMaker.fileDB(f)
                .fileMmapEnable()
                .fileMmapPreclearDisable()
                .cleanerHackEnable()
                .concurrencyDisable()
                .closeOnJvmShutdown()
                .make()
        db.close()

    }

}