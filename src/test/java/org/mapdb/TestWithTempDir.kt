package org.mapdb

import org.junit.After
import java.io.File

/**
 * Test case with temporary dir,
 * dir is deleted after tests finishes
 */
abstract class TestWithTempDir{

    private var tempDirCreated = false

    fun tempFile() = File(tempDir, Math.random().toString())

    val  tempDir: File by lazy{
        tempDirCreated = true
        TT.tempDir()
    }

    @After fun deleteTempDir(){
        if(tempDirCreated)
            TT.tempDeleteRecur(tempDir)
    }


}