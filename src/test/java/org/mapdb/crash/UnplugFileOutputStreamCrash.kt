package org.mapdb.crash

import org.mapdb.DataIO
import org.mapdb.TT
import java.io.DataInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream

/**
 * Tests crash resistance by manually unplugging the drive.
 *
 */

fun waitUntilAvailable(file:File){
    if(file.exists().not())
        println("File not available, waiting until it exist again.")
    while(file.exists().not())
        Thread.sleep(100)
}

fun main(args : Array<String>) {
    // local directory, this is permanent storage
    val d = TT.tempDir()
    // file on storage which can be unpluged
    val file = File(args[0])
    waitUntilAvailable(file.parentFile)
    file.delete()
    file.createNewFile()
    var out = FileOutputStream(file)
    val b = ByteArray(8)

    var a = 0L
    while(true){
        a++
        if(file.exists().not()){
           break
        }
        try {
            File(d, "$a").createNewFile()
            DataIO.putLong(b, 0, 8)
            out.write(b)
            out.flush()
        }catch(e:Exception){

        }
    }

    println("Storage gone, progress is $a");
    waitUntilAvailable(file)

    //replay file
    val ins = DataInputStream(FileInputStream(file))
    try{
        while(true){
            ins.readLong()
        }
    }catch(e:Exception){
    }
    println("Replayed $a")
    file.delete()


}
