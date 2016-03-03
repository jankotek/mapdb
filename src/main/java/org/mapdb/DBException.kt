package org.mapdb

import java.io.IOException
import java.nio.file.Path

/**
 * Exception hieroarchy for MapDB
 */
open class DBException(message: String?, cause: Throwable?) : RuntimeException(message, cause) {

    class NotSorted():DBException("Keys are not sorted")

    class WrongConfiguration(message: String) : DBException(message) {}

    constructor(message: String):this(message, null)


    class OutOfMemory(e: Throwable) : VolumeIOError(
            if ("Direct buffer memory" == e.message)
                "Out of Direct buffer memory. Increase it with JVM option '-XX:MaxDirectMemorySize=10G'"
            else
                e.message, e)


    class GetVoid(recid:Long): DBException("Record does not exist, recid="+recid);

    class WrongFormat(msg: String) : DBException(msg);
    class Interrupted(e:InterruptedException) : DBException("One of threads was interrupted while accessing store", e);
    open class DataCorruption(msg: String) : DBException(msg);

    class HeadChecksumBroken(msg:String):DataCorruption(msg);


    class PointerChecksumBroken():DataCorruption("Broken bit parity")

    class FileLocked(path: Path, exception: Exception):
            DBException("File is already opened and is locked: "+path, exception)


    open class VolumeClosed(msg:String?, e: Throwable?) : DBException(msg, e){
        constructor(e: Throwable):this(null, e)
        constructor(msg: String):this(msg,null)
    }

    open class VolumeClosedByInterrupt(e: Throwable?) : DBException("Thread was interrupted during IO, FileChannel closed in result", e){
    }

    open class VolumeIOError(msg:String?, e: Throwable?) : DBException(msg, e){
        constructor(e: IOException):this(null, e)
        constructor(msg: String):this(msg, null)
    }

    open class VolumeEOF(msg:String?, e: IOException?) : VolumeIOError(msg, e){
        constructor(e: IOException):this(null, e)
        constructor(msg: String):this(msg,null)
    }


    class VolumeMaxSizeExceeded(length: Long, requestedLength: Long) :
            DBException("Could not expand store. Maximal store size: $length, new requested size: $requestedLength")

    class SerializationError(e: Exception) : DBException(null, e);

}
