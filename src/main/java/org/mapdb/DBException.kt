package org.mapdb

import java.nio.file.Path

open class DBException: RuntimeException {

    constructor(cause:Exception): super(cause)
    constructor(msg:String): super(msg)
    constructor(msg:String, cause:Exception): super(msg, cause)

    class RecordNotFound():DBException("record not found")

    class RecidNotFound():DBException("recid was not found")

    class StoreClosed(): DBException("Store was closed")

    class PointerChecksumBroken():DBException("data corrupted")

    open class WrongConfig(msg: String) : DBException(msg)

    class WrongSerializer(msg:String) : WrongConfig(msg)

    class StoreReentry(): DBException("Can not modify store during updateAtomic")

    class DataAssert(msg:String = "data corrupted"):DBException(msg)

    class DataCorruption(msg:String = "data corrupted"):DBException(msg)


    //TODO add assertion to build system, if this is referenced from source code
    class TODO(msg:String = "not yet implemented"):DBException(msg)


    class FileLocked(path: Path):DBException("File locked: $path")


    class SerializationError(e:Exception):DBException(e)
}