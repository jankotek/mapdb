package org.mapdb

import java.nio.file.Path

open class DBException(msg:String): RuntimeException(msg) {

    class RecidNotFound():DBException("recid was not found")

    class StoreClosed(): DBException("Store was closed")

    class PointerChecksumBroken():DBException("data corrupted")

    open class WrongConfig(msg: String) : DBException(msg)

    class WrongSerializer(msg:String) : WrongConfig(msg)

    class StoreReentry(): DBException("Can not modify store during updateAtomic")

    class DataAssert(msg:String = "data corrupted"):DBException(msg)


    class FileLocked(path: Path):DBException("File locked: $path")
}