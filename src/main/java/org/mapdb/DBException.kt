package org.mapdb

open class DBException(msg:String): RuntimeException(msg) {

    class RecidNotFound():DBException("recid was not found")

    class StoreClosed(): DBException("Store was closed")

    class PointerChecksumBroken():DBException("data corrupted")

    class WrongConfig(msg: String) : DBException(msg)

    class StoreReentry(): DBException("Can not modify store during update")
}