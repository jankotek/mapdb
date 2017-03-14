@file:Suppress("CAST_NEVER_SUCCEEDS")

package org.mapdb.StoreAccess

import org.eclipse.collections.api.list.primitive.MutableLongList
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.fest.reflect.core.Reflection
import org.mapdb.*
import org.mapdb.volume.SingleByteArrayVol
import org.mapdb.volume.Volume
import java.util.concurrent.locks.Lock


val StoreDirectAbstract.maxRecid: Long
    get() = Reflection.method("getMaxRecid").withReturnType(Long::class.java).`in`(this).invoke()

val StoreDirectAbstract.dataTail: Long
    get() = Reflection.method("getDataTail").withReturnType(Long::class.java).`in`(this).invoke()

val StoreDirectAbstract.volume: Volume
    get() = Reflection.method("getVolume").withReturnType(Volume::class.java).`in`(this).invoke()

val StoreDirectAbstract.indexPages: MutableLongList
    get() = Reflection.method("getIndexPages").withReturnType(MutableLongList::class.java).`in`(this).invoke()

val StoreDirectAbstract.structuralLock: Lock?
    get() = Reflection.method("getStructuralLock").`in`(this).invoke() as Lock?


val StoreDirectAbstract.locks: Utils.SingleEntryReadWriteSegmentedLock?
    get() = Reflection.method("getLocks").`in`(this).invoke() as Utils.SingleEntryReadWriteSegmentedLock?

fun StoreDirectAbstract.indexValCompose(size: Long,
                                        offset: Long,
                                        linked: Int,
                                        unused: Int,
                                        archive: Int
): Long = Reflection.method("indexValCompose")
        .withParameterTypes(size.javaClass, offset.javaClass, linked.javaClass, unused.javaClass, archive.javaClass)
        .`in`(this)
        .invoke(size, offset, linked, unused, archive) as Long


fun StoreDirectAbstract.allocateNewPage(): Long =
        Reflection.method("allocateNewPage")
                .`in`(this)
                .invoke() as Long

fun StoreDirectAbstract.allocateRecid(): Long =
        Reflection.method("allocateRecid")
                .`in`(this)
                .invoke() as Long


fun StoreDirectAbstract.calculateFreeSize(): Long {
    return Utils.lock(this.structuralLock) {
        Reflection.method("calculateFreeSize")
                .`in`(this)
                .invoke() as Long
    }
}

fun StoreDirectAbstract.allocateNewIndexPage(): Long =
        Reflection.method("allocateNewIndexPage")
                .`in`(this)
                .invoke() as Long


fun StoreDirectAbstract.getIndexVal(recid: Long): Long =
        Reflection.method("getIndexVal")
                .withParameterTypes(recid.javaClass)
                .`in`(this)
                .invoke(recid) as Long

fun StoreDirectAbstract.recidToOffset(recid: Long): Long =
        Reflection.method("recidToOffset")
                .withParameterTypes(recid.javaClass)
                .`in`(this)
                .invoke(recid) as Long

fun StoreDirectAbstract.allocateData(size: Int, recursive: Boolean): Long =
        Reflection.method("allocateData")
                .withParameterTypes(size.javaClass, recursive.javaClass)
                .`in`(this)
                .invoke(size, recursive) as Long

fun StoreDirectAbstract._longStackTake(masterLinkOffset: Long, recursive: Boolean): Long =
        Reflection.method("longStackTake")
                .withParameterTypes(masterLinkOffset.javaClass, recursive.javaClass)
                .`in`(this)
                .invoke(masterLinkOffset, recursive) as Long

fun StoreDirect._longStackForEach(masterLinkOffset: Long, body: Function1<Long, Unit>) {
    Reflection.method("longStackForEach")
            .withParameterTypes(masterLinkOffset.javaClass, Function1::class.java, Function2::class.java)
            .`in`(this)
            .invoke(masterLinkOffset, body, null)
}


fun StoreDirectAbstract._longStackPut(masterLinkOffset: Long, value: Long, recursive: Boolean) {
    Reflection.method("longStackPut")
            .withParameterTypes(masterLinkOffset.javaClass, value.javaClass, recursive.javaClass)
            .`in`(this)
            .invoke(masterLinkOffset, value, recursive)
}


fun StoreDirectAbstract.linkedRecordPut(output: ByteArray, size: Int): Long =
        Reflection.method("linkedRecordPut")
                .withParameterTypes(output.javaClass, size.javaClass)
                .`in`(this)
                .invoke(output, size) as Long

fun StoreDirectAbstract.indexValFlagLinked(indexValue: Long): Boolean =
        Reflection.method("indexValFlagLinked")
                .withParameterTypes(indexValue.javaClass)
                .`in`(this)
                .invoke(indexValue) as Boolean

fun StoreDirectAbstract.linkedRecordGet(indexValue: Long): ByteArray =
        Reflection.method("linkedRecordGet")
                .withParameterTypes(indexValue.javaClass)
                .`in`(this)
                .invoke(indexValue) as ByteArray




val StoreWAL.headVol:SingleByteArrayVol
    get() = Reflection.method("getHeadVol").`in`(this).invoke() as SingleByteArrayVol

/** stack pages, key is offset, value is content */
val StoreWAL.cacheStacks:LongObjectHashMap<ByteArray>
    get() = Reflection.method("getCacheStacks").`in`(this).invoke() as LongObjectHashMap<ByteArray>


/** modified indexVals, key is offset, value is indexValue */
val StoreWAL.cacheIndexValsA: Array<LongLongHashMap>
    get() = Reflection.method("getCacheIndexVals").`in`(this).invoke() as Array<LongLongHashMap>

val StoreWAL.cacheIndexLinks: LongLongHashMap
    get() = Reflection.method("getCacheIndexLinks").`in`(this).invoke() as LongLongHashMap

/** modified records, key is offset, value is WAL ID */
val StoreWAL.cacheRecords: Array<LongLongHashMap>
    get() = Reflection.method("getCacheRecords").`in`(this).invoke() as Array<LongLongHashMap>


val StoreWAL.wal: WriteAheadLog
    get() = Reflection.method("getWal").`in`(this).invoke() as WriteAheadLog


/** backup for `indexPages`, restored on rollback */
val StoreWAL.indexPagesBackup: Array<Long>
    get() = Reflection.method("getIndexPagesBackup").`in`(this).invoke() as Array<Long>


val StoreWAL.allocatedPages: LongArrayList
    get() = Reflection.method("getAllocatedPages").`in`(this).invoke() as LongArrayList
