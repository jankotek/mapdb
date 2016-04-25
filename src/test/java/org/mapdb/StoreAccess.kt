package org.mapdb.StoreAccess

import org.eclipse.collections.api.list.primitive.MutableLongList
import org.fest.reflect.core.Reflection
import org.mapdb.StoreDirectAbstract
import org.mapdb.volume.Volume
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock


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


val StoreDirectAbstract.locks: Array<ReadWriteLock?>
    get() = Reflection.method("getLocks").`in`(this).invoke() as Array<ReadWriteLock?>

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


fun StoreDirectAbstract.calculateFreeSize(): Long =
        Reflection.method("calculateFreeSize")
                .`in`(this)
                .invoke() as Long

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

