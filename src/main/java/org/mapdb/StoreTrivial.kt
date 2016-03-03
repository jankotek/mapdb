package org.mapdb

import org.eclipse.collections.api.LongIterable
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack
import java.io.*
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.*
import java.util.*
import java.util.concurrent.locks.ReadWriteLock


/**
 * Store which serializes its content into primitive `Map<Long,byte[]>`.
 * It optionally persist its content into file, in this case it supports rollback and durability.
 */
open class StoreTrivial(
        override val isThreadSafe:Boolean=true
    ):Store {

    internal val lock: ReadWriteLock? = Utils.newReadWriteLock(isThreadSafe)

    private @Volatile var closed = false;

    /** stack of deleted recids, those will be reused*/
    //TODO check for duplicates in freeRecids
    private val freeRecids = LongArrayStack();
    /** maximal allocated recid. All other recids should be in `freeRecid` stack or in `records`*/
    @Volatile  private var maxRecid:Long = 0;

    /** Stores data */
    private val records = LongObjectHashMap<ByteArray>();


    companion object {
        private val NULL_RECORD = ByteArray(0);
    }

    fun loadFrom(inStream: InputStream){
        Utils.lockWrite(lock){
            loadFromInternal(inStream)
        }
    }

    internal fun loadFromInternal(inStream: InputStream){
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)

        var maxRecid2 = 0L;
        freeRecids.clear()
        records.clear();

        //fill recids
        recidLoop@ while (true) {
            val recid = DBUtil.unpackLong(inStream)
            if (recid == 0L)
                break@recidLoop
            maxRecid2 = Math.max(maxRecid2, recid)
            var size = DBUtil.unpackLong(inStream) - 1
            var data = NULL_RECORD
            if (size >= 0) {
                data = ByteArray((size).toInt())
                DBUtil.readFully(inStream, data)
            }

            records.put(recid, data)
        }
        //fill free recids
        for (recid in 1..maxRecid2) {
            if (!records.containsKey(recid))
                freeRecids.push(recid)
        }
        maxRecid = maxRecid2

        Utils.logDebug { "Loaded ${records.size()} objects" }
    }

    fun saveTo(outStream: OutputStream) {
        Utils.lockRead(lock) {
            val recidIter = records.keySet().longIterator()
            //ByteArray has no equal method, must compare one by one
            while (recidIter.hasNext()) {
                val recid = recidIter.next();
                val bytes = records.get(recid)
                DBUtil.packLong(outStream, recid)
                val sizeToWrite: Long =
                        if (bytes === NULL_RECORD) {
                            -1L
                        } else {
                            bytes.size.toLong()
                        }
                DBUtil.packLong(outStream, sizeToWrite + 1L)

                if (sizeToWrite >= 0)
                    outStream.write(bytes)
            }

            //zero recid marks end
            DBUtil.packLong(outStream, 0L)

            Utils.logDebug { "Saved ${records.size()} records" }
        }
    }

    override fun preallocate(): Long {
        Utils.lockWrite(lock) {
            return preallocateInternal();
        }
    }

    private fun preallocateInternal(): Long {
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)

        val recid =
                if (freeRecids.isEmpty)
                    ++maxRecid
                else
                    freeRecids.pop()

        val old = records.put(recid, NULL_RECORD)
        if (old != null)
            throw DBException.DataCorruption("Old data were not null");

        return recid
    }

    override fun <R> put(record: R?, serializer: Serializer<R>): Long {
        val bytes = toByteArray(record, serializer)
        Utils.lockWrite(lock) {
            val recid = preallocateInternal()
            val old =records.put(recid, bytes)
            if(CC.ASSERT && old!=NULL_RECORD)
                throw AssertionError("wrong preallocation")
            return recid;
        }
    }


    override fun <R> update(recid: Long, record: R?, serializer: Serializer<R>) {
        val bytes = toByteArray(record, serializer)
        Utils.lockWrite(lock) {
            val old = records.get(recid)
                    ?: throw DBException.GetVoid(recid);

            records.put(recid, bytes)
        }
    }

    override fun <R> compareAndSwap(recid: Long, expectedOldRecord: R?, newRecord: R?, serializer: Serializer<R>): Boolean {
        val expectedOld:ByteArray = toByteArray(expectedOldRecord, serializer)

        //TODO stamped lock?
        Utils.lockWrite(lock) {
            val old = records.get(recid)
                    ?: throw DBException.GetVoid(recid);

            //handle nulls, compare by reference equality
            if (expectedOldRecord == null && !(old === NULL_RECORD)) {
                return false
            }

            if (!Arrays.equals(expectedOld, old)) {
                return false
            }

            records.put(recid, toByteArray(newRecord, serializer))
            return true
        }
    }

    override fun <R> delete(recid: Long, serializer: Serializer<R>) {
        Utils.lockWrite(lock) {
            val old = records.get(recid)
                    ?: throw DBException.GetVoid(recid);

            records.remove(recid)
            freeRecids.push(recid)
        }
    }

    override fun commit() {
    }

    override fun compact() {
    }

    override fun close() {
        if(CC.PARANOID) {
            Utils.lockRead(lock) {
                val freeRecidsSet = LongHashSet();
                freeRecidsSet.addAll(freeRecids)
                for (recid in 1..maxRecid) {
                    if (!freeRecidsSet.contains(recid) && !records.containsKey(recid))
                        throw AssertionError("Recid not used " + recid);
                }
            }
        }
        closed = true
    }

    override fun isClosed() = closed

    override fun <R> get(recid: Long, serializer: Serializer<R>): R? {
        val bytes:ByteArray? =
            Utils.lockRead(lock) {
                records.get(recid)
            }
        if(bytes===null){
            throw DBException.GetVoid(recid); //does not exist
        }

        if(bytes===NULL_RECORD)
            return null;

        val dataIn = DataInput2.ByteArray(bytes)
        return serializer.deserialize(dataIn, bytes.size)
    }

    fun clear(){
        Utils.lockWrite(lock){
            clearInternal()
        }
    }

    internal fun clearInternal(){
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        records.clear()
        freeRecids.clear()
        maxRecid = 0
    }

    private fun <R> toByteArray(record: R?, serializer: Serializer<R>): ByteArray {
        if(record === null)
            return NULL_RECORD
        val out = DataOutput2()
        serializer.serialize(out, record)
        return out.copyBytes();
    }


    override fun equals(other: Any?): Boolean {
        if (other !is StoreTrivial)
            return false

        Utils.lockRead(lock) {
            if (records.size() != other.records.size())
                return false;

            val recidIter = records.keySet().longIterator()
            //ByteArray has no equal method, must compare one by one
            while (recidIter.hasNext()) {
                val recid = recidIter.next();
                val b1 = records.get(recid)
                val b2 = other.records.get(recid)

                if (b1 !== b2 && !Arrays.equals(b1, b2)) {
                    return false;
                }

                if (b1 === NULL_RECORD)
                    return false;
            }

            return freeRecids.equals(other.freeRecids)
        }
    }


    override fun getAllRecids(): LongIterator {
        Utils.lockRead(lock) {
            return records.keySet().toArray().iterator()
        }
    }

    override fun verify() {
    }

}

class StoreTrivialTx(val file:File, isThreadSafe:Boolean=true)
    :StoreTrivial(
        isThreadSafe = isThreadSafe
    ), StoreTx{

    val path = file.toPath()

    companion object{
        internal val COMMIT_MARKER_SUFFIX = ".c";
        internal val DATA_SUFFIX = ".d";
    }

    private val fileChannel: FileChannel =
                FileChannel.open(path, StandardOpenOption.READ,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE)

    private val fileLock: FileLock =
                try {
                    fileChannel.tryLock()
                }catch(e: OverlappingFileLockException) {
                    throw DBException.FileLocked(path!!, e)
                }

    private var lastFileNum:Long = -1

    init{
        Utils.lockWrite(lock){
            Utils.logDebug { "Opened file ${path}"}
            val lattest = findLattestCommitMarker()
            lastFileNum = lattest ?: -1L;
            if(lattest!=null) {
                loadFrom(lattest);
            }
        }
    }
    internal  fun findLattestCommitMarker():Long?{
        Utils.assertReadLock(lock)
        if(null == path)
            return null

        var highestCommitNumber = -1L;

        val name = path.fileName!!.toString()

        for(child in Files.list(path.parent)){
            if(!Files.isRegularFile(child))
                continue
            val cname = child.fileName!!.toString()
            if(!cname.startsWith(name))
                continue
            if(!cname.endsWith(COMMIT_MARKER_SUFFIX))
                continue;

            //parse number
            val splited = cname.toString().split('.');
            try {
                val commitNumber = java.lang.Long.valueOf(splited[splited.size - 2])
                if(commitNumber>highestCommitNumber){
                    highestCommitNumber = commitNumber
                }
            }catch(e:NumberFormatException){
                //not a number, ignore this file
                continue
            }
        }

        return if(highestCommitNumber==-1L)
               null
            else
                highestCommitNumber
    }


    internal fun loadFrom(number:Long){
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        val readFrom = Utils.pathChangeSuffix(path, "."+number + DATA_SUFFIX)

        Utils.logDebug { "Loading from ${readFrom} with length ${readFrom.toFile().length()}" }
        Files.newInputStream(readFrom, StandardOpenOption.READ).buffered().use {
            loadFromInternal(it)
        }
    }

    override fun commit() {
        Utils.lockRead(lock) {
            val prev = lastFileNum;
            val next = prev + 1;

            //save to file
            val saveTo = Utils.pathChangeSuffix(path, "." + next + DATA_SUFFIX)
            Files.newOutputStream(saveTo, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE).buffered().use {
                saveTo(it)
            }
            //create commit marker
            Files.createFile(Utils.pathChangeSuffix(path, "." + next + COMMIT_MARKER_SUFFIX))
            lastFileNum = next
            //delete old data
            Files.deleteIfExists(Utils.pathChangeSuffix(path, "." + prev + COMMIT_MARKER_SUFFIX))
            Files.deleteIfExists(Utils.pathChangeSuffix(path, "." + prev + DATA_SUFFIX))

            Utils.logDebug { "Commited into ${saveTo} with length ${saveTo.toFile().length()}" }
        }
    }

    override fun rollback() {
        Utils.lockWrite(lock) {
            if(lastFileNum===-1L){
                //no commit was made yet, revert to empty state
                clearInternal()
                return
            }
            loadFrom(lastFileNum)
        }
    }

    override fun close() {
        Utils.lockWrite(lock) {
            fileLock.release();
            fileChannel.close()
            super.close()
        }
    }



}