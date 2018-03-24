package org.mapdb.util

import org.mapdb.CC
import java.util.concurrent.*
import java.util.concurrent.locks.*

/**
 * Lock related utilities
 */




inline fun <E> ReadWriteLock?.lockWrite(f:()->E):E{
    if(this!=null)
        this.writeLock().lock()
    try{
        return f.invoke()
    }finally{
        if(this!=null)
            this.writeLock().unlock()
    }
}

inline fun <E> ReadWriteLock?.lockRead(f:()->E):E{
    if(this!=null)
        this.readLock().lock()
    try{
        return f.invoke()
    }finally{
        if(this!=null)
            this.readLock().unlock()
    }
}



inline fun <E> SingleEntryReadWriteSegmentedLock?.lockWriteAll(f:()->E):E{
    this?.lockWriteAll()
    try{
        return f.invoke()
    }finally{
        this?.unlockWriteAll()
    }
}

inline fun <E>  SingleEntryReadWriteSegmentedLock?.lockReadAll(f:()->E):E{
    this?.lockReadAll()
    try{
        return f.invoke()
    }finally{
        this?.unlockReadAll()
    }
}




inline fun <E> SingleEntryReadWriteSegmentedLock?.lockWrite(segment:Int, f:()->E):E{
    this?.writeLock(segment)
    try{
        return f.invoke()
    }finally{
        this?.writeUnlock(segment)
    }
}

inline fun <E> SingleEntryReadWriteSegmentedLock?.lockRead(segment:Int, f:()->E):E{
    this?.readLock(segment)
    try{
        return f.invoke()
    }finally{
        this?.readUnlock(segment)
    }
}



fun ReadWriteLock?.assertReadLock() {
    if(CC.PARANOID && this is ReentrantReadWriteLock && this.readLockCount==0 && !this.isWriteLockedByCurrentThread)
        throw AssertionError("not read locked")
    if(CC.PARANOID && this is SingleEntryReadWriteLock && this.readLockCount()==0 && !this.isWriteLockedByCurrentThread())
        throw AssertionError("not read locked")
}

fun ReadWriteLock?.assertWriteLock() {
    if(CC.PARANOID && this is ReentrantReadWriteLock && !this.isWriteLockedByCurrentThread)
        throw AssertionError("not write locked")
    if(CC.PARANOID && this is SingleEntryReadWriteLock && !this.isWriteLockedByCurrentThread())
        throw AssertionError("not write locked")
}

inline fun <E> Lock?.lock(body: () -> E):E {
    this?.lock()
    try{
        return body()
    }finally{
        this?.unlock()
    }
}


class SingleEntryReadWriteLock: ReadWriteLock {

    private val lock: ReentrantReadWriteLock = ReentrantReadWriteLock()

    private val readLockThreads = ConcurrentHashMap<Thread,Lock>()

    fun checkNotWriteLocked(){
        if (lock.isWriteLockedByCurrentThread)
            throw IllegalMonitorStateException("can not lock, already locked for write by current thread")

    }

    fun checkNotReadLocked() {
         if(readLockThreads.containsKey(Thread.currentThread()))
            throw IllegalMonitorStateException("can not lock, already locked for read by current thread")
    }

    fun checkNotLocked(){
        checkNotWriteLocked()
        checkNotReadLocked()
    }

    fun checkWriteLocked() {
        if (!lock.isWriteLockedByCurrentThread)
            throw IllegalMonitorStateException("not locked for write")
    }

    /** checks if locked for read or locked for write, fails if not locked */
    fun checkReadLocked() {
        if(!lock.isWriteLockedByCurrentThread
                && !readLockThreads.containsKey(Thread.currentThread()))
            throw IllegalMonitorStateException("not locked for read")
    }

    /** checks if locked for read, but fails if not locked or locked for write */
    fun checkReadLockedStrict() {
        if(lock.isWriteLocked)
            throw IllegalMonitorStateException("is write locked")
        if(lock.readHoldCount!=1)
            throw IllegalMonitorStateException("readHoldCount is wrong")
        if(!readLockThreads.containsKey(Thread.currentThread()))
            throw IllegalMonitorStateException("not locked for read")
    }


    private val origWriteLock = lock.writeLock()
    private val origReadLock = lock.readLock()

    private val newWriteLock = object: Lock {
        override fun unlock() {
            origWriteLock.unlock()
        }

        override fun tryLock(): Boolean {
            checkNotLocked()
            return origWriteLock.tryLock()
        }

        override fun tryLock(time: Long, unit: TimeUnit?): Boolean {
            checkNotLocked()
            return origWriteLock.tryLock(time, unit)
        }

        override fun newCondition(): Condition {
            throw UnsupportedOperationException()
        }

        override fun lock() {
            checkNotLocked()
            origWriteLock.lock()
        }

        override fun lockInterruptibly() {
            checkNotLocked()
            origWriteLock.lockInterruptibly()
        }
    }

    private val newReadLock = object: Lock {

        override fun tryLock(): Boolean {
            checkNotLocked()
            val r =  origReadLock.tryLock()
            if(r)
                readLockThreads.put(Thread.currentThread(), this)
            return r
        }

        override fun tryLock(time: Long, unit: TimeUnit?): Boolean {
            checkNotLocked()
            val r = origReadLock.tryLock(time, unit)
            if(r)
                readLockThreads.put(Thread.currentThread(), this)
            return r
        }

        override fun newCondition(): Condition {
            throw UnsupportedOperationException()
        }

        override fun lock() {
            checkNotWriteLocked()
            if(readLockThreads.put(Thread.currentThread(), this)!=null)
                throw IllegalMonitorStateException("can not lock, already locked for read by current thread")

            origReadLock.lock()
        }

        override fun lockInterruptibly() {
            checkNotWriteLocked()
            if(readLockThreads.put(Thread.currentThread(), this)!=null)
                throw IllegalMonitorStateException("can not lock, already locked for read by current thread")
            origReadLock.lockInterruptibly()
        }

        override fun unlock() {
            readLockThreads.remove(Thread.currentThread()) ?:
                    throw IllegalMonitorStateException("Can not unlock, current thread does not have a lock")
            origReadLock.unlock()
        }
    }

    override fun writeLock() = newWriteLock
    override fun readLock() = newReadLock

    fun readLockCount(): Int = readLockThreads.size
    fun isWriteLockedByCurrentThread(): Boolean = lock.isWriteLockedByCurrentThread()
    fun isReadLockedByCurrentThread(): Boolean = readLockThreads.containsKey(Thread.currentThread())

}

class SingleEntryLock(val lock: ReentrantLock = ReentrantLock()): Lock by lock{
    override fun lock() {
        if(lock.isHeldByCurrentThread)
            throw IllegalMonitorStateException("already locked by current thread")
        lock.lock()
    }

    override fun lockInterruptibly() {
        if(lock.isHeldByCurrentThread)
            throw IllegalMonitorStateException("already locked by current thread")

        lock.lockInterruptibly()
    }

}


fun newLock(threadSafe: Boolean): Lock? {
    return if(CC.PARANOID){
        if(threadSafe) SingleEntryLock()
        else null   //TODO assert no reentry in single threaded mode
    }else{
        if(threadSafe) ReentrantLock()
        else null
    }
}


fun newReadWriteSegmentedLock(threadSafe: Boolean, segmentCount:Int): SingleEntryReadWriteSegmentedLock? =
        if(threadSafe) SingleEntryReadWriteSegmentedLock(segmentCount)
        else null

fun newReadWriteLock(threadSafe: Boolean): ReadWriteLock? {
    return if(CC.PARANOID){
        if(threadSafe) SingleEntryReadWriteLock()
        else null; //TODO assert no reentry even in thread safe mode
    }else{
        if(threadSafe) ReentrantReadWriteLock()
        else null
    }
}

fun Lock?.assertLocked() {
    if(CC.PARANOID &&
            ((this is ReentrantLock && this.isHeldByCurrentThread.not())
                    || this is SingleEntryLock && this.lock.isHeldByCurrentThread.not()))
        throw AssertionError("Not locked")
}


class SingleEntryReadWriteSegmentedLock(
        val segmentCount:Int
){

    private val locks = Array(segmentCount, { SingleEntryReadWriteLock() })

    private fun s(segment:Int) = segment % locks.size

    private fun l(segment:Int) = locks[s(segment)]

    fun writeLock(segment:Int){
        for(lock in locks)
            lock.checkNotLocked()

        l(segment).writeLock().lock()
    }

    fun writeUnlock(segment:Int){
        l(segment).writeLock().unlock()
    }

    fun readLock(segment:Int){
        for(lock in locks)
            lock.checkNotLocked()

        l(segment).readLock().lock()
    }

    fun readUnlock(segment:Int){
        l(segment).readLock().unlock()
    }

    fun checkReadLocked(segment:Int){
        l(segment).checkReadLocked()
    }

    fun checkWriteLocked(segment:Int){
        l(segment).checkWriteLocked()
    }

    fun checkAllReadLocked() {
        for(lock in locks)
            lock.checkReadLocked()
    }


    fun checkAllReadLockedStrict() {
        for(lock in locks)
            lock.checkReadLockedStrict()
    }



    fun checkAllWriteLocked() {
        for(lock in locks)
            lock.checkWriteLocked()
    }

    fun lockReadAll() {
        while(true) {
            var i = 0;
            while(i<locks.size){
                //try to lock all locks
                val lock = locks[i++]

                if(!lock.readLock().tryLock()){
                    i--
                    //could not lock, rollback all locks
                    while(i>0){
                        locks[--i].readLock().unlock()
                    }

                    Thread.sleep(0, 100*1000)
                    //and try again to lock all
                    i = 0
                    continue
                }
            }
            return //all locked fine
        }
    }

    fun unlockReadAll() {
        for(lock in locks)
            lock.checkReadLocked()

        //unlock in reverse order to prevent deadlock
        for(i in locks.size-1 downTo 0)
            locks[i].readLock().unlock()
    }

    //TODO unify 'lockWriteAll' naming convention (order of words 'all' and 'lock')
    fun lockWriteAll() {
        while(true) {
            var i = 0;
            while(i<locks.size){
                //try to lock all locks
                val lock = locks[i++]

                if(!lock.writeLock().tryLock()){
                    i--
                    //could not lock, rollback all locks
                    while(i>0){
                        locks[--i].writeLock().unlock()
                    }

                    Thread.sleep(0, 100*1000)
                    //and try again to lock all
                    i = 0
                    continue
                }
            }
            return //all locked fine
        }
    }

    fun unlockWriteAll() {
        for(lock in locks)
            lock.checkWriteLocked()

        //unlock in reverse order to prevent deadlock
        for(i in locks.size-1 downTo 0) {
            locks[i].writeLock().unlock()
        }
    }

    fun  isWriteLockedByCurrentThread(segment: Int): Boolean = l(segment).isWriteLockedByCurrentThread()
    fun isReadLockedByCurrentThread(segment: Int): Boolean = l(segment).isReadLockedByCurrentThread()

}


@JvmField val FAKE_LOCK: Lock = object  : Lock {
    override fun unlock() {}

    override fun lockInterruptibly() {}

    override fun newCondition(): Condition {
        throw UnsupportedOperationException("condition not implemented on FakeLock")
    }

    override fun lock() {}

    override fun tryLock(): Boolean = true

    override fun tryLock(time: Long, unit: TimeUnit): Boolean = true
}

/** Thread unsafe lock, which wraps some code and ensures no double entry into section */
class SingleProtectionLock(val name:String): Lock {

    @Volatile var locked:Boolean = false;

    override fun lockInterruptibly() {
        lock()
    }

    override fun newCondition(): Condition {
        throw UnsupportedOperationException()
    }

    override fun tryLock(): Boolean {
        lock()
        return true
    }

    override fun tryLock(time: Long, unit: TimeUnit): Boolean {
        lock()
        return true;
    }

    override fun unlock() {
        if(!locked)
            throw IllegalStateException(name+": Not locked")
        locked = false
    }

    override fun lock() {
        if(!locked)
            throw IllegalStateException(name+": Already locked")
        locked = true
    }

}

