package org.mapdb

import com.google.common.collect.MapMaker
import java.io.File
import java.nio.file.Path
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.*
import java.util.logging.Level
import java.util.logging.Logger

internal object Utils {

    @JvmField val FAKE_LOCK:Lock = object  :Lock{
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
    class SingleProtectionLock(val name:String):Lock{

        @Volatile var locked:Boolean = false;

        override fun lockInterruptibly() {
            lock();
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
                throw IllegalAccessError(name+": Not locked")
            locked = false
        }

        override fun lock() {
            if(!locked)
                throw IllegalAccessError(name+": Already locked")
            locked = true
        }

    }

    val LOG = Logger.getLogger("org.mapdb");

    /**
     * Return Path in the same parent folder, but with different suffix.
     */
    fun pathChangeSuffix(path: Path, suffix: String): Path {
        //TODO this might not work with alternative filesystems
        return File(path.toFile().path + suffix).toPath();
    }


    inline fun logDebug(msg:()->String ){
        if(CC.LOG && LOG.isLoggable(Level.FINE))
            LOG.log(Level.FINE,msg.invoke())
    }

    inline fun logInfo(msg:()->String ){
        if(LOG.isLoggable(Level.INFO))
            LOG.log(Level.INFO,msg.invoke())
    }

    inline fun <E> lockWrite(lock:ReadWriteLock?,f:()->E):E{
        if(lock!=null)
            lock.writeLock().lock()
        try{
            return f.invoke();
        }finally{
            if(lock!=null)
                lock.writeLock().unlock()
        }
    }

    inline fun <E> lockRead(lock:ReadWriteLock?,f:()->E):E{
        if(lock!=null)
            lock.readLock().lock()
        try{
            return f.invoke();
        }finally{
            if(lock!=null)
                lock.readLock().unlock()
        }
    }

    fun assertReadLock(lock: ReadWriteLock?) {
        if(CC.ASSERT && lock is ReentrantReadWriteLock && lock.readLockCount==0 && !lock.isWriteLockedByCurrentThread)
            throw AssertionError("not read locked");
        if(CC.ASSERT && lock is SingleEntryReadWriteLock && lock.lock.readLockCount==0 && !lock.lock.isWriteLockedByCurrentThread)
            throw AssertionError("not read locked");
    }

    fun assertWriteLock(lock: ReadWriteLock?) {
        if(CC.ASSERT && lock is ReentrantReadWriteLock && !lock.isWriteLockedByCurrentThread)
            throw AssertionError("not write locked");
        if(CC.ASSERT && lock is SingleEntryReadWriteLock && !lock.lock.isWriteLockedByCurrentThread)
            throw AssertionError("not write locked");
    }

    inline fun <E> lock(lock: Lock?, body: () -> E):E {
        lock?.lock()
        try{
            return body()
        }finally{
            lock?.unlock()
        }
    }


    fun roundDownToIntMAXVAL(size: Long?): Int {
        if (size!! > Integer.MAX_VALUE)
            return Integer.MAX_VALUE
        return size.toInt();
    }


    fun singleEntryLock():Lock{
        val lock = ReentrantLock()
        return object:Lock by lock{

            private fun ensureNotLocked() {
                if (lock.isHeldByCurrentThread)
                    throw IllegalMonitorStateException("already locked by current thread")
            }

            override fun lock() {
                ensureNotLocked()
                lock.lock()
            }


            override fun lockInterruptibly() {
                ensureNotLocked()
                lock.lockInterruptibly()
            }

        }
    }

    class SingleEntryReadWriteLock:ReadWriteLock{

        //TODO private
        val lock:ReentrantReadWriteLock=ReentrantReadWriteLock()

        private val readLockThreads = MapMaker().weakKeys().makeMap<Thread, Lock>()

        fun checkNotLocked() {
            if (lock.isWriteLockedByCurrentThread)
                throw IllegalMonitorStateException("can not lock, already locked for write by current thread")
            if(readLockThreads.containsKey(Thread.currentThread()))
                throw IllegalMonitorStateException("can not lock, already locked for read by current thread")
        }

        fun checkWriteLocked() {
            if (!lock.isWriteLockedByCurrentThread)
                throw IllegalMonitorStateException("not locked for write")
        }

        fun checkReadLocked() {
            if(!lock.isWriteLockedByCurrentThread
                    && !readLockThreads.containsKey(Thread.currentThread()))
                throw IllegalMonitorStateException("not locked for read")
        }


        private val origWriteLock = lock.writeLock()
        private val origReadLock = lock.readLock()

        private val newWriteLock = object: Lock{
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

        private val newReadLock = object: Lock{

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
                checkNotLocked()
                readLockThreads.put(Thread.currentThread(), this)
                origReadLock.lock()
            }

            override fun lockInterruptibly() {
                checkNotLocked()
                readLockThreads.put(Thread.currentThread(), this)
                origReadLock.lockInterruptibly()
            }

            override fun unlock() {
                readLockThreads.remove(Thread.currentThread())
                origReadLock.unlock()
            }
        }

        override fun writeLock() = newWriteLock
        override fun readLock() = newReadLock

    }

    class SingleEntryLock(val lock:ReentrantLock = ReentrantLock()): Lock by lock{
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
        return if(CC.ASSERT){
            if(threadSafe) SingleEntryLock()
            else null   //TODO assert no reentry in single threaded mode
        }else{
            if(threadSafe) ReentrantLock()
            else null
        }
    }

    fun newReadWriteLock(threadSafe: Boolean): ReadWriteLock? {
        return if(CC.ASSERT){
            if(threadSafe) SingleEntryReadWriteLock()
            else null; //TODO assert no reentry even in thread safe mode
        }else{
            if(threadSafe) ReentrantReadWriteLock()
            else null
        }
    }

    fun assertLocked(lock: Lock?) {
        if(CC.ASSERT &&
                ((lock is ReentrantLock && lock.isHeldByCurrentThread.not())
                  || lock is SingleEntryLock && lock.lock.isHeldByCurrentThread.not()))
            throw AssertionError("Not locked")

    }

    @JvmStatic fun <E> clone(value: E, serializer: Serializer<E>, out:DataOutput2 = DataOutput2()): E {
        out.pos = 0
        serializer.serialize(out, value)
        val in2 = DataInput2.ByteArray(out.copyBytes())
        return serializer.deserialize(in2, out.pos)
    }


    inline fun <E> lockWrite(locks: Array<ReadWriteLock?>,f:()->E):E{
        lockWriteAll(locks)
        try{
            return f.invoke();
        }finally{
            unlockWriteAll(locks)
        }
    }

    inline fun <E> lockRead(locks: Array<ReadWriteLock?>,f:()->E):E{
       lockReadAll(locks)
        try{
            return f.invoke();
        }finally{
            unlockReadAll(locks)
        }
    }

    fun lockReadAll(locks: Array<ReadWriteLock?>) {
        if(locks==null)
            return
        while(true) {
            var i = 0;
            while(i<locks.size){
                //try to lock all locks
                val lock = locks[i++]?: continue

                if(!lock.readLock().tryLock()){
                    i--
                    //could not lock, rollback all locks
                    while(i>0){
                        (locks[--i]?:continue).readLock().unlock()
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

    fun unlockReadAll(locks: Array<ReadWriteLock?>) {
        if(locks==null)
            return
        //unlock in reverse order to prevent deadlock
        for(i in locks.size-1 downTo 0)
            locks[i]!!.readLock().unlock()
    }

    fun lockWriteAll(locks: Array<ReadWriteLock?>) {
        if(locks==null)
            return
        while(true) {
            var i = 0;
            while(i<locks.size){
                //try to lock all locks
                val lock = locks[i++]?: continue

                if(!lock.writeLock().tryLock()){
                    i--
                    //could not lock, rollback all locks
                    while(i>0){
                        (locks[--i]?:continue).writeLock().unlock()
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

    fun unlockWriteAll(locks: Array<ReadWriteLock?>) {
        if(locks==null)
            return
        //unlock in reverse order to prevent deadlock
        for(i in locks.size-1 downTo 0) {
            val lock = locks[i]
            if (lock != null)
                lock.writeLock().unlock()
        }
    }

    fun identityCount(vals: Array<*>): Int {
        val a = IdentityHashMap<Any?, Any?>()
        vals.forEach { a.put(it, "") }
        return a.size
    }


    inline fun logExceptions(crossinline run:()->Unit):()->Unit = {
        try {
            run()
        }catch (e:Throwable){
            LOG.log(Level.SEVERE,"Exception in background task", e)
            throw e
        }
    }


    class SingleEntryReadWriteSegmentedLock(
            segmentCount:Int
    ){

        private val locks = Array(segmentCount, {SingleEntryReadWriteLock()})

        inline fun s(segment:Int) = segment % locks.size

        inline fun l(segment:Int) = locks[s(segment)]

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

    }
}