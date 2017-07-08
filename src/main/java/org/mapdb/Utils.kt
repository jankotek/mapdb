package org.mapdb

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

    class SingleEntryReadWriteLock(
            val lock:ReentrantReadWriteLock=ReentrantReadWriteLock()
    ):ReadWriteLock by lock{

        val origWriteLock = lock.writeLock()
        val newWriteLock = object: Lock by origWriteLock{
            private fun ensureNotLocked() {
                if (lock.isWriteLockedByCurrentThread)
                    throw IllegalMonitorStateException("already locked by current thread")
            }

            override fun lock() {
                ensureNotLocked()
                origWriteLock.lock()
            }

            override fun lockInterruptibly() {
                ensureNotLocked()
                origWriteLock.lockInterruptibly()
            }
        }

        override fun writeLock() = newWriteLock
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
        return if(CC.PARANOID){
            if(threadSafe) SingleEntryLock()
            else null   //TODO assert no reentry in single threaded mode
        }else{
            if(threadSafe) ReentrantLock()
            else null
        }
    }

    fun newReadWriteLock(threadSafe: Boolean): ReadWriteLock? {
        return if(CC.PARANOID){
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

    fun lockReadAll(locks: Array<ReadWriteLock?>) {
        if(locks==null)
            return
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
        vals.forEach { a.put(it,"") }
        return a.size
    }


}