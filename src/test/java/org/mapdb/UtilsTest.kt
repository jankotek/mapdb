package org.mapdb

import org.junit.Test
import org.mapdb.TT.assertFailsWith
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class UtilsTest{



    @Test(timeout = 10000)
    fun single_entry_lock(){
        val lock = Utils.singleEntryLock()
        lock.lock()
        lock.unlock()

        lock.lock()
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.lock()
        }
        lock.unlock()
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.unlock()
        }
    }


    @Test(timeout = 10000)
    fun single_entry_read_write_lock(){
        val lock = Utils.SingleEntryReadWriteLock()
        lock.writeLock().lock()
        lock.writeLock().unlock()

        lock.writeLock().lock()
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock().lock()
        }
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock().lock()
        }

        lock.writeLock().unlock()
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock().unlock()
        }
    }

    @Test(timeout = 10000)
    fun single_entry_read_write_lock2(){
        val lock = Utils.SingleEntryReadWriteLock()

        lock.readLock().lock()
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock().lock()
        }
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock().lock()
        }

        lock.readLock().unlock()
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock().unlock()
        }
    }


    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock(){
        val lock = Utils.SingleEntryReadWriteSegmentedLock(16)
        lock.writeLock(1)
        lock.writeUnlock(1)

        lock.writeLock(1)
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock(1)
        }
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock(1)
        }

        lock.writeUnlock(1)
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeUnlock(1)
        }
    }

    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock2(){
        val lock = Utils.SingleEntryReadWriteSegmentedLock(16)

        lock.readLock(1)
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock(1)
        }
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock(1)
        }

        lock.readUnlock(1)
        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readUnlock(1)
        }
    }


    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock3(){
        val lock = Utils.SingleEntryReadWriteSegmentedLock(16)
        lock.writeLock(1)

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readUnlock(1)
        }

        lock.checkWriteLocked(1)
        lock.checkReadLocked(1)

        assertFailsWith(IllegalMonitorStateException::class.java) {
            lock.checkWriteLocked(2)
        }
        assertFailsWith(IllegalMonitorStateException::class.java) {
            lock.checkReadLocked(2)
        }

        lock.writeUnlock(1)

        lock.writeLock(1)

    }


    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock4(){
        val lock = Utils.SingleEntryReadWriteSegmentedLock(16)
        lock.readLock(1)

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.readUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class.java){
            lock.writeUnlock(1)
        }
        assertFailsWith(IllegalMonitorStateException::class.java) {
            lock.checkWriteLocked(1)
        }
        lock.checkReadLocked(1)


        assertFailsWith(IllegalMonitorStateException::class.java) {
            lock.checkWriteLocked(2)
        }
        assertFailsWith(IllegalMonitorStateException::class.java) {
            lock.checkReadLocked(2)
        }
        lock.readUnlock(1)

    }



    @Test(timeout = 10000)
    fun lockWriteAll(){
        val locks = Array<ReadWriteLock?>(10, { ReentrantReadWriteLock() })

        var locked = AtomicBoolean(false)
        val waitUntilFinish = TT.async {
            locks[5]!!.writeLock().lock()
            locked.set(true)
            Thread.sleep(500)
            locks[3]!!.writeLock().lock()

            locks[5]!!.writeLock().unlock()
            locks[3]!!.writeLock().unlock()

        }

        while(!locked.get()){}
        Utils.lockWriteAll(locks)

        waitUntilFinish()

        locks.forEach {
            assert((it!! as ReentrantReadWriteLock).isWriteLockedByCurrentThread)
        }
    }


    @Test(timeout = 10000)
    fun lockReadAll(){
        val locks = Array<ReadWriteLock?>(10, { ReentrantReadWriteLock() })


        var locked = AtomicBoolean(false)
        val waitUntilFinish = TT.async {
            locks[5]!!.writeLock().lock()
            locked.set(true)
            Thread.sleep(500)
            locks[3]!!.writeLock().lock()

            locks[5]!!.writeLock().unlock()
            locks[3]!!.writeLock().unlock()

        }

        //wait until first lock is locked
        while(!locked.get()){}

        Utils.lockReadAll(locks)

        waitUntilFinish()

        locks.forEach {
            assert((it!! as ReentrantReadWriteLock).isWriteLocked.not())
            assert((it!! as ReentrantReadWriteLock).readHoldCount==1)
        }
    }
}