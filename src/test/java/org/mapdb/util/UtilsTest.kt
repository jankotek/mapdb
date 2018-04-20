package org.mapdb.util

import org.junit.Test
import org.mapdb.TT
import org.mapdb.TT.assertFailsWith
import java.util.concurrent.CountDownLatch

class UtilsTest{



    @Test(timeout = 10000)
    fun single_entry_lock(){
        val lock = SingleEntryLock()
        lock.lock()
        lock.unlock()

        lock.lock()
        assertFailsWith(IllegalMonitorStateException::class){
            lock.lock()
        }
        lock.unlock()
        assertFailsWith(IllegalMonitorStateException::class){
            lock.unlock()
        }
    }


    @Test(timeout = 10000)
    fun single_entry_read_write_lock(){
        val lock = SingleEntryReadWriteLock()
        lock.writeLock().lock()
        lock.writeLock().unlock()

        lock.writeLock().lock()
        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock().lock()
        }
        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock().lock()
        }

        lock.writeLock().unlock()
        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock().unlock()
        }
    }

    @Test(timeout = 10000)
    fun single_entry_read_write_lock2(){
        val lock = SingleEntryReadWriteLock()

        lock.readLock().lock()
        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock().lock()
        }
        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock().lock()
        }

        lock.readLock().unlock()
        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock().unlock()
        }
    }


    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock(){
        val lock = SingleEntryReadWriteSegmentedLock(16)
        lock.writeLock(1)
        lock.writeUnlock(1)

        lock.writeLock(1)
        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock(1)
        }
        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock(1)
        }

        lock.writeUnlock(1)
        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeUnlock(1)
        }
    }

    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock2(){
        val lock = SingleEntryReadWriteSegmentedLock(16)

        lock.readLock(1)
        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock(1)
        }
        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock(1)
        }

        lock.readUnlock(1)
        assertFailsWith(IllegalMonitorStateException::class){
            lock.readUnlock(1)
        }
    }


    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock3(){
        val lock = SingleEntryReadWriteSegmentedLock(16)
        lock.writeLock(1)

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readUnlock(1)
        }

        lock.checkWriteLocked(1)
        lock.checkReadLocked(1)

        assertFailsWith(IllegalMonitorStateException::class) {
            lock.checkWriteLocked(2)
        }
        assertFailsWith(IllegalMonitorStateException::class) {
            lock.checkReadLocked(2)
        }

        lock.writeUnlock(1)

        lock.writeLock(1)

    }


    @Test(timeout = 10000)
    fun single_entry_read_write_segment_lock4(){
        val lock = SingleEntryReadWriteSegmentedLock(16)
        lock.readLock(1)

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeLock(1)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.readUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeUnlock(2)
        }

        assertFailsWith(IllegalMonitorStateException::class){
            lock.writeUnlock(1)
        }
        assertFailsWith(IllegalMonitorStateException::class) {
            lock.checkWriteLocked(1)
        }
        lock.checkReadLocked(1)


        assertFailsWith(IllegalMonitorStateException::class) {
            lock.checkWriteLocked(2)
        }
        assertFailsWith(IllegalMonitorStateException::class) {
            lock.checkReadLocked(2)
        }
        lock.readUnlock(1)

    }



    @Test(timeout = 10000)
    fun lockWriteAll(){
        if(TT.shortTest())
            return

        val locks = SingleEntryReadWriteSegmentedLock(10)

        val locked = CountDownLatch(2)
        val waitUntilFinish1 = TT.async {
            locks.writeLock(5)
            locked.countDown()
            Thread.sleep(500)
            locks.writeUnlock(5)
        }

        val waitUntilFinish2 = TT.async {
            locks.writeLock(3)
            locked.countDown()
            Thread.sleep(500)
            locks.writeUnlock(3)
        }

        locked.await()
        locks.lockWriteAll()
        waitUntilFinish1()
        waitUntilFinish2()

        locks.checkAllWriteLocked()
    }


    @Test(timeout = 10000)
    fun lockReadAll(){
        if(TT.shortTest())
            return

        val locks = SingleEntryReadWriteSegmentedLock(10)


        val locked = CountDownLatch(2)
        val waitUntilFinish1 = TT.async {
            locks.writeLock(5)
            locked.countDown()
            Thread.sleep(500)
            locks.writeUnlock(5)
        }

        val waitUntilFinish2 = TT.async {
            locks.writeLock(3)
            locked.countDown()
            Thread.sleep(500)
            locks.writeUnlock(3)
        }



        //wait until first lock is locked
        locked.await()

        locks.lockReadAll()

        waitUntilFinish1()
        waitUntilFinish2()

        locks.checkAllReadLockedStrict()
    }
}