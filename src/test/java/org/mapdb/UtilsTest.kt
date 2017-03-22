package org.mapdb

import org.junit.Test
import org.mapdb.TT.assertFailsWith
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock


class UtilsTest{


    @Test fun single_entry_lock(){
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

    @Test fun single_entry_read_write_lock(){
        val lock = Utils.SingleEntryReadWriteLock().writeLock()
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
    fun lockWriteAll(){
        val locks = Array<ReadWriteLock?>(10, { ReentrantReadWriteLock() })

        var locked = AtomicBoolean(false)
        val waitUntilFinish = TT.async2 {
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
        val waitUntilFinish = TT.async2 {
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