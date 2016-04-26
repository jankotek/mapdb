package org.mapdb

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.TT.assertFailsWith


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


}