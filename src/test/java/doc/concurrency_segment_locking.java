package doc;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class concurrency_segment_locking {
    public static void main(String[] args) {
        ReadWriteLock[] locks = new ReentrantReadWriteLock[16];
        int recid = 0;
        //a

        // read record from store
        locks[recid % locks.length].readLock().lock(); //note readLock
        try{
            //look up recid, deserialize and return
        }finally {
            locks[recid % locks.length].readLock().unlock();
        }

        // update record from store
        locks[recid % locks.length].writeLock().lock();
        try{

            //TODO finish update example
        }finally {
            locks[recid % locks.length].readLock().unlock();
        }
        //z
    }
}
