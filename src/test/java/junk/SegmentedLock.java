package junk;

import org.mapdb.CC;

import java.util.Arrays;

/**
 * An locking mechanism which can lock parts (segments) of large file.
 *
 */
public class SegmentedLock {

    protected final int segmentSize;

    protected volatile long[] lockSegments = new long[32];
    protected volatile Thread[] lockThreads = new Thread[32];
    protected volatile int lockSize = 0;

    protected final Object lockLock = new Object();

    public SegmentedLock(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    public void lock(final long offset, final int len){
        if(CC.ASSERT && len>segmentSize) throw new IllegalArgumentException();

        final long seg1 = offset%segmentSize;
        final long seg2 = (offset+len)%segmentSize;
        final boolean seg12Differs = seg1!=seg2;
        boolean first = true;
        mainLoop:
        while(true){
            if(first) first = false;
            else Thread.yield();

            synchronized (lockLock){
                if(lockSize!=0){
                    //iterate over existing locks to check if something else is locked
                    for(int i=0;i <lockSize;i++){
                        if(lockSegments[i]==seg1 || (seg12Differs && lockSegments[i]==seg2)){
                            //this area is already locked, so release lock and try again in another spin
                            continue mainLoop;
                        }
                    }
                }
                //no locks found, so we can add self
                if(lockSize == lockSegments.length){
                    //grow if necessary, array never shrinks
                    lockSegments = Arrays.copyOf(lockSegments, lockSize * 2);
                    lockThreads = Arrays.copyOf(lockThreads, lockSize*2);
                }
                lockSegments[lockSize] = seg1;
                lockThreads[lockSize] = Thread.currentThread();
                lockSize++;
                if(seg12Differs){
                    //lock second segment as well
                    lockSegments[lockSize] = seg2;
                    lockThreads[lockSize] = Thread.currentThread();
                    lockSize++;
                }
                return;
            }
        }
    }

    public void unlock(final long offset, final int len){
        if(CC.ASSERT && len>segmentSize) throw new IllegalArgumentException();

        final long seg1 = offset%segmentSize;
        final long seg2 = (offset+len)%segmentSize;

        synchronized (lockLock){
            for(int i=0;i<lockSize;i++){
                if(seg1 == lockSegments[i]){
                    if(CC.ASSERT && lockThreads[i]!=Thread.currentThread()) throw new InternalError();

                    //remove by swapping with last item and decreasing size
                    lockSize--;
                    lockThreads[i] = lockThreads[lockSize];
                    lockSegments[i] = lockSegments[lockSize];

                    //prevent memory leak
                    lockThreads[lockSize] = null;

                    break;
                }
            }
            if(seg1!=seg2){
                for(int i=0;i<lockSize;i++){
                     if(seg2 == lockSegments[i]){
                         if(CC.ASSERT && lockThreads[i]!=Thread.currentThread()) throw new InternalError();

                        //remove by swapping with last item and decreasing size
                        lockSize--;
                        lockThreads[i] = lockThreads[lockSize];
                        lockSegments[i] = lockSegments[lockSize];

                         //prevent memory leak
                         lockThreads[lockSize] = null;

                         break;
                    }
                }
            }

        }

    }

    public void unlockAll(){
        final Thread currentThread = Thread.currentThread();
        synchronized (lockLock){
            for(int i=0;i<lockSize;i++){
                if(lockThreads[i] == currentThread){
                    //remove by swapping with last item and decreasing size
                    lockSize--;
                    lockThreads[i] = lockThreads[lockSize];
                    lockSegments[i] = lockSegments[lockSize];

                    //prevent memory leak
                    lockThreads[lockSize] = null;

                    //last item was moved to current position, so we need to try again on current position
                    i--;
                }
            }
        }
    }

}
