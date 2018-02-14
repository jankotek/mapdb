package org.mapdb.tree;

import org.mapdb.*;
import org.mapdb.StoreBinaryGetLong;
import org.mapdb.serializer.Serializer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Java utils for TreeArrayList
 */
public class IndexTreeListJava {
    static final int maxDirShift = 7;

    static final long full = 0xFFFFFFFFFFFFFFFFL;

    public static final Serializer<long[]> dirSer =  new Serializer<long[]>() {
        @Override
        public void serialize(DataOutput2 out, long[] value) throws IOException {

            if(CC.PARANOID){
                int len = 2 +
                        2*Long.bitCount(value[0])+
                        2*Long.bitCount(value[1]);

                if(len!=value.length)
                    throw new DBException.DataCorruption("bitmap!=len");
            }

            out.writeLong(value[0]);
            out.writeLong(value[1]);

            if(value.length==2)
                return;
            value = value.clone();

            long prev = value[3];

            //every second value is Index, those are incrementing and can be delta packed
            for(int i=5;i<value.length;i+=2){
                long old = value[i];
                value[i] = old-prev;
                prev = old;
            }

            out.packLongArray(value, 2, value.length);
        }


        @Override
        public long[] deserialize(DataInput2 in, int available) throws IOException {
            //there is bitmap at first 16 bytes, each non-zero long has bit set
            //to determine offset one must traverse bitmap and count number of bits set
            long bitmap1 = in.readLong();
            long bitmap2 = in.readLong();
            int len = 2+2*(Long.bitCount(bitmap1) + Long.bitCount(bitmap2));

            if (len == 2) {
                return dirEmpty();
            }

            long[] ret = new long[len];
            ret[0] = bitmap1;
            ret[1] = bitmap2;
            in.unpackLongArray(ret, 2, len);

            //unpack delta
            for(int i=5;i<ret.length;i+=2){
                ret[i] += ret[i-2];
            }
            return ret;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }
    };


    public static long[] dirEmpty(){
        return new long[2];
    }

    /** converts hash slot into actual offset in dir array, using bitmap */
    static final int dirOffsetFromSlot(long[] dir, int slot) {
        if(CC.PARANOID && slot>127)
            throw new DBException.DataCorruption("slot too high");

        int offset = 0;
        long v = dir[0];

        if(slot>63){
            offset+=Long.bitCount(v)*2;
            v = dir[1];
        }

        slot &= 63;
        long mask = ((1L)<<(slot&63))-1;
        offset += 2+Long.bitCount(v & mask)*2;

        int v2 = (int) ((v>>>(slot))&1);
        v2<<=1;

        //turn into negative value if bit is not set, do not use conditions
        return -offset + v2*offset;
    }

    static final int dirOffsetFromLong(long bitmap1, long bitmap2, int slot) {
        if(CC.PARANOID && slot>127)
            throw new DBException.DataCorruption("slot too high");

        int offset = 0;
        long v = bitmap1;

        if(slot>63){
            offset+=Long.bitCount(v)*2;
            v = bitmap2;
        }

        slot &= 63;
        long mask = ((1L)<<(slot&63))-1;
        offset += 2+Long.bitCount(v & mask)*2;

        int v2 = (int) ((v>>>(slot))&1);
        v2<<=1;

        //turn into negative value if bit is not set, do not use conditions
        return -offset + v2*offset;
    }


    static final long[] dirPut(long[] dir_, int slot, long v1, long v2){
        int offset = dirOffsetFromSlot(dir_, slot);
        //make copy and expand it if necessary
        if (offset < 0) {
            offset = -offset;
            dir_ = Arrays.copyOf(dir_, dir_.length + 2);
            //make space for new value
            System.arraycopy(dir_, offset, dir_, offset + 2, dir_.length - 2 - offset);
            //and update bitmap
            int bytePos = slot / 64;
            int bitPos = slot % 64;
            dir_[bytePos] = (dir_[bytePos] | (1L << bitPos));
        } else {
            dir_ = dir_.clone();
        }
        //and insert value itself
        dir_[offset] = v1;
        dir_[offset+1] = v2;
        return dir_;
    }

    static final long[] dirRemove(long[] dir, final int slot){
        int offset = dirOffsetFromSlot(dir, slot);
        if(CC.PARANOID && offset<=0){
            throw new DBException.DataCorruption("offset too low");
        }
        //shrink and copy data
        long[] dir2 = new long[dir.length - 2];
        System.arraycopy(dir, 0, dir2, 0, offset);
        System.arraycopy(dir, offset + 2, dir2, offset, dir2.length - offset);

        //unset bitmap bit
        int bytePos = slot / 64;
        int bitPos = slot % 64;
        dir2[bytePos] =  (dir2[bytePos] & ~(1L << bitPos));
        return dir2;
    }


    /**
     * Traverses tree structure
     *
     * @param recid starting directory
     * @param store to get next dir from
     * @param index in tree
     * @return value recid, 0 if not found
     */
    static final long treeGet(int dirShift, long recid, StoreImmutable store, int level, final long index) {
        if(CC.PARANOID && index<0)
            throw new AssertionError();
        if(CC.PARANOID && index>>>(level*dirShift)!=0)
            throw new AssertionError();
        if(CC.PARANOID && (dirShift<0||dirShift>maxDirShift))
            throw new AssertionError();

        if(!(store instanceof StoreBinary)) {
            //fallback for non binary store
            return treeGetNonBinary(dirShift, recid, store, level, index);
        }

        final StoreBinary binStore = (StoreBinary) store;
        return treeGetBinary(dirShift, recid, binStore, level, index);
    }

    private static long treeGetBinary(final int dirShift, long recid, StoreBinary binStore, int level, final long index) {
        for (; level>= 0;) {
            final int level2 = level;
            StoreBinaryGetLong f = (input, size) -> {
                long bitmap1 = input.readLong();
                long bitmap2 = input.readLong();

                //index
                int dirPos = dirOffsetFromLong(bitmap1, bitmap2, treePos(dirShift, level2, index));
                if(dirPos<0){
                    //not set
                    return 0L;
                }

                //second value is index, it is delta packed and can not be skipped, reenable binaryGet once its supported

                //skip until offset
                //input.unpackLongSkip(dirPos-2);
                long oldIndex=0;
                for(int i=0; i<(dirPos-2)/2;i++){
                    input.unpackLong();
                    oldIndex += input.unpackLong();
                }
                
                long recid1 = input.unpackLong();
                if(recid1 ==0)
                    return 0L; //TODO this should not be here, if tree collapse exist

                oldIndex += input.unpackLong()-1;

                if (oldIndex == index) {
                    //found it, return value (recid)
                    return recid1;
                }else  if (oldIndex != -1) {
                    // there is wrong index stored here, given index is not found
                    return 0L;
                }

                return -recid1; //continue
            };


            long ret = binStore.getBinaryLong(recid, f);
            if(ret>=0) {
                return ret;
            }
            recid = -ret;

            level--;

        }
        throw new DBException.DataCorruption("Cyclic reference in TreeArrayList");
    }

    private static long treeGetNonBinary(int dirShift, long recid, StoreImmutable store, int level, long index) {
        // tree structure
        // each iteration goes one level deeper
        for (; level>= 0;) {
            long[] dir = store.get(recid, dirSer);
            int dirPos = dirOffsetFromSlot(dir,treePos(dirShift, level, index));
            if(dirPos<0)
                return 0L; //slot is empty

            recid = dir[dirPos];
            if(recid==0)
                return 0L; //TODO this should not be here, if tree collapse exist

            long oldIndex = dir[dirPos +1]-1;

            if (oldIndex == index) {
                //found it, return value (recid)
                return recid;
            }else  if (oldIndex != -1) {
                // there is wrong index stored here, given index is not found
                return 0L;
            }

            // there is a reference to sub dir here
            // so move one level deeper
            level--;
        }
        throw new DBException.DataCorruption("Cyclic reference in TreeArrayList");
    }

    static final Long treeGetNullable(int dirShift, long recid, StoreImmutable store, int level, long index) {
        if(CC.PARANOID && index<0)
            throw new AssertionError();
        if(CC.PARANOID && index>>>(level*dirShift)!=0)
            throw new AssertionError();
        if(CC.PARANOID && (dirShift<0||dirShift>maxDirShift))
            throw new AssertionError();


        // tree structure
        // each iteration goes one level deeper
        for (; level>= 0;) {
            long[] dir = store.get(recid, dirSer);
            int dirPos = dirOffsetFromSlot(dir, treePos(dirShift, level, index));
            if(dirPos<0)
                return null; //slot is empty
            recid = dir[dirPos];
            long oldIndex = dir[dirPos +1]-1;

            if(oldIndex!=-1){
                //we found value
                return oldIndex==index?recid:null;
            }

            if(recid==0){
                return 0L; //TODO this should not be here, if tree collapse exist
            }

            // there is a reference to sub dir here
            // so move one level deeper
            level--;
        }
        throw new DBException.DataCorruption("Cyclic reference in TreeArrayList");
    }


    protected static int treePos(int dirShift, int level, long index) {
        int shift = dirShift*level;
        return (int) ((index >>> shift) & ((1<<dirShift)-1));
    }

    static final void treePut(
            int dirShift,
            long recid,
            final Store store,
            int level,
            final long index,
            long value){
        if(CC.PARANOID && index<0)
            throw new AssertionError();
        if(CC.PARANOID && index>>>(level*dirShift)!=0)
            throw new AssertionError();


        for(;level>=0;) {
            long[] dir = store.get(recid, dirSer);
            final int slot = treePos(dirShift, level, index);
            int dirPos = dirOffsetFromSlot(dir,slot);
            if(dirPos<0){
                //empty slot, just update
                dir = dirPut(dir, slot, value, index+1);
                store.update(recid, dir, dirSer);
                return;
            }

            final long oldVal = dir[dirPos];
            final long oldIndex = dir[dirPos + 1]-1;

            if (oldIndex == -1) {
                if (oldVal == 0) {
                    throw new IllegalStateException(); //empty pos, but that should be already covered by dirPos<0
                } else {
                    //dive deeper
                    recid = oldVal;
                    level--;
                    continue; // recursive call to treePut (sort of)
                }
            } else if (oldIndex == index) {
                //slot is occupied by the same index
                if (oldVal == value)
                    return; //do not update if same
                dir = dir.clone();
                dir[dirPos] = value;
                store.update(recid, dir, dirSer);
            } else {
                // is occupied by the different value, must split it
                dir = dir.clone();
                //recid of subdir
                dir[dirPos] = treePutSub(dirShift, store, level-1, index, value, oldIndex, oldVal);
                //this is turning into directory
                dir[dirPos + 1] = 0;
                store.update(recid, dir, dirSer);
            }
            return;
        }
        throw new DBException.DataCorruption("level too low");
    }

    /**
     * inserts new dir with two values
     */
    static long treePutSub(int dirShift, Store store, int level, long index1, long value1, long index2, long value2) {
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");
        if(CC.PARANOID && (dirShift<0||dirShift>maxDirShift))
            throw new AssertionError();
        if(CC.PARANOID && index1>>>((level+1)*dirShift)!=index2>>>((level+1)*dirShift)){
            throw new DBException.DataCorruption("inconsistent index");
        }
        int pos1 = treePos(dirShift, level, index1);
        int pos2 = treePos(dirShift, level, index2);
        long[] dir = dirEmpty();
        if(pos1==pos2){
            //insert new dir
            long recid = treePutSub(dirShift, store, level-1, index1, value1, index2, value2);
            dir = dirPut(dir, pos1, recid, 0L);//allocate after recursive call to save memory
        }else{
            //insert two records into this dir
            dir = dirPut(dir, pos1, value1, index1+1);
            dir = dirPut(dir, pos2, value2, index2+1);
        }
        return store.put(dir, dirSer);
    }

    static boolean treeRemove(int dirShift,
                              long recid,
                              Store store,
                              int level,
                              long index,
                              Long expectedValue //null for always remove
    ){
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");
        if(CC.PARANOID && index<0)
            throw new AssertionError();
        if(CC.PARANOID && (dirShift<0||dirShift>maxDirShift))
            throw new AssertionError();

//      TODO assert at top level
//        if(CC.PARANOID && index>>>(level*dirShift)!=0)
//            throw new AssertionError();

        long[] dir = store.get(recid, dirSer);
        final int slot = treePos(dirShift, level, index);
        final int pos = dirOffsetFromSlot(dir, slot);
        if(pos<0){
            //slot not found
            return false;
        }
        long oldVal = dir[pos];
        long oldIndex= dir[pos+1]-1;

        if (oldIndex == -1) {
            if (oldVal == 0) {
                throw new IllegalStateException(); //this was already covered by negative pos
            } else {
                //dive deeper
                return treeRemove(dirShift, oldVal, store, level-1, index, expectedValue);
                //TODO this should collapse node, if it becomes occupied by single record
            }
        } else if (oldIndex == index) {
            //slot is occupied by the same index
            if (expectedValue!=null && expectedValue.longValue()!=oldVal)
                return false;
            dir = dirRemove(dir, slot);
            store.update(recid, dir, dirSer);
            return true;
        } else {
            // is occupied by the different value, must split it
            return false;
        }
    }



    static final long[] treeRemoveCollapsingTrue = new long[0];

    static long[] treeRemoveCollapsing(
                                int dirShift,
                                long recid,
                                Store store,
                                int level,
                                boolean topLevel,
                                long index,
                                Long expectedValue //null for always remove
    ){
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");
        if(CC.PARANOID && index<0)
            throw new AssertionError();
        if(CC.PARANOID && (dirShift<0||dirShift>maxDirShift))
            throw new AssertionError();

//      TODO assert at top level
//        if(CC.PARANOID && index>>>(level*dirShift)!=0)
//            throw new AssertionError();

        long[] dir = store.get(recid, dirSer);
        final int slot = treePos(dirShift, level, index);
        final int pos = dirOffsetFromSlot(dir, slot);
        if(pos<0){
            //slot not found
            return null;
        }
        long oldVal = dir[pos];
        long oldIndex= dir[pos+1]-1;

        if (oldIndex == -1) {
            if (oldVal == 0) {
                throw new IllegalStateException(); //this was already covered by negative pos
            } else {
                //dive deeper
                long[] result =  treeRemoveCollapsing(dirShift, oldVal, store, level-1, false, index, expectedValue);
                if(result==null ||result==treeRemoveCollapsingTrue)
                    return result;
                //child node collapsed, put its content into here
                if(dir.length==4 && !topLevel){
                    //this was the only occupant of this node, collapse this node and push result up
                    store.delete(recid, dirSer);
                    return result;
                }
                //update existing node, with result from parent node
                dir = dir.clone();
                dir[pos] = result[2];
                dir[pos+1] = result[3];
                store.update(recid,dir,dirSer);
                return treeRemoveCollapsingTrue;
            }
        } else if (oldIndex == index) {
            //slot is occupied by the same index
            if (expectedValue!=null && expectedValue.longValue()!=oldVal)
                return null;
            dir = dirRemove(dir, slot);
            if(dir.length==4 && dir[3]>0){
                //this node has now only single occupant, and its not reference to another dir
                store.delete(recid, dirSer);
                return dir;
            }
            store.update(recid, dir, dirSer);
            return treeRemoveCollapsingTrue;
        } else {
            // is occupied by the different value, must split it
            return null;
        }
    }
    public static long[] treeIter(int dirShift, long recid, Store store, int level, long indexStart){
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");
        if(CC.PARANOID && indexStart<0)
            throw new AssertionError();
        if(CC.PARANOID && (dirShift<0||dirShift>maxDirShift))
            throw new AssertionError();


        long[] dir = store.get(recid, dirSer);

        boolean first = true;
        final int slot = treePos(dirShift, level, indexStart);
        int pos = dirOffsetFromSlot(dir,slot);
        if(pos<0)
            pos = -pos;
        posLoop:
        for(;
            pos<dir.length;
            pos+=2)
        {
            long oldVal = dir[pos];
            long oldIndex = dir[pos + 1]-1;

            if (oldIndex == -1) {
                if(oldVal == 0){
                    first = false;
                    //nothing here continue
                    continue posLoop;
                }

                //calculate corresponding index from our pos
                long index = first? indexStart : (
                        //upper part of level, strip out part for pos
                        indexStart & (full << ((level+1)*dirShift)) |
                        //part with current pos
                        ((long)slot)<<(dirShift*level)
                );
                // recid here, dive deeper
                long[] ret = treeIter(dirShift, oldVal, store, level-1, index);
                if (ret != null)
                    return ret;
                //nothing in this dir, continue

                //TODO PERF: add another type of iteration
                // this place  should not be reached if we collapse nodes on delete, or delete is forbidden
                // in that case we do not have to use recursion, and `dir` variable can be reused
            } else {
                if(oldIndex>=indexStart) {
                    //there is value here, return it
                    return new long[]{oldIndex, oldVal};
                }
                //this position is occupied by smaller index
            }
            first = false;
        }
        //reached end of this dir, nothing found
        return null;
    }

    interface TreeTraverseCallback<V>{
        V visit(long key, long value,V foldValue);
    }

    public static <V> V treeFold(long recid, Store store, int level, V initValue, TreeTraverseCallback<V> callback){
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");


        long[] dir = store.get(recid, dirSer);
        for(int pos=2;pos<dir.length; pos+=2){
            long oldVal = dir[pos];
            long oldIndex = dir[pos + 1]-1;

            if(oldVal==0 && oldIndex==-1)
                continue;

            if(oldIndex==-1){
                //directory
                initValue = treeFold(oldVal, store, level-1, initValue, callback);
            }else{
                initValue = callback.visit(oldIndex, oldVal, initValue);
            }
        }

        return initValue;
    }

    public static void treeClear(long recid, Store store, int level){
        treeClear(recid, store, level, true);
    }

    private static void treeClear(long recid, Store store, int level, boolean topLevel){
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");

        long[] dir = store.get(recid, dirSer);
        if(topLevel) {
            store.update(recid, dirEmpty(), dirSer);
        }else{
            store.delete(recid, dirSer);
        }
        for(int pos=2;pos<dir.length; pos+=2){
            long oldVal = dir[pos];
            long oldIndex = dir[pos + 1]-1;

            if(oldIndex==-1 && oldVal!=0){
                //directory
                treeClear(oldVal, store, level-1, false);
            }
        }
    }

    public static long[] treeLast(long recid, Store store, int level){
        if(CC.PARANOID && level<0)
            throw new DBException.DataCorruption("level too low");

        long[] dir = store.get(recid, dirSer);

        posLoop:
        for(int pos=dir.length-2;
            pos>=2;
            pos-=2)
        {
            long oldVal = dir[pos];
            long oldIndex = dir[pos + 1]-1;

            if(oldVal==0 && oldIndex==-1)
                continue; //nothing here
            if(oldIndex==-1){
                //directory
                long[] ret = treeLast(oldVal, store, level-1);
                if(ret!=null)
                    return ret;
            }else{
                return new long[]{oldIndex, oldVal};
            }

        }
        //reached end of this dir, nothing found
        return null;
    }

}
