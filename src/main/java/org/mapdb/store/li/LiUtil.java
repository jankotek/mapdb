package org.mapdb.store.li;

import java.nio.ByteBuffer;

public final class LiUtil {

    public static final long R_VOID = 0L;

    public static final long R_PREALLOC = 1;

    public static final long R_SMALL = 2;
    public static final long R_LINKED = 3;


    public static void zeroOut(ByteBuffer data, long offset, int size) {
        int end = (int) (offset+size);
        for(int i = (int) offset; i<end; i++){
            data.put(i, (byte) 0);
        }
    }


    public static long decompIndexValPage(long indexVal) {
        return indexVal & 0xFFFFFFFFFFL;
    }

    public static int decompIndexValSize(long indexVal) {
        return (int) ((indexVal >>> (5*8)) & 0xFFFF);
    }

    public static int decompIndexValType(long indexVal) {
        return (int) (indexVal >>> (7*8));
    }


    public static long composeIndexValSmall(int size, long page) {
        return  (R_SMALL<< (7*8)) |
                (((long)size)<<(5*8)) |
                page;
    }

    public static final long composeRecordType(long recType){
        return recType<<(7*8);
    }

}
