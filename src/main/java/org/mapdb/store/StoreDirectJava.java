package org.mapdb.store;

import org.mapdb.CC;

/**
 * Low level utilities for StoreDirect
 */
final class StoreDirectJava {

    static final long MAX_RECORD_SIZE = 0xFFFF-15;
    static final long NULL_RECORD_SIZE = 0xFFFF;
    static final long DELETED_RECORD_SIZE = 0xFFFF-1;


    static final long MOFFSET   = 0x0000FFFFFFFFFFF0L;
    static final long MLINKED   = 0x8L;
    static final long MUNUSED   = 0x4L;
    static final long MARCHIVE  = 0x2L;

    static final int HEAD_CHECKSUM_SEED = 1142099053;

    static final long HEADER_CHECKSUM   = 2*8; //TODO benchmarks
    static final long DATA_TAIL_OFFSET  = 3*8;
    static final long INDEX_TAIL_OFFSET = 4*8;
    static final long FILE_TAIL_OFFSET  = 5*8;


//    static final long LONG_STACK_UNUSED1 = 64;
//    static final long LONG_STACK_UNUSED16 = LONG_STACK_UNUSED1+16*8;

    static final long RECID_LONG_STACK = 8*8;

    static final long NUMBER_OF_SPACE_SLOTS = 1+MAX_RECORD_SIZE/16;

    static final long UNUSED1_LONG_STACK    = 8L * NUMBER_OF_SPACE_SLOTS + RECID_LONG_STACK;
    static final long UNUSED2_LONG_STACK    = UNUSED1_LONG_STACK+8;
    static final long UNUSED3_LONG_STACK    = UNUSED2_LONG_STACK+8;
    static final long UNUSED4_LONG_STACK    = UNUSED3_LONG_STACK+8;
    static final long HEAD_END              = UNUSED4_LONG_STACK+8;
    static final long ZERO_PAGE_LINK        = HEAD_END;

    protected final static long LONG_STACK_PREF_SIZE = 160;
    protected final static long LONG_STACK_MIN_SIZE = 16;
    protected final static long LONG_STACK_MAX_SIZE = 256;


    static final long RECIDS_PER_INDEX_PAGE = (CC.PAGE_SIZE-16)/8;
    static final long RECIDS_PER_ZERO_INDEX_PAGE = (CC.PAGE_SIZE-HEAD_END-16)/8;

    static long indexValToSize(long ival){
        return ival>>>48;
    }



    static long indexValToOffset(long ival){
        return ival&MOFFSET;
    }


}
