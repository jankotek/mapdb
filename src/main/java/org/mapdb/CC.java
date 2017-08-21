package org.mapdb;

import net.jpountz.xxhash.XXHashFactory;
import org.mapdb.volume.ByteArrayVol;
import org.mapdb.volume.RandomAccessFileVol;
import org.mapdb.volume.VolumeFactory;
/**
 * Compilation Configuration. Uses dead code elimination to remove `if(CONSTANT){code}` blocks
 */
public interface CC{

    /** compile with Logger statements */
    boolean LOG = true;

    /** compile MapDB with assertions enabled*/
    boolean PARANOID = false;

    /**
     * If enabled store space is filled with zeroes on allocation, deletion and update.
     * This will causer lower performance, but
     */
    boolean ZEROS = false;

    /** Boolean parameter passed to `ReentrantLock` and `ReadWriteReentrantLock`.
     * If true mapdb will use more fair thread scheduling, at expense of total throughput.
     */
    boolean FAIR_LOCK = true;

    /**
     * `StoreDirect` allocates space in 1MB incremental chunks, this changes chunks size.
     *
     * Record (byte[]) can not be read from two overlapping chunks, so there is some padding to prevent chunks overlap. Changing this parameter affects storage format and MapDB might not be able to read stores with different chunk sizes.
     *
     * There is DBMaker parameter to change allocation increment (DBMaker#allocationIncrement())
     */
    int PAGE_SHIFT = 20; // 1 MB
    long PAGE_SIZE = 1<<PAGE_SHIFT;

    /**
     * Will print stack trace of all operations which are write any data at given offset
     * Used for debugging.
     */
    long VOLUME_PRINT_STACK_AT_OFFSET = 0;

    /** default volume used for in-memory stores */
    VolumeFactory DEFAULT_MEMORY_VOLUME_FACTORY = ByteArrayVol.FACTORY;
    /** defuault volume used for file stores */
    VolumeFactory DEFAULT_FILE_VOLUME_FACTORY = RandomAccessFileVol.FACTORY;

    /** default maximal node size for BTreeMap nodes. This can be changed in DB config, max value is 32K? */
    int BTREEMAP_MAX_NODE_SIZE = 32;

    int HTREEMAP_CONC_SHIFT = 3;
    int HTREEMAP_DIR_SHIFT = 7;
    int HTREEMAP_LEVELS = 4;

    int INDEX_TREE_LONGLONGMAP_DIR_SHIFT = 7;
    int INDEX_TREE_LONGLONGMAP_LEVELS = 4;

    boolean LOG_WAL_CONTENT = false;

    XXHashFactory HASH_FACTORY = XXHashFactory.fastestJavaInstance();

    /** first byte on every file */
    long FILE_HEADER = 0x4A;

    /** second byte in {@link org.mapdb.StoreDirect} file format */
    long FILE_TYPE_STOREDIRECT  = 3;

    /** second byte in {@link org.mapdb.StoreWAL} write ahead log */
    long FILE_TYPE_STOREWAL_WAL = 2;

    /** second byte in {@link org.mapdb.SortedTableMap} file format,  with only single table (is probably read only)*/
    long FILE_TYPE_SORTED_SINGLE = 10;

    /** second byte in {@link org.mapdb.SortedTableMap} file format, with multiple tables (is probably writeable)*/
    long FILE_TYPE_SORTED_MULTI = 11;

    /** second byte in {@link org.mapdb.SortedTableMap} Write Ahead Log*/
    long FILE_TYPE_SORTED_WAL = 12;

    /** second byte in {@link org.mapdb.StoreTrivial} file format */
    long FILE_TYPE_STORETRIVIAL  = 20;

    boolean LOG_VOLUME_GCED = false;

    int STORE_DIRECT_CONC_SHIFT = 3;

    int FEAT_CHECKSUM_SHIFT = 1;
    int FEAT_CHECKSUM_MASK = 3;
    int FEAT_ENCRYPT_SHIFT = 0;
    int FEAT_ENCRYPT_MASK = 1;

    long RECID_NAME_CATALOG = 1L;
    long RECID_CLASS_INFOS = 2L;
    long RECID_MAX_RESERVED = 8L;

}