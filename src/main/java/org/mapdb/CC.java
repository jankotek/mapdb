package org.mapdb;

import org.mapdb.volume.ByteArrayVol;
import org.mapdb.volume.RandomAccessFileVol;
import org.mapdb.volume.VolumeFactory;

/**
 * Compilation Configuration. Uses dead code elimination to remove `if(CONSTANT){code}` blocks
 */
public interface CC{

    boolean LOG = true;

    /** compile MapDB with assertions enabled */
    boolean ASSERT = true;
    /** compile MapDB with paranoid mode enabled */
    boolean PARANOID = false;

    boolean ZEROS = false;

    boolean FAIR_LOCK = true;

    int PAGE_SHIFT = 20; // 1 MB
    long PAGE_SIZE = 1<<PAGE_SHIFT;

    /**
     * Will print stack trace of all operations which are write any data at given offset
     * Used for debugging.
     */
    long VOLUME_PRINT_STACK_AT_OFFSET = 0;

    VolumeFactory DEFAULT_MEMORY_VOLUME_FACTORY = ByteArrayVol.FACTORY;
    VolumeFactory DEFAULT_FILE_VOLUME_FACTORY = RandomAccessFileVol.FACTORY;

    int BTREEMAP_MAX_NODE_SIZE = 32;

    int HTREEMAP_CONC_SHIFT = 3;
    int HTREEMAP_DIR_SHIFT = 4;
    int HTREEMAP_LEVELS = 4;
}