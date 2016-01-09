package org.mapdb;

import org.jetbrains.annotations.NotNull;

/**
 * Compilation Configuration. Uses dead code elimination to remove `if(CONSTANT){code}` blocks
 */
interface CC{

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

    Volume.VolumeFactory DEFAULT_MEMORY_VOLUME_FACTORY = Volume.ByteArrayVol.FACTORY;
    Volume.VolumeFactory DEFAULT_FILE_VOLUME_FACTORY = Volume.RandomAccessFileVol.FACTORY;


    int HTREEMAP_CONC_SHIFT = 3;
    int HTREEMAP_DIR_SHIFT = 4;
    int HTREEMAP_LEVELS = 4;
}