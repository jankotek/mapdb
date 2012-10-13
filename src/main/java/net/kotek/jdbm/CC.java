package net.kotek.jdbm;

/**
 * Compiler Configuration.
 * Static final booleans to enable/disable features you want.
 * Compiler and dead code elimination will take care of removing unwanted features from bytecode.
 */
interface CC {

    /**
     * Compile with assertions.
     */
    boolean ASSERT = true;

    /**
     * Compile with trace logging statements (Logger.debug and Logger.trace)
     */
    boolean TRACE = false;

    /**
     * JDBM has some long running acceptance tests. For daily development it makes sense to skip those.
     * This flag controls whatever all tests are run.
     */
    boolean FULL_TEST = false;


    /**
     * Log all binary writes into log.
     */
    boolean BB_LOG_WRITES = false;

    boolean BB_CHECK_AVAILABLE_SIZE = false;


    short STORE_FORMAT_VERSION = 10000 + 1;


    /**
     * Values in BTreeMap are stored as part of nodes.
     * However if serialized size is greater then this,
     * value is placed as separate record and loaded
     * on request.
     */
    int MAX_BTREE_INLINE_VALUE_SIZE = 32;
}
