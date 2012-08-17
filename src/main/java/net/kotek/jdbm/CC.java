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
     * Compile without trace logging statements (Logger.debug and Logger.trace)
     */
    boolean TRACE = true;

    /**
     * JDBM has some long running acceptance tests. For daily development it makes sense to skip those.
     * This flag controls whatever all tests are run.
     */
    boolean FULL_TEST = false;



    short STORE_FORMAT_VERSION = 10000 + 1;
}
