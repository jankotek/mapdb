package org.mapdb

/**
 * Concurrency aware, can verify that its configuration is thread safe
 */
interface ConcurrencyAware{

    /** returns true if this is configured to be thread safe */
    val isThreadSafe:Boolean

    /** checks that class and all of its subcomponents is really thread safe, and throws an exception if is not thread safe */
    fun assertThreadSafe() {
        if(isThreadSafe.not())
            throw IllegalStateException();
    }
}