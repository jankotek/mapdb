package org.mapdb

/**
 * Concurrency aware, can verify that its configuration is thread safe
 */
interface ConcurrencyAware{

    /** returns true if this is configured to be thread safe */
    val isThreadSafe:Boolean

    /** checks all subcomponents, if this component is really thread safe, and throws an exception if not thread safe */
    fun checkThreadSafe() {
        if(isThreadSafe.not())
            throw AssertionError();
    }
}