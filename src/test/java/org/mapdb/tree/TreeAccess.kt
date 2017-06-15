package org.mapdb.tree

import org.fest.reflect.core.Reflection
import org.mapdb.Atomic

/**
 * Provides access to private methods in trees
 */

val HTreeMap<*,*>.counters:Array<Atomic.Long>?
        get() = Reflection.method("getCounters").`in`(this).invoke() as Array<Atomic.Long>?
