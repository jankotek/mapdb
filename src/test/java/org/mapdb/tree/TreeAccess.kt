package org.mapdb.tree

import org.mapdb.*

/**
 * Provides access to private methods in trees
 */

val HTreeMap<*,*>.counters:Array<Atomic.Long>?
        get() = TT.reflectionGetField(this, "counters")
