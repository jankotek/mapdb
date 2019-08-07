package org.mapdb.jsr166Tests;/*
 * Written by Doug Lea and Martin Buchholz with assistance from
 * members of JCP JSR-166 Expert Group and released to the public
 * domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

import java.util.concurrent.BlockingDeque;
import java.util.Spliterator;

public abstract class LinkedBlockingDeque8Test extends JSR166TestCase {


    protected abstract BlockingDeque<Integer> newDeque();


    /**
     * Spliterator.getComparator always throws IllegalStateException
     */
    public void testSpliterator_getComparator() {
        assertThrows(IllegalStateException.class,
                     () -> newDeque().spliterator().getComparator());
    }

    /**
     * Spliterator characteristics are as advertised
     */
    public void testSpliterator_characteristics() {
        BlockingDeque q = newDeque();
        Spliterator s = q.spliterator();
        int characteristics = s.characteristics();
        int required = Spliterator.CONCURRENT
            | Spliterator.NONNULL
            | Spliterator.ORDERED;
        assertEquals(required, characteristics & required);
        assertTrue(s.hasCharacteristics(required));
        assertEquals(0, characteristics
                     & (Spliterator.DISTINCT
                        | Spliterator.IMMUTABLE
                        | Spliterator.SORTED));
    }

}
