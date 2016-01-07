package org.mapdb.jsr166Tests;/*
 * Written by Doug Lea and Martin Buchholz with assistance from
 * members of JCP JSR-166 Expert Group and released to the public
 * domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

import junit.framework.Test;

/**
 * Contains tests applicable to all Collection implementations.
 */
public abstract class CollectionTest extends JSR166TestCase {
    final CollectionImplementation impl;

    /** Tests are parameterized by a Collection implementation. */
    CollectionTest(CollectionImplementation impl, String methodName) {
        super(methodName);
        this.impl = impl;
    }

    public static Test testSuite(CollectionImplementation impl) {
        return newTestSuite
            (parameterizedTestSuite(CollectionTest.class,
                                    CollectionImplementation.class,
                                    impl),
             jdk8ParameterizedTestSuite(CollectionTest.class,
                                        CollectionImplementation.class,
                                        impl));
    }

    /** A test of the CollectionImplementation implementation ! */
    public void testEmptyMeansEmpty() {
        assertTrue(impl.emptyCollection().isEmpty());
        assertEquals(0, impl.emptyCollection().size());
    }

    // public void testCollectionDebugFail() { fail(); }
}
