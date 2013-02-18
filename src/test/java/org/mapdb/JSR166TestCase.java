package org.mapdb;

import junit.framework.TestCase;

abstract public class JSR166TestCase extends TestCase {

    /**
     * The number of elements to place in collections, arrays, etc.
     */
    public static final int SIZE = 20;



    public static final Integer zero  = new Integer(0);
    public static final Integer one   = new Integer(1);
    public static final Integer two   = new Integer(2);
    public static final Integer three = new Integer(3);
    public static final Integer four  = new Integer(4);
    public static final Integer five  = new Integer(5);
    public static final Integer six   = new Integer(6);
    public static final Integer seven = new Integer(7);
    public static final Integer eight = new Integer(8);
    public static final Integer nine  = new Integer(9);
    public static final Integer m1  = new Integer(-1);
    public static final Integer m2  = new Integer(-2);
    public static final Integer m3  = new Integer(-3);
    public static final Integer m4  = new Integer(-4);
    public static final Integer m5  = new Integer(-5);
    public static final Integer m6  = new Integer(-6);
    public static final Integer m10 = new Integer(-10);

    /**
     * Fails with message "should throw exception".
     */
    public void shouldThrow() {
        fail("Should throw exception");
    }


}
