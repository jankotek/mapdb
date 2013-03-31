package org.mapdb;/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes, 
 * Pat Fisher, Mike Judd. 
 */

import junit.framework.TestCase;

public class AtomicIntegerTest extends TestCase {

    DB db = DBMaker.newMemoryDB().writeAheadLogDisable().make();
    Atomic.Integer ai = Atomic.createInteger(db,"test", 1);

    /**
     * constructor initializes to given value
     */
    public void testConstructor(){
        assertEquals(1,ai.get());
    }

    /**
     * default constructed initializes to zero
     */
    public void testConstructor2(){
        Atomic.Integer  ai = Atomic.getInteger(db, "test2");
        assertEquals(0,ai.get());
    }

    /**
     * get returns the last value set
     */
    public void testGetSet(){
        assertEquals(1,ai.get());
        ai.set(2);
        assertEquals(2,ai.get());
        ai.set(-3);
        assertEquals(-3,ai.get());

    }

    /**
     * compareAndSet succeeds in changing value if equal to expected else fails
     */
    public void testCompareAndSet(){
        assertTrue(ai.compareAndSet(1,2));
        assertTrue(ai.compareAndSet(2,-4));
        assertEquals(-4,ai.get());
        assertFalse(ai.compareAndSet(-5,7));
        assertFalse((7 == ai.get()));
        assertTrue(ai.compareAndSet(-4,7));
        assertEquals(7,ai.get());
    }

    /**
     * compareAndSet in one thread enables another waiting for value
     * to succeed
     */
    public void testCompareAndSetInMultipleThreads() throws InterruptedException {
        Thread t = new Thread(new Runnable() {
            public void run() {
                while(!ai.compareAndSet(2, 3)) Thread.yield();
            }});

        t.start();
        assertTrue(ai.compareAndSet(1, 2));
        t.join(10);
        assertFalse(t.isAlive());
        assertEquals(ai.get(), 3);

    }


    /**
     * getAndSet returns previous value and sets to given value
     */
    public void testGetAndSet(){
        assertEquals(1,ai.getAndSet(0));
        assertEquals(0,ai.getAndSet(-10));
        assertEquals(-10,ai.getAndSet(1));
    }

    /**
     * getAndAdd returns previous value and adds given value
     */
    public void testGetAndAdd(){
        assertEquals(1,ai.getAndAdd(2));
        assertEquals(3,ai.get());
        assertEquals(3,ai.getAndAdd(-4));
        assertEquals(-1,ai.get());
    }

    /**
     * getAndDecrement returns previous value and decrements
     */
    public void testGetAndDecrement(){
        assertEquals(1,ai.getAndDecrement());
        assertEquals(0,ai.getAndDecrement());
        assertEquals(-1,ai.getAndDecrement());
    }

    /**
     * getAndIncrement returns previous value and increments
     */
    public void testGetAndIncrement(){

        assertEquals(1,ai.getAndIncrement());
        assertEquals(2,ai.get());
        ai.set(-2);
        assertEquals(-2,ai.getAndIncrement());
        assertEquals(-1,ai.getAndIncrement());
        assertEquals(0,ai.getAndIncrement());
        assertEquals(1,ai.get());
    }

    /**
     * addAndGet adds given value to current, and returns current value
     */
    public void testAddAndGet(){

        assertEquals(3,ai.addAndGet(2));
        assertEquals(3,ai.get());
        assertEquals(-1,ai.addAndGet(-4));
        assertEquals(-1,ai.get());
    }

    /**
     * decrementAndGet decrements and returns current value
     */
    public void testDecrementAndGet(){

        assertEquals(0,ai.decrementAndGet());
        assertEquals(-1,ai.decrementAndGet());
        assertEquals(-2,ai.decrementAndGet());
        assertEquals(-2,ai.get());
    }

    /**
     * incrementAndGet increments and returns current value
     */
    public void testIncrementAndGet(){

        assertEquals(2,ai.incrementAndGet());
        assertEquals(2,ai.get());
        ai.set(-2);
        assertEquals(-1,ai.incrementAndGet());
        assertEquals(0,ai.incrementAndGet());
        assertEquals(1,ai.incrementAndGet());
        assertEquals(1,ai.get());
    }


    /**
     * toString returns current value.
     */
    public void testToString() {
        for (int i = -12; i < 6; ++i) {
            ai.set(i);
            assertEquals(ai.toString(), Long.toString(i));
        }
    }

    /**
     * longValue returns current value.
     */
    public void testLongValue() {
        for (int i = -12; i < 6; ++i) {
            ai.set(i);
            assertEquals((long)i, ai.longValue());
        }
    }

    /**
     * floatValue returns current value.
     */
    public void testFloatValue() {
        for (int i = -12; i < 6; ++i) {
            ai.set(i);
            assertEquals((float)i, ai.floatValue());
        }
    }

    /**
     * doubleValue returns current value.
     */
    public void testDoubleValue() {
        for (int i = -12; i < 6; ++i) {
            ai.set(i);
            assertEquals((double)i, ai.doubleValue());
        }
    }


}
