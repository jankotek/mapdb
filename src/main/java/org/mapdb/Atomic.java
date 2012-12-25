package org.mapdb;

/*
 * Adopted from Apache Harmony with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/**
 *
 * TODO change JavaDoc to match MapDB.
 *
 * A small toolkit of classes that support lock-free thread-safe
 * programming on single variables.  In essence, the classes in this
 * package extend the notion of {@code volatile} values, fields, and
 * array elements to those that also provide an atomic conditional update
 * operation of the form:
 *
 * <pre>
 *   boolean compareAndSet(expectedValue, updateValue);
 * </pre>
 *
 * <p>This method (which varies in argument types across different
 * classes) atomically sets a variable to the {@code updateValue} if it
 * currently holds the {@code expectedValue}, reporting {@code true} on
 * success.  The classes in this package also contain methods to get and
 * unconditionally set values, as well as a weaker conditional atomic
 * update operation {@code weakCompareAndSet} described below.
 *
 * <p>The specifications of these methods enable implementations to
 * employ efficient machine-level atomic instructions that are available
 * on contemporary processors.  However on some platforms, support may
 * entail some form of internal locking.  Thus the methods are not
 * strictly guaranteed to be non-blocking --
 * a thread may block transiently before performing the operation.
 *
 * <p>Instances of classes
 * {@link java.util.concurrent.atomic.AtomicBoolean},
 * {@link java.util.concurrent.atomic.AtomicInteger},
 * {@link java.util.concurrent.atomic.AtomicLong}, and
 * {@link java.util.concurrent.atomic.AtomicReference}
 * each provide access and updates to a single variable of the
 * corresponding type.  Each class also provides appropriate utility
 * methods for that type.  For example, classes {@code AtomicLong} and
 * {@code AtomicInteger} provide atomic increment methods.  One
 * application is to generate sequence numbers, as in:
 *
 * <pre>
 * class Sequencer {
 *   private final AtomicLong sequenceNumber
 *     = new AtomicLong(0);
 *   public long next() {
 *     return sequenceNumber.getAndIncrement();
 *   }
 * }
 * </pre>
 *
 * <p>The memory effects for accesses and updates of atomics generally
 * follow the rules for volatiles, as stated in
 * <a href="http://java.sun.com/docs/books/jls/"> The Java Language
 * Specification, Third Edition (17.4 Memory Model)</a>:
 *
 * <ul>
 *
 *   <li> {@code get} has the memory effects of reading a
 * {@code volatile} variable.
 *
 *   <li> {@code set} has the memory effects of writing (assigning) a
 * {@code volatile} variable.
 *
 *   <li> {@code lazySet} has the memory effects of writing (assigning)
 *   a {@code volatile} variable except that it permits reorderings with
 *   subsequent (but not previous) memory actions that do not themselves
 *   impose reordering constraints with ordinary non-{@code volatile}
 *   writes.  Among other usage contexts, {@code lazySet} may apply when
 *   nulling out, for the sake of garbage collection, a reference that is
 *   never accessed again.
 *
 *   <li>{@code weakCompareAndSet} atomically reads and conditionally
 *   writes a variable but does <em>not</em>
 *   create any happens-before orderings, so provides no guarantees
 *   with respect to previous or subsequent reads and writes of any
 *   variables other than the target of the {@code weakCompareAndSet}.
 *
 *   <li> {@code compareAndSet}
 *   and all other read-and-update operations such as {@code getAndIncrement}
 *   have the memory effects of both reading and
 *   writing {@code volatile} variables.
 * </ul>
 *
 * <p>In addition to classes representing single values, this package
 * contains <em>Updater</em> classes that can be used to obtain
 * {@code compareAndSet} operations on any selected {@code volatile}
 * field of any selected class.
 *
 * {@link java.util.concurrent.atomic.AtomicReferenceFieldUpdater},
 * {@link java.util.concurrent.atomic.AtomicIntegerFieldUpdater}, and
 * {@link java.util.concurrent.atomic.AtomicLongFieldUpdater} are
 * reflection-based utilities that provide access to the associated
 * field types.  These are mainly of use in atomic data structures in
 * which several {@code volatile} fields of the same node (for
 * example, the links of a tree node) are independently subject to
 * atomic updates.  These classes enable greater flexibility in how
 * and when to use atomic updates, at the expense of more awkward
 * reflection-based setup, less convenient usage, and weaker
 * guarantees.
 *
 * <p>The
 * {@link java.util.concurrent.atomic.AtomicIntegerArray},
 * {@link java.util.concurrent.atomic.AtomicLongArray}, and
 * {@link java.util.concurrent.atomic.AtomicReferenceArray} classes
 * further extend atomic operation support to arrays of these types.
 * These classes are also notable in providing {@code volatile} access
 * semantics for their array elements, which is not supported for
 * ordinary arrays.
 *
 * <a name="Spurious">
 * <p>The atomic classes also support method {@code weakCompareAndSet},
 * which has limited applicability.  On some platforms, the weak version
 * may be more efficient than {@code compareAndSet} in the normal case,
 * but differs in that any given invocation of the
 * {@code weakCompareAndSet} method may return {@code false}
 * <em>spuriously</em> (that is, for no apparent reason)</a>.  A
 * {@code false} return means only that the operation may be retried if
 * desired, relying on the guarantee that repeated invocation when the
 * variable holds {@code expectedValue} and no other thread is also
 * attempting to set the variable will eventually succeed.  (Such
 * spurious failures may for example be due to memory contention effects
 * that are unrelated to whether the expected and current values are
 * equal.)  Additionally {@code weakCompareAndSet} does not provide
 * ordering guarantees that are usually needed for synchronization
 * control.  However, the method may be useful for updating counters and
 * statistics when such updates are unrelated to the other
 * happens-before orderings of a program.  When a thread sees an update
 * to an atomic variable caused by a {@code weakCompareAndSet}, it does
 * not necessarily see updates to any <em>other</em> variables that
 * occurred before the {@code weakCompareAndSet}.  This may be
 * acceptable when, for example, updating performance statistics, but
 * rarely otherwise.
 *
 * <p>The {@link java.util.concurrent.atomic.AtomicMarkableReference}
 * class associates a single boolean with a reference.  For example, this
 * bit might be used inside a data structure to mean that the object
 * being referenced has logically been deleted.
 *
 * The {@link java.util.concurrent.atomic.AtomicStampedReference}
 * class associates an integer value with a reference.  This may be
 * used for example, to represent version numbers corresponding to
 * series of updates.
 *
 * <p>Atomic classes are designed primarily as building blocks for
 * implementing non-blocking data structures and related infrastructure
 * classes.  The {@code compareAndSet} method is not a general
 * replacement for locking.  It applies only when critical updates for an
 * object are confined to a <em>single</em> variable.
 *
 * <p>Atomic classes are not general purpose replacements for
 * {@code java.lang.Integer} and related classes.  They do <em>not</em>
 * define methods such as {@code hashCode} and
 * {@code compareTo}.  (Because atomic variables are expected to be
 * mutated, they are poor choices for hash table keys.)  Additionally,
 * classes are provided only for those types that are commonly useful in
 * intended applications.  For example, there is no atomic class for
 * representing {@code byte}.  In those infrequent cases where you would
 * like to do so, you can use an {@code AtomicInteger} to hold
 * {@code byte} values, and cast appropriately.
 *
 * You can also hold floats using
 * {@link java.lang.Float#floatToIntBits} and
 * {@link java.lang.Float#intBitsToFloat} conversions, and doubles using
 * {@link java.lang.Double#doubleToLongBits} and
 * {@link java.lang.Double#longBitsToDouble} conversions.
 *
 * @since 1.5
 */
final public class Atomic {

    public static Long createLong(DB db, java.lang.String name, long  initVal) {
        db.checkNameNotExists(name);
        long recid = db.getEngine().recordPut(initVal, Serializer.LONG_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new Long(db.getEngine(), recid);
    }

    public static Long getLong(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
            createLong(db, name, 0) :
            new Long(db.getEngine(),recid);
    }

    public static Integer createInteger(DB db, java.lang.String name, int  initVal) {
        db.checkNameNotExists(name);
        long recid = db.getEngine().recordPut(initVal, Serializer.INTEGER_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new Integer(db.getEngine(), recid);
    }

    public static Integer getInteger(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
                createInteger(db, name, 0) :
                new Integer(db.getEngine(),recid);
    }

    public static Boolean createBoolean(DB db, java.lang.String name, boolean  initVal) {
        db.checkNameNotExists(name);
        long recid = db.getEngine().recordPut(initVal, Serializer.BOOLEAN_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new Boolean(db.getEngine(), recid);
    }

    public static Boolean getBoolean(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
                createBoolean(db, name, false) :
                new Boolean(db.getEngine(),recid);
    }


    /**
     * An {@code int} value that may be updated atomically.  See the
     * {@link java.util.concurrent.atomic} package specification for
     * description of the properties of atomic variables. An
     * {@code AtomicInteger} is used in applications such as atomically
     * incremented counters, and cannot be used as a replacement for an
     * {@link java.lang.Integer}. However, this class does extend
     * {@code Number} to allow uniform access by tools and utilities that
     * deal with numerically-based classes.
     */
    public final static class Integer extends Number {

        protected final Engine engine;
        protected final long recid;

        public Integer(Engine engine, long recid) {
            this.engine = engine;
            this.recid = recid;
        }

        /**
         * Gets the current value.
         *
         * @return the current value
         */
        public final int get() {
            return engine.recordGet(recid, Serializer.INTEGER_SERIALIZER);
        }

        /**
         * Sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(int newValue) {
            engine.recordUpdate(recid, newValue, Serializer.INTEGER_SERIALIZER);
        }


        /**
         * Atomically sets to the given value and returns the old value.
         *
         * @param newValue the new value
         * @return the previous value
         */
        public final int getAndSet(int newValue) {
            for (;;) {
                int current = get();
                if (compareAndSet(current, newValue))
                    return current;
            }
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value {@code ==} the expected value.
         *
         * @param expect the expected value
         * @param update the new value
         * @return true if successful. False return indicates that
         * the actual value was not equal to the expected value.
         */
        public final boolean compareAndSet(int expect, int update) {
            return engine.recordCompareAndSwap(recid, expect,update, Serializer.INTEGER_SERIALIZER);
        }


        /**
         * Atomically increments by one the current value.
         *
         * @return the previous value
         */
        public final int getAndIncrement() {
            for (;;) {
                int current = get();
                int next = current + 1;
                if (compareAndSet(current, next))
                    return current;
            }
        }

        /**
         * Atomically decrements by one the current value.
         *
         * @return the previous value
         */
        public final int getAndDecrement() {
            for (;;) {
                int current = get();
                int next = current - 1;
                if (compareAndSet(current, next))
                    return current;
            }
        }

        /**
         * Atomically adds the given value to the current value.
         *
         * @param delta the value to add
         * @return the previous value
         */
        public final int getAndAdd(int delta) {
            for (;;) {
                int current = get();
                int next = current + delta;
                if (compareAndSet(current, next))
                    return current;
            }
        }

        /**
         * Atomically increments by one the current value.
         *
         * @return the updated value
         */
        public final int incrementAndGet() {
            for (;;) {
                int current = get();
                int next = current + 1;
                if (compareAndSet(current, next))
                    return next;
            }
        }

        /**
         * Atomically decrements by one the current value.
         *
         * @return the updated value
         */
        public final int decrementAndGet() {
            for (;;) {
                int current = get();
                int next = current - 1;
                if (compareAndSet(current, next))
                    return next;
            }
        }

        /**
         * Atomically adds the given value to the current value.
         *
         * @param delta the value to add
         * @return the updated value
         */
        public final int addAndGet(int delta) {
            for (;;) {
                int current = get();
                int next = current + delta;
                if (compareAndSet(current, next))
                    return next;
            }
        }

        /**
         * Returns the String representation of the current value.
         * @return the String representation of the current value.
         */
        public java.lang.String toString() {
            return java.lang.Integer.toString(get());
        }


        public int intValue() {
            return get();
        }

        public long longValue() {
            return (long)get();
        }

        public float floatValue() {
            return (float)get();
        }

        public double doubleValue() {
            return (double)get();
        }

    }


    /**
     * A {@code long} value that may be updated atomically.  See the
     * {@link java.util.concurrent.atomic} package specification for
     * description of the properties of atomic variables. An
     * {@code AtomicLong} is used in applications such as atomically
     * incremented sequence numbers, and cannot be used as a replacement
     * for a {@link java.lang.Long}. However, this class does extend
     * {@code Number} to allow uniform access by tools and utilities that
     * deal with numerically-based classes.
     */
    public final static class Long extends Number{


        protected final Engine engine;
        protected final long recid;

        public Long(Engine engine, long recid) {
            this.engine = engine;
            this.recid = recid;
        }


        /**
         * Gets the current value.
         *
         * @return the current value
         */
        public final long get() {
            return engine.recordGet(recid, Serializer.LONG_SERIALIZER);
        }

        /**
         * Sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(long newValue) {
            engine.recordUpdate(recid, newValue, Serializer.LONG_SERIALIZER);
        }


        /**
         * Atomically sets to the given value and returns the old value.
         *
         * @param newValue the new value
         * @return the previous value
         */
        public final long getAndSet(long newValue) {
            while (true) {
                long current = get();
                if (compareAndSet(current, newValue))
                    return current;
            }
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value {@code ==} the expected value.
         *
         * @param expect the expected value
         * @param update the new value
         * @return true if successful. False return indicates that
         * the actual value was not equal to the expected value.
         */
        public final boolean compareAndSet(long expect, long update) {
            return engine.recordCompareAndSwap(recid, expect, update, Serializer.LONG_SERIALIZER);
        }


        /**
         * Atomically increments by one the current value.
         *
         * @return the previous value
         */
        public final long getAndIncrement() {
            while (true) {
                long current = get();
                long next = current + 1;
                if (compareAndSet(current, next))
                    return current;
            }
        }

        /**
         * Atomically decrements by one the current value.
         *
         * @return the previous value
         */
        public final long getAndDecrement() {
            while (true) {
                long current = get();
                long next = current - 1;
                if (compareAndSet(current, next))
                    return current;
            }
        }

        /**
         * Atomically adds the given value to the current value.
         *
         * @param delta the value to add
         * @return the previous value
         */
        public final long getAndAdd(long delta) {
            while (true) {
                long current = get();
                long next = current + delta;
                if (compareAndSet(current, next))
                    return current;
            }
        }

        /**
         * Atomically increments by one the current value.
         *
         * @return the updated value
         */
        public final long incrementAndGet() {
            for (;;) {
                long current = get();
                long next = current + 1;
                if (compareAndSet(current, next))
                    return next;
            }
        }

        /**
         * Atomically decrements by one the current value.
         *
         * @return the updated value
         */
        public final long decrementAndGet() {
            for (;;) {
                long current = get();
                long next = current - 1;
                if (compareAndSet(current, next))
                    return next;
            }
        }

        /**
         * Atomically adds the given value to the current value.
         *
         * @param delta the value to add
         * @return the updated value
         */
        public final long addAndGet(long delta) {
            for (;;) {
                long current = get();
                long next = current + delta;
                if (compareAndSet(current, next))
                    return next;
            }
        }

        /**
         * Returns the String representation of the current value.
         * @return the String representation of the current value.
         */
        public java.lang.String toString() {
            return java.lang.Long.toString(get());
        }


        public int intValue() {
            return (int)get();
        }

        public long longValue() {
            return get();
        }

        public float floatValue() {
            return (float)get();
        }

        public double doubleValue() {
            return (double)get();
        }

    }


    /**
     * A {@code boolean} value that may be updated atomically. See the
     * {@link java.util.concurrent.atomic} package specification for
     * description of the properties of atomic variables. An
     * {@code AtomicBoolean} is used in applications such as atomically
     * updated flags, and cannot be used as a replacement for a
     * {@link java.lang.Boolean}.
     */
    public final static class Boolean {

        protected final Engine engine;
        protected final long recid;

        public Boolean(Engine engine, long recid) {
            this.engine = engine;
            this.recid = recid;
        }


        /**
         * Returns the current value.
         *
         * @return the current value
         */
        public final boolean get() {
            return engine.recordGet(recid, Serializer.BOOLEAN_SERIALIZER);
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value {@code ==} the expected value.
         *
         * @param expect the expected value
         * @param update the new value
         * @return true if successful. False return indicates that
         * the actual value was not equal to the expected value.
         */
        public final boolean compareAndSet(boolean expect, boolean update) {
            return engine.recordCompareAndSwap(recid, expect, update, Serializer.BOOLEAN_SERIALIZER);
        }


        /**
         * Unconditionally sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(boolean newValue) {
            engine.recordUpdate(recid, newValue, Serializer.BOOLEAN_SERIALIZER);
        }


        /**
         * Atomically sets to the given value and returns the previous value.
         *
         * @param newValue the new value
         * @return the previous value
         */
        public final boolean getAndSet(boolean newValue) {
            for (;;) {
                boolean current = get();
                if (compareAndSet(current, newValue))
                    return current;
            }
        }

        /**
         * Returns the String representation of the current value.
         * @return the String representation of the current value.
         */
        public java.lang.String toString() {
            return java.lang.Boolean.toString(get());
        }

    }


    public final static class String implements CharSequence{

        protected final Engine engine;
        protected final long recid;

        public String(Engine engine, long recid) {
            this.engine = engine;
            this.recid = recid;
        }


        public java.lang.String toString() {
            return get();
        }

        /**
         * Returns the current value.
         *
         * @return the current value
         */
        public final java.lang.String get() {
            return engine.recordGet(recid, Serializer.STRING_SERIALIZER);
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value equals the expected value.
         *
         * @param expect the expected value
         * @param update the new value
         * @return true if successful. False return indicates that
         * the actual value was not equal to the expected value.
         */
        public final boolean compareAndSet(java.lang.String expect, java.lang.String update) {
            return engine.recordCompareAndSwap(recid, expect, update, Serializer.STRING_SERIALIZER);
        }


        /**
         * Unconditionally sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(java.lang.String newValue) {
            engine.recordUpdate(recid, newValue, Serializer.STRING_SERIALIZER);
        }


        /**
         * Atomically sets to the given value and returns the previous value.
         *
         * @param newValue the new value
         * @return the previous value
         */
        public final java.lang.String getAndSet(java.lang.String newValue) {
            for (;;) {
                java.lang.String current = get();
                if (compareAndSet(current, newValue))
                    return current;
            }
        }




        @Override
        public int length() {
            return get().length();
        }

        @Override
        public char charAt(int index) {
            return get().charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return get().subSequence(start,end);
        }
    }

    /**
     * Atomically updated variable which may contain any type
     */
    public static final class Var<E> {

        protected final Engine engine;
        protected final long recid;
        protected final Serializer<E> serializer;

        public Var(Engine engine, long recid, Serializer<E> serializer) {
            this.engine = engine;
            this.recid = recid;
            this.serializer = serializer;
        }

        public java.lang.String toString() {
            E v = get();
            return v==null? null : v.toString();
        }

        /**
         * Returns the current value.
         *
         * @return the current value
         */
        public final E get() {
            return engine.recordGet(recid, serializer);
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value equals the expected value.
         *
         * @param expect the expected value
         * @param update the new value
         * @return true if successful. False return indicates that
         * the actual value was not equal to the expected value.
         */
        public final boolean compareAndSet(E expect, E update) {
            return engine.recordCompareAndSwap(recid, expect, update, serializer);
        }


        /**
         * Unconditionally sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(E newValue) {
            engine.recordUpdate(recid, newValue, serializer);
        }


        /**
         * Atomically sets to the given value and returns the previous value.
         *
         * @param newValue the new value
         * @return the previous value
         */
        public final E getAndSet(E newValue) {
            for (;;) {
                E current = get();
                if (compareAndSet(current, newValue))
                    return current;
            }
        }


    }

}
