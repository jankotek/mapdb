package org.mapdb;

/*
 * Adopted from Apache Harmony with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

/**
 * A small toolkit of classes that support lock-free thread-safe
 * programming on single records.  In essence, the classes here
 * provide provide an atomic conditional update operation of the form:
 * <p>
 *
 * <pre>
 *   boolean compareAndSet(expectedValue, updateValue);
 * </pre>
 *
 * <p>This method (which varies in argument types across different
 * classes) atomically sets a record to the {@code updateValue} if it
 * currently holds the {@code expectedValue}, reporting {@code true} on
 * success. Classes jere also contain methods to get and
 * unconditionally set values.
 *
 * <p>The specifications of these methods enable to
 * employ more efficient internal DB locking. CompareAndSwap
 * operation is typically faster than using transactions, global lock or other
 * concurrent protection.
 *
 * <p>Instances of classes
 * {@link Atomic.Boolean},
 * {@link Atomic.Integer},
 * {@link Atomic.Long},
 * {@link Atomic.String} and
 * {@link Atomic.Var}
 * each provide access and updates to a single record of the
 * corresponding type.  Each class also provides appropriate utility
 * methods for that type.  For example, classes {@code Atomic.Long} and
 * {@code Atomic.Integer} provide atomic increment methods.  One
 * application is to generate unique keys for Maps:
 *
 * <pre>
 *    Atomic.Long id = Atomic.getLong("mapId");
 *    map.put(id.getAndIncrement(), "something");
 * </pre>
 *
 * <p>Atomic classes are designed primarily as building blocks for
 * implementing non-blocking data structures and related infrastructure
 * classes.  The {@code compareAndSet} method is not a general
 * replacement for locking.  It applies only when critical updates for an
 * object are confined to a <em>single</em> record.
 *
 * <p>Atomic classes are not general purpose replacements for
 * {@code java.lang.Integer} and related classes.  They do <em>not</em>
 * define methods such as {@code hashCode} and
 * {@code compareTo}.  (Because atomic records are expected to be
 * mutated, they are poor choices for hash table keys.)  Additionally,
 * classes are provided only for those types that are commonly useful in
 * intended applications. Other types has to be wrapped into general {@link Atomic.Var}
 * <p/>
 * You can also hold floats using
 * {@link java.lang.Float#floatToIntBits} and
 * {@link java.lang.Float#intBitsToFloat} conversions, and doubles using
 * {@link java.lang.Double#doubleToLongBits} and
 * {@link java.lang.Double#longBitsToDouble} conversions.
 *
 */
final public class Atomic {

    private Atomic(){}

    /**
     * Creates new record with given name and initial value.
     *
     * @param db to create record in
     * @param name name of new record
     * @param initVal initial value for Atomic.Long
     * @throws IllegalArgumentException if name is already used
     * @return Atomic.Long record
     */
    public static Long createLong(DB db, java.lang.String name, long  initVal) {
        db.checkNameNotExists(name);
        long recid = db.getEngine().put(initVal, Serializer.LONG_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new Long(db.getEngine(), recid);
    }

    /**
     * Gets or creates new record with given name.
     * If name does not exist, new record is created with default value <code>0</code>
     *
     * @param db to get record from
     * @param name of record
     * @return record
     */
    public static Long getLong(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
            createLong(db, name, 0) :
            new Long(db.getEngine(),recid);
    }

    /**
     * Creates new record with given name and initial value.
     *
     * @param db to create record in
     * @param name name of new record
     * @param initVal initial value for Atomic.Integer
     * @throws IllegalArgumentException if name is already used
     * @return  Atomic.Interger record
     */
    public static Integer createInteger(DB db, java.lang.String name, int  initVal) {
        db.checkNameNotExists(name);
        long recid = db.getEngine().put(initVal, Serializer.INTEGER_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new Integer(db.getEngine(), recid);
    }

    /**
     * Gets or creates new record with given name.
     * If name does not exist, new record is created with default value <code>0</code>
     *
     * @param db to get record from
     * @param name of record
     * @return record
    */
    public static Integer getInteger(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
                createInteger(db, name, 0) :
                new Integer(db.getEngine(),recid);
    }

    /**
     * Creates new record with given name and initial value.
     *
     * @param db to create record in
     * @param name name of new record
     * @param initVal initial value for Atomic.Boolean
     * @throws IllegalArgumentException if name is already used
     * @return Atomic.Boolean record
     */
    public static Boolean createBoolean(DB db, java.lang.String name, boolean  initVal) {
        db.checkNameNotExists(name);
        long recid = db.getEngine().put(initVal, Serializer.BOOLEAN_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new Boolean(db.getEngine(), recid);
    }

    /**
     * Gets or creates new record with given name.
     * If name does not exist, new record is created with default value <code>false</code>
     *
     * @param db to get record from
     * @param name of record
     * @return record
     */
    public static Boolean getBoolean(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
                createBoolean(db, name, false) :
                new Boolean(db.getEngine(),recid);
    }

    /**
     * Creates new record with given name and initial value.
     *
     * @param db to create record in
     * @param name name of new record
     * @param initVal initial value for Atomic.String, can not be a null
     * @throws IllegalArgumentException if name is already used or initVal was null
     * @return Atomic.String record
     */
    public static String createString(DB db, java.lang.String name, java.lang.String  initVal) {
        if(initVal==null) throw new IllegalArgumentException("initVal may not be null");
        db.checkNameNotExists(name);
        long recid = db.getEngine().put(initVal, Serializer.STRING_SERIALIZER);
        db.getNameDir().put(name, recid);
        return new String(db.getEngine(), recid);
    }

    /**
     * Gets or creates new record with given name.
     * If name does not exist, new record is created with default value <code>""</code>
     *
     * @param db to get record from
     * @param name of record
     * @return record
     */
    public static String getString(DB db, java.lang.String name) {
        java.lang.Long recid = db.nameDir.get(name);
        return  recid == null ?
                createString(db, name, "") :
                new String(db.getEngine(),recid);
    }

    /**
     * Creates new record with given name and initial value.
     *
     * @param db to create record in
     * @param name name of new record
     * @param initVal initial value for Atomic.Var
     * @param serializer used to convert value from/to binary form
     * @throws IllegalArgumentException if name is already used
     * @return Atomic.Var record
     */
    @SuppressWarnings("unchecked")
	public static <E> Var<E> createVar(DB db, java.lang.String name, E  initVal, Serializer<E> serializer) {
        db.checkNameNotExists(name);
        if(serializer == null) serializer = (Serializer<E>) db.getDefaultSerializer();
        long recid = db.getEngine().put(initVal, serializer);
        db.getNameDir().put(name, recid);
        return new Var<E>(db.getEngine(), recid, serializer);
    }

    /**
     * Gets or creates new record with given name.
     * If name does not exist, new record is created with default value <code>null</code>
     *
     * @param db to get record from
     * @param name of record
     * @return record
     */
    @SuppressWarnings("unchecked")
	public static <E> Var<E> getVar(DB db, java.lang.String name, Serializer<E> serializer) {
        java.lang.Long recid = db.nameDir.get(name);
        if(serializer == null) serializer = (Serializer<E>) db.getDefaultSerializer();
        return  recid == null ?
                createVar(db, name, null, serializer) :
                new Var<E>(db.getEngine(),recid, serializer);
    }



    /**
     * An {@code int} record that may be updated atomically.  An
     * {@code Atomic@Integer} is used in applications such as atomically
     * incremented counters, and cannot be used as a replacement for an
     * {@link java.lang.Integer}. However, this class does extend
     * {@code Number} to allow uniform access by tools and utilities that
     * deal with numerically-based classes.
     */
    public final static class Integer extends Number {

		private static final long serialVersionUID = 4615119399830853054L;
		
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
            return engine.get(recid, Serializer.INTEGER_SERIALIZER);
        }

        /**
         * Sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(int newValue) {
            engine.update(recid, newValue, Serializer.INTEGER_SERIALIZER);
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
            return engine.compareAndSwap(recid, expect, update, Serializer.INTEGER_SERIALIZER);
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
     * A {@code long} record that may be updated atomically.   An
     * {@code Atomic#Long} is used in applications such as atomically
     * incremented sequence numbers, and cannot be used as a replacement
     * for a {@link java.lang.Long}. However, this class does extend
     * {@code Number} to allow uniform access by tools and utilities that
     * deal with numerically-based classes.
     */
    public final static class Long extends Number{

		private static final long serialVersionUID = 2882620413591274781L;
		
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
            return engine.get(recid, Serializer.LONG_SERIALIZER);
        }

        /**
         * Sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(long newValue) {
            engine.update(recid, newValue, Serializer.LONG_SERIALIZER);
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
            return engine.compareAndSwap(recid, expect, update, Serializer.LONG_SERIALIZER);
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
     * A {@code boolean} record that may be updated atomically.
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
            return engine.get(recid, Serializer.BOOLEAN_SERIALIZER);
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
            return engine.compareAndSwap(recid, expect, update, Serializer.BOOLEAN_SERIALIZER);
        }


        /**
         * Unconditionally sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(boolean newValue) {
            engine.update(recid, newValue, Serializer.BOOLEAN_SERIALIZER);
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

    /*
    * A {@code String} record that may be updated atomically.
    */
    public final static class String{

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
            return engine.get(recid, Serializer.STRING_SERIALIZER);
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
            return engine.compareAndSwap(recid, expect, update, Serializer.STRING_SERIALIZER);
        }


        /**
         * Unconditionally sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(java.lang.String newValue) {
            engine.update(recid, newValue, Serializer.STRING_SERIALIZER);
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

    }

    /**
     * Atomically updated variable which may contain any type of record.
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
            return engine.get(recid, serializer);
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
            return engine.compareAndSwap(recid, expect, update, serializer);
        }


        /**
         * Unconditionally sets to the given value.
         *
         * @param newValue the new value
         */
        public final void set(E newValue) {
            engine.update(recid, newValue, serializer);
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
