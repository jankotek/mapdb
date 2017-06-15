/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Adopted from Apache Harmony with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package org.mapdb

/**
 *
 *
 * A small toolkit of classes that support lock-free thread-safe
 * programming on single records.  In essence, the classes here
 * provide provide an atomic conditional update operation of the form:
 *

 * <pre>
 * boolean compareAndSet(expectedValue, updateValue);
</pre> *

 *
 *
 * This method (which varies in argument types across different
 * classes) atomically sets a record to the `updateValue` if it
 * currently holds the `expectedValue`, reporting `true` on
 * success. Classes jere also contain methods to get and
 * unconditionally set values.
 *
 *

 * The specifications of these methods enable to
 * employ more efficient internal DB locking. CompareAndSwap
 * operation is typically faster than using transactions, global lock or other
 * concurrent protection.

 *
 *
 * Instances of classes
 * [Atomic.Boolean],
 * [Atomic.Integer],
 * [Atomic.Long],
 * [Atomic.String] and
 * [Atomic.Var]
 * each provide access and updates to a single record of the
 * corresponding type.  Each class also provides appropriate utility
 * methods for that type.  For example, classes `Atomic.Long` and
 * `Atomic.Integer` provide atomic increment methods.  One
 * application is to generate unique keys for Maps:
 *
 * <pre>
 * Atomic.Long id = Atomic.getLong("mapId");
 * map.put(id.getAndIncrement(), "something");
</pre> *

 *
 *
 * Atomic classes are designed primarily as building blocks for
 * implementing non-blocking data structures and related infrastructure
 * classes.  The `compareAndSet` method is not a general
 * replacement for locking.  It applies only when critical updates for an
 * object are confined to a *single* record.
 *
 *

 * Atomic classes are not general purpose replacements for
 * `java.lang.Integer` and related classes.  They do *not*
 * define methods such as `hashCode` and
 * `compareTo`.  (Because atomic records are expected to be
 * mutated, they are poor choices for hash table keys.)  Additionally,
 * classes are provided only for those types that are commonly useful in
 * intended applications. Other types has to be wrapped into general [Atomic.Var]
 *
 *

 * You can also hold floats using
 * [java.lang.Float.floatToIntBits] and
 * [java.lang.Float.intBitsToFloat] conversions, and doubles using
 * [java.lang.Double.doubleToLongBits] and
 * [java.lang.Double.longBitsToDouble] conversions.
 *

 */
object Atomic {


    /**
     * An `int` record that may be updated atomically.  An
     * `Atomic@Integer` is used in applications such as atomically
     * incremented counters, and cannot be used as a replacement for an
     * [java.lang.Integer]. However, this class does extend
     * `Number` to allow uniform access by tools and utilities that
     * deal with numerically-based classes.
     */
    class Integer(protected val store: Store,
                  /**
                   * @return recid under which value is saved
                   */
                  val recid: kotlin.Long) : java.lang.Number() {
        init{
            assert(recid>0)
        }
        /**
         * Gets the current value.

         * @return the current value
         */
        fun get(): Int {
            return store.get(recid, Serializer.INTEGER)!!
        }

        /**
         * Sets to the given value.

         * @param newValue the new value
         */
        fun set(newValue: Int) {
            store.update(recid, newValue, Serializer.INTEGER)
        }


        /**
         * Atomically sets to the given value and returns the old value.

         * @param newValue the new value
         * *
         * @return the previous value
         */
        fun getAndSet(newValue: Int): Int? {

            while (true) {
                val current = get()

                if (compareAndSet(current, newValue)) {

                    return current
                }
            }
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value `==` the expected value.

         * @param expect the expected value
         * *
         * @param update the new value
         * *
         * @return true if successful. False return indicates that
         * * the actual value was not equal to the expected value.
         */
        fun compareAndSet(expect: Int, update: Int): kotlin.Boolean {
            return store.compareAndSwap(recid, expect, update, Serializer.INTEGER)
        }


        /**
         * Atomically increments by one the current value.

         * @return the previous value
         */
        fun getAndIncrement(): Int {

            while (true) {
                val current = get()
                val next = current + 1
                if (compareAndSet(current, next)) {
                    return current
                }

            }
        }

        fun increment(){
            getAndIncrement()
        }

        fun decrement(){
            getAndDecrement()
        }


        /**
         * Atomically decrements by one the current value.

         * @return the previous value
         */
        fun getAndDecrement(): Int {
            while (true) {
                val current = get()
                val next = current - 1
                if (compareAndSet(current, next)) {
                    return current
                }
            }
        }

        /**
         * Atomically adds the given value to the current value.

         * @param delta the value to add
         * *
         * @return the previous value
         */
        fun getAndAdd(delta: Int): Int {

            while (true) {

                val current = get()
                val next = current + delta

                if (compareAndSet(current, next)) {

                    return current
                }
            }
        }

        /**
         * Atomically increments by one the current value.

         * @return the updated value
         */
        fun incrementAndGet(): Int {

            while (true) {

                val current = get()
                val next = current + 1

                if (compareAndSet(current, next)) {

                    return next
                }
            }
        }

        /**
         * Atomically decrements by one the current value.

         * @return the updated value
         */
        fun decrementAndGet(): Int {

            while (true) {

                val current = get()
                val next = current - 1

                if (compareAndSet(current, next)) {

                    return next
                }
            }
        }


        /**
         * Atomically adds the given value to the current value.

         * @param delta the value to add
         * *
         * @return the updated value
         */
        fun addAndGet(delta: Int): Int {

            while (true) {

                val current = get()
                val next = current + delta

                if (compareAndSet(current, next)) {

                    return next
                }
            }
        }

        /**
         * Returns the String representation of the current value.
         * @return the String representation of the current value.
         */
        override fun toString(): kotlin.String {
            return get().toString()
        }


        override fun intValue(): Int {
            return get()
        }

        override fun longValue(): kotlin.Long {
            return get().toLong()
        }

        override fun floatValue(): Float {
            return get().toFloat()
        }

        override fun doubleValue(): Double {
            return get().toDouble()
        }

        companion object {

            @JvmStatic private val serialVersionUID = 4615119399830853054L
        }

    }


    /**
     * A `long` record that may be updated atomically.   An
     * `Atomic#Long` is used in applications such as atomically
     * incremented sequence numbers, and cannot be used as a replacement
     * for a [java.lang.Long]. However, this class does extend
     * `Number` to allow uniform access by tools and utilities that
     * deal with numerically-based classes.
     */
    class Long(protected val store: Store,
               /**
                * @return recid under which value is saved
                */
               val recid: kotlin.Long,
               val notNegative: kotlin.Boolean) : java.lang.Number() {
        init{
            assert(recid>0)
        }

        //TODO ensure that PACKED provides real perf boost, ie is not negative
        private val serializer = if(!notNegative) Serializer.LONG else Serializer.LONG_PACKED

        /**
         * Gets the current value.

         * @return the current value
         */
        fun get(): kotlin.Long {
            return store.get(recid, serializer)!!
        }

        /**
         * Sets to the given value.

         * @param newValue the new value
         */
        fun set(newValue: kotlin.Long) {
            store.update(recid, newValue, serializer)
        }


        /**
         * Atomically sets to the given value and returns the old value.

         * @param newValue the new value
         * *
         * @return the previous value
         */
        fun getAndSet(newValue: kotlin.Long): kotlin.Long {
            while (true) {
                val current = get()
                if (compareAndSet(current, newValue)) {

                    return current
                }
            }
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value `==` the expected value.

         * @param expect the expected value
         * *
         * @param update the new value
         * *
         * @return true if successful. False return indicates that
         * * the actual value was not equal to the expected value.
         */
        fun compareAndSet(expect: kotlin.Long, update: kotlin.Long): kotlin.Boolean {
            return store.compareAndSwap(recid, expect, update, serializer)
        }


        /**
         * Atomically increments by one the current value.

         * @return the previous value
         */
        fun getAndIncrement(): kotlin.Long{
            while (true) {
                val current = get()
                val next = current + 1
                if (compareAndSet(current, next)) {
                    return current
                }
            }
        }

        /**
         * Atomically decrements by one the current value.

         * @return the previous value
         */
        fun getAndDecrement(): kotlin.Long{
            while (true) {
                val current = get()
                val next = current - 1
                if (compareAndSet(current, next)) {
                    return current
                }
            }
        }


        fun increment(){
            getAndIncrement()
        }

        fun decrement(){
            getAndDecrement()
        }

        /**
         * Atomically adds the given value to the current value.

         * @param delta the value to add
         * *
         * @return the previous value
         */
        fun getAndAdd(delta: kotlin.Long): kotlin.Long {

            while (true) {

                val current = get()
                val next = current + delta

                if (compareAndSet(current, next)) {

                    return current
                }
            }
        }

        /**
         * Atomically increments by one the current value.

         * @return the updated value
         */
        fun incrementAndGet(): kotlin.Long {
            while (true) {
                val current = get()
                val next = current + 1
                if (compareAndSet(current, next)) {
                    return next
                }
            }
        }

        /**
         * Atomically decrements by one the current value.

         * @return the updated value
         */
        fun decrementAndGet(): kotlin.Long {
            while (true) {
                val current = get()
                val next = current - 1
                if (compareAndSet(current, next)) {
                    return next
                }
            }
        }

        /**
         * Atomically adds the given value to the current value.

         * @param delta the value to add
         * *
         * @return the updated value
         */
        fun addAndGet(delta: kotlin.Long): kotlin.Long {
            while (true) {
                val current = get()
                val next = current + delta
                if (compareAndSet(current, next)) {
                    return next
                }
            }
        }

        /**
         * Returns the String representation of the current value.
         * @return the String representation of the current value.
         */
        override fun toString(): kotlin.String {
            return get().toString()
        }


        override fun intValue(): Int {
            return get().toInt()
        }

        override fun longValue(): kotlin.Long {
            return get()
        }

        override fun floatValue(): Float {
            return get().toFloat()
        }

        override fun doubleValue(): Double {
            return get().toDouble()
        }

        companion object {
            @JvmStatic private val serialVersionUID = 2882620413591274781L
        }

    }


    /**
     * A `boolean` record that may be updated atomically.
     */
    class Boolean(protected val store: Store,
                  /**
                   * @return recid under which value is saved
                   */
                  val recid: kotlin.Long) {

        init{
            assert(recid>0)
        }

        /**
         * Returns the current value.

         * @return the current value
         */
        fun get(): kotlin.Boolean {
            return store.get(recid, Serializer.BOOLEAN)!!
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value `==` the expected value.

         * @param expect the expected value
         * *
         * @param update the new value
         * *
         * @return true if successful. False return indicates that
         * * the actual value was not equal to the expected value.
         */
        fun compareAndSet(expect: kotlin.Boolean, update: kotlin.Boolean): kotlin.Boolean {
            return store.compareAndSwap(recid, expect, update, Serializer.BOOLEAN)
        }


        /**
         * Unconditionally sets to the given value.

         * @param newValue the new value
         */
        fun set(newValue: kotlin.Boolean) {
            store.update(recid, newValue, Serializer.BOOLEAN)
        }


        /**
         * Atomically sets to the given value and returns the previous value.

         * @param newValue the new value
         * *
         * @return the previous value
         */
        fun getAndSet(newValue: kotlin.Boolean): kotlin.Boolean {

            while (true) {

                val current = get()

                if (compareAndSet(current, newValue)) {

                    return current
                }
            }
        }

        /**
         * Returns the String representation of the current value.
         * @return the String representation of the current value.
         */
        override fun toString(): kotlin.String {
            return get().toString()
        }


        companion object {
            @JvmStatic private val serialVersionUID = 23904324090330932L
        }

    }

    /**
     * A `String` record that may be updated atomically.
     */
    class String(protected val store: Store,
                 /**
                  * @return recid under which value is saved
                  */
                 val recid: kotlin.Long) {

        init{
            assert(recid>0)
        }

        override fun toString(): kotlin.String {
            return get() ?:"null"
        }

        /**
         * Returns the current value.

         * @return the current value
         */
        fun get(): kotlin.String?{
            return store.get(recid, Serializer.STRING_NOSIZE)
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value equals the expected value.

         * @param expect the expected value
         * *
         * @param update the new value
         * *
         * @return true if successful. False return indicates that
         * * the actual value was not equal to the expected value.
         */
        fun compareAndSet(expect: kotlin.String?, update: kotlin.String?): kotlin.Boolean {
            return store.compareAndSwap(recid, expect, update, Serializer.STRING_NOSIZE)
        }


        /**
         * Unconditionally sets to the given value.

         * @param newValue the new value
         */
        fun set(newValue: kotlin.String?) {
            store.update(recid, newValue, Serializer.STRING_NOSIZE)
        }


        /**
         * Atomically sets to the given value and returns the previous value.

         * @param newValue the new value
         * *
         * @return the previous value
         */
        fun getAndSet(newValue: kotlin.String?): kotlin.String? {
            while (true) {
                val current = get()
                if (compareAndSet(current, newValue)) {
                    return current
                }
            }
        }


        companion object {
            @JvmStatic private val serialVersionUID = 430902902309023432L
        }

    }

    /**
     * Atomically updated variable which may contain any type of record.
     */
    class Var<E>(protected val store: Store,

                 /**
                  * @return recid under which value is saved
                  */
                 val recid: kotlin.Long, val serializer: Serializer<E>) {

        init{
            assert(recid>0)
        }

        override fun toString(): kotlin.String {
            val v = get()
            return v?.toString() ?: "null"
        }

        /**
         * Returns the current value.

         * @return the current value
         */
        fun get(): E? {
            return store.get(recid, serializer)
        }

        /**
         * Atomically sets the value to the given updated value
         * if the current value equals the expected value.

         * @param expect the expected value
         * *
         * @param update the new value
         * *
         * @return true if successful. False return indicates that
         * * the actual value was not equal to the expected value.
         */
        fun compareAndSet(expect: E?, update: E?): kotlin.Boolean {
            return store.compareAndSwap(recid, expect, update, serializer)
        }


        /**
         * Unconditionally sets to the given value.

         * @param newValue the new value
         */
        fun set(newValue: E?) {
            store.update(recid, newValue, serializer)
        }


        /**
         * Atomically sets to the given value and returns the previous value.

         * @param newValue the new value
         * *
         * @return the previous value
         */
        fun getAndSet(newValue: E?): E? {
            while (true) {
                val current = get()
                if (compareAndSet(current, newValue)) {
                    return current
                }
            }
        }

        companion object {
            @JvmStatic private val serialVersionUID = 9009290490239002390L
        }

    }

}
