package org.mapdb.atomic

import org.mapdb.db.DB
import org.mapdb.ser.Serializer


class AtomicBooleanMaker(private val db:DB, private val name:String){

    private var initVal:Boolean = false

    fun create(): Atomic.Boolean {
        val recid = db.store.put()
    }

    companion object {
        @JvmStatic
        fun maker(db:DB, name: String): AtomicBooleanMaker {
              return AtomicBooleanMaker(db, name);
        }

    }

    fun init(initialValue:Boolean):AtomicBooleanMaker{
        initVal = initialValue
        return this
    }

}

class AtomicLongMaker(private val db:DB, private val name:String){

    private var initVal:Long = 0L

    fun create(): Atomic.Long {
        TODO("not implemented")
    }

    companion object {
        @JvmStatic
        fun maker(db:DB, name: String): AtomicLongMaker {
            TODO("not implemented")

            return AtomicLongMaker();
        }
    }

    fun init(initialValue:Long):AtomicLongMaker{
        initVal = initialValue
        return this
    }
}

class AtomicIntegerMaker(private val db:DB, private val name:String){

    private var initVal:Integer = 0

    fun create(): Atomic.Integer {
        TODO("not implemented")
    }

    fun init(initialValue:Integer):AtomicIntegerMaker{
        initVal = initialValue
        return this
    }

    companion object {
        @JvmStatic
        fun maker(db:DB, name: String): AtomicIntegerMaker {
            TODO("not implemented")

            return AtomicIntegerMaker();
        }
    }

}

class AtomicStringMaker(private val db:DB, private val name:String){

    private var initVal = ""

    fun create(): Atomic.String {
        TODO("not implemented")
    }

    companion object {
        @JvmStatic
        fun maker(db: DB, name: String): AtomicStringMaker {
            TODO("not implemented")

            return AtomicStringMaker();
        }
    }


    fun init(initialValue:String):AtomicStringMaker{
        initVal = initialValue
        return this
    }


}


class AtomicVarMaker<E>(private val db:DB, private val name:String){

    private var initVal:E? = null

    fun create(): Atomic.Var<E> {
        TODO("not implemented")
    }

    companion object {
        @JvmStatic
        fun <E> maker(db: DB, name: String, ser: Serializer<E>): AtomicVarMaker<E> {
            TODO("not implemented")

            return AtomicVarMaker();
        }
    }


    fun init(initialValue:E):AtomicVarMaker<E>{
        initVal = initialValue
        return this
    }


}
