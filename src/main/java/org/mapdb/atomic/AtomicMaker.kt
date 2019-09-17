package org.mapdb.atomic

import org.mapdb.db.DB
import org.mapdb.db.DBProps
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers

abstract class AbstractMaker<E>(protected val db:DB, protected val name:String){

    fun create() = create2()

    // TODO methods not callable from java
    abstract fun create2():E
    abstract fun open2(props:Map<String,String>):E

    protected fun Map<String, String>.getLong(name:String):Long = this.get(name)!!.toLong()
    protected fun Map<String, String>.getRecid():Long = this.get(DBProps.RECID)!!.toLong()

}

class AtomicBooleanMaker(db:DB, name:String): AbstractMaker<Atomic.Boolean>(db, name) {

    private var initVal:Boolean = false

    override fun create2(): Atomic.Boolean {
        val recid = db.store.put(initVal, Serializers.BOOLEAN)
        return Atomic.Boolean(db.store, recid)
    }

    override fun open2(props: Map<String, String>): Atomic.Boolean {
        val recid = props.getRecid()
        return Atomic.Boolean(db.store, recid)
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

class AtomicLongMaker(db:DB, name:String): AbstractMaker<Atomic.Long>(db, name) {

    private var initVal:Long = 0L

    override fun create2(): Atomic.Long {
        val recid = db.store.put(initVal, Serializers.LONG)
        return Atomic.Long(db.store, recid)
    }

    override fun open2(props: Map<String, String>): Atomic.Long {
        val recid = props.getRecid()
        return Atomic.Long(db.store, recid)
    }

    fun init(initialValue:Long):AtomicLongMaker{
        initVal = initialValue
        return this
    }

    companion object {
        @JvmStatic
        fun maker(db:DB, name: String): AtomicLongMaker {
            return AtomicLongMaker(db, name);
        }
    }
}

class AtomicIntegerMaker(db:DB, name:String): AbstractMaker<Atomic.Integer>(db, name) {

    private var initVal:Int = 0


    override fun create2(): Atomic.Integer {
        val recid = db.store.put(initVal, Serializers.INTEGER)
        return Atomic.Integer(db.store, recid)
    }

    override fun open2(props: Map<String, String>): Atomic.Integer {
        val recid = props.getRecid()
        return Atomic.Integer(db.store, recid)
    }

    fun init(initialValue:Int):AtomicIntegerMaker{
        initVal = initialValue
        return this
    }


    companion object {
        @JvmStatic
        fun maker(db:DB, name: String): AtomicIntegerMaker {
            return AtomicIntegerMaker(db, name);
        }
    }

}

class AtomicStringMaker(db:DB, name:String): AbstractMaker<Atomic.String>(db, name) {

    private var initVal = ""

    override fun create2(): Atomic.String {
        val recid = db.store.put(initVal, Serializers.STRING) //TODO nosize ser
        return Atomic.String(db.store, recid)
    }

    override fun open2(props: Map<String, String>): Atomic.String {
        val recid = props.getRecid()
        return Atomic.String(db.store, recid)
    }


    fun init(initialValue:String):AtomicStringMaker{
        initVal = initialValue
        return this
    }

    companion object {
        @JvmStatic
        fun maker(db: DB, name: String): AtomicStringMaker {
            return AtomicStringMaker(db, name);
        }
    }



}


class AtomicVarMaker<E>(db:DB, name:String, private val serializer:Serializer<E>): AbstractMaker<Atomic.Var<E>>(db, name) {

    private var initVal:E? = null

    override fun create2(): Atomic.Var<E> {
        val recid = db.store.put(initVal, serializer) //TODO nosize ser
        return Atomic.Var(db.store, recid, serializer)
    }

    override fun open2(props: Map<String, String>): Atomic.Var<E> {
        val recid = props.getRecid()
        return Atomic.Var(db.store, recid, serializer)
    }

    companion object {
        @JvmStatic
        fun <E> maker(db: DB, name: String, ser: Serializer<E>): AtomicVarMaker<E> {
            return AtomicVarMaker(db, name, ser);
        }
    }

    fun init(initialValue:E):AtomicVarMaker<E>{
        initVal = initialValue
        return this
    }


}
