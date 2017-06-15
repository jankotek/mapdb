package org.mapdb.util

import org.mapdb.*
import java.io.File
import java.nio.file.Path
import java.util.*
import java.util.logging.*




object Utils {
    val LOG = Logger.getLogger("org.mapdb");

    /**
     * Return Path in the same parent folder, but with different suffix.
     */
    fun pathChangeSuffix(path: Path, suffix: String): Path {
        //TODO this might not work with alternative filesystems
        return File(path.toFile().path + suffix).toPath();
    }


    inline fun logDebug(msg:()->String ){
        if(CC.LOG && LOG.isLoggable(Level.FINE))
            LOG.log(Level.FINE,msg.invoke())
    }

    inline fun logInfo(msg:()->String ){
        if(LOG.isLoggable(Level.INFO))
            LOG.log(Level.INFO,msg.invoke())
    }


    fun roundDownToIntMAXVAL(size: Long?): Int {
        if (size!! > Integer.MAX_VALUE)
            return Integer.MAX_VALUE
        return size.toInt();
    }

    @JvmStatic fun <E> clone(value: E, serializer: Serializer<E>, out: DataOutput2 = DataOutput2()): E {
        out.pos = 0
        serializer.serialize(out, value)
        val in2 = DataInput2.ByteArray(out.copyBytes())
        return serializer.deserialize(in2, out.pos)
    }


    fun identityCount(vals: Array<*>): Int {
        val a = IdentityHashMap<Any?, Any?>()
        vals.forEach { a.put(it, "") }
        return a.size
    }


    inline fun logExceptions(crossinline run:()->Unit):()->Unit = {
        try {
            run()
        }catch (e:Throwable){
            LOG.log(Level.SEVERE,"Exception in background task", e)
            throw e
        }
    }



    @JvmStatic fun  <V> iterableEquals(serializer: Serializer<V>, col: Iterable<V>, other: Any?): Boolean {
        if(col === other)
            return true
        if(other==null || other !is Iterable<*>)
            return false

        val iter1 = col.iterator()
        val iter2 = other.iterator()
        while(iter1.hasNext()){
            if(!iter2.hasNext())
                return false
            if(!serializer.equals(iter1.next(), iter2.next() as V))
                return false
        }
        return !iter2.hasNext()
    }

    @JvmStatic fun <V> iterableHashCode(ser: Serializer<V>, collection: Iterable<V>): Int {
        var h = 0
        for(e in collection){
            h += DataIO.intHash(ser.hashCode(e, 0))
        }
        return h
    }
}