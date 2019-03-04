package org.mapdb.db

import org.mapdb.DBException
import org.mapdb.io.DataInput2
import org.mapdb.queue.LinkedFIFOQueue
import org.mapdb.ser.Serializer
import org.mapdb.store.MutableStore
import java.util.*

class QueueMaker<T>(private val db: DB, private val name: String, private val serializer: Serializer<T>){

    private var importInput: DataInput2? = null

    enum class Order {
        FIFO, LIFO
    }


    fun make(): Queue<T> {
        val params = db.paramsLoad()
        var qp = params[name]

        if (qp == null) {
            //create new
            val qp2 = LinkedFIFOQueue.createWithParams(db.store as MutableStore, serializer, importInput = importInput)
            val params2 = TreeMap(params)
            qp2[DB.ParamNames.serializer] = db.serializerName(serializer)!!
            params2[name] = qp2
            qp = qp2
            db.paramsSave(params2)
        }else if(importInput!=null){
            //TODO option to ignore?
            throw DBException.WrongConfig("Can not import, collection already exists")
        }


        if (qp[DB.ParamNames.serializer] != db.serializerName(serializer))
            throw DBException.WrongSerializer("Wrong serializer, expected ${qp[DB.ParamNames.serializer]} user supplied $serializer")

        return db.instances.get(name, {
            LinkedFIFOQueue.openWithParams(db.store, serializer as Serializer<T>, qp)
        }) as Queue<T>

    }

    fun importFromDataInput2(input: DataInput2): QueueMaker<T> {
        importInput = input
        return this
    }

    companion object {

        @JvmStatic
        fun <E> newLinkedFifoQueue(db:DB, name: String, clazz: Class<E>): QueueMaker<E> {
            return QueueMaker(db,name, db.serializerForClass(clazz));
        }
    }
}
