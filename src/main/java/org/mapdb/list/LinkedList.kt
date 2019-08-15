package org.mapdb.list

import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2
import org.mapdb.ser.Serializer
import org.mapdb.store.MutableStore
import org.mapdb.util.lockRead
import org.mapdb.util.lockWrite
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class LinkedList<E>(
        val store: MutableStore,
        val serializer: Serializer<E>,
        val rootRecid:Long = store.preallocate(),
        val isThreadSafe:Boolean=true
        )
//  : AbstractSequentialList<E>(),

//TODO implement list
//TODO implement Dequeu
{

    data class Node<E>(val prevRecid:Long, val nextRecid:Long, val e:E)

    val nodeSerializer: Serializer<Node<E>> = object:Serializer<Node<E>> {

        override fun serializedType() = Node::class.java

        override fun serialize(out: DataOutput2, k: Node<E>) {
            out.writePackedRecid(k.prevRecid)
            out.writePackedRecid(k.nextRecid)
            serializer.serialize(out, k.e)
        }

        override fun deserialize(input: DataInput2): Node<E> {
            return Node(
                    prevRecid = input.readPackedRecid(),
                    nextRecid = input.readPackedRecid(),
                    e = serializer.deserialize(input))
        }
    }

    private val lock: ReadWriteLock? = if(isThreadSafe) ReentrantReadWriteLock() else null

    val size: Int
        get() = lock.lockRead{
            var counter = 0L
            forEach { counter++ }
            return Math.min(Int.MAX_VALUE.toLong(), counter).toInt()
        }

    fun forEach(f:(E)->Unit){
        lock.lockRead {
            var recid = rootRecid
            while(true){
                val node = store.get(recid, nodeSerializer) ?: return
                f(node.e)
                recid = node.nextRecid
            }
        }
    }

    fun toList():List<E> {
        val ret = ArrayList<E>()
        forEach{e:E->
            ret+=e
        }
        return ret;
    }

    fun put(e:E){
        lock.lockWrite{
            //walk list
            var recid = rootRecid
            while(true){
                val node = store.get(recid, nodeSerializer)
                if(node==null){
                    //reached end, current recid is in preallocated state
                    val next = store.preallocate()
                    val node2 = Node(prevRecid = 0L, nextRecid = next, e=e)
                    store.update(recid, nodeSerializer, node2)
                    return
                }
                recid = node.nextRecid
            }
        }
    }

}