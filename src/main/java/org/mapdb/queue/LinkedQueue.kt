package org.mapdb.queue

import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2
import org.mapdb.serializer.Serializer
import org.mapdb.store.MutableStore
import org.mapdb.util.*
import org.mapdb.util.RecidRecord
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.NoSuchElementException


/**
 * Unbounded uncounted FIFO Queue (stack)
 */
class LinkedQueue<E> (
        private val store: MutableStore,
        private val rootRecid:Long,
        private val serializer:Serializer<E>)
    : AbstractQueue<E>(),
        BlockingQueue<E>{


    private data class Node<E>(val prevRecid:Long, val e:E)

    private val nodeSer = object:Serializer<Node<E>>{
        override fun serialize(k: Node<E>, out: DataOutput2) {
            out.writePackedRecid(k.prevRecid)
            serializer.serialize(k.e, out)
        }

        override fun deserialize(input: DataInput2): Node<E> {
            return Node(input.readPackedRecid(), serializer.deserialize(input))
        }

        override fun serializedType() = Node::class.java

    }

    private val head = RecidRecord(store, rootRecid)
    private val lock = ReentrantReadWriteLock()
    private var modCount = 0L

    private val notEmpty = lock.writeLock().newCondition()


    override fun offer(e: E?): Boolean {
        put(e)
        return true;
    }

    override fun offer(e: E?, timeout: Long, unit: TimeUnit?): Boolean {
        put(e)
        return true
    }

    override fun iterator(): MutableIterator<E> {
        lock.lockRead {
            return Iter(this)
        }
    }

    class Iter<E>(val q: LinkedQueue<E>) : MutableIterator<E> {

        val modCount = q.modCount;
        var headRecid = q.head.get()

        //used for delete
        var headRecid2 = 0L
        var headRecid3 = 0L

        val iterLock = ReentrantLock() //protect variables, TODO use atomic CAS or Mutex instead of lock with memory barrier

        override fun hasNext(): Boolean {
            return headRecid!=0L
        }

        override fun next(): E {
            iterLock.lock {
                q.lock.lockRead {
                    if (modCount != q.modCount)
                        throw ConcurrentModificationException()
                    if(headRecid==0L)
                        throw NoSuchElementException()

                    val node = q.store.get(headRecid, q.nodeSer)!!
                    headRecid3 = headRecid2
                    headRecid2 = headRecid
                    headRecid = node.prevRecid

                    return node.e
                }
            }
        }

        override fun remove() {
            iterLock.lock{
                if(headRecid2 == 0L)
                    throw NoSuchElementException()
                q.lock.lockWrite {
                    if (modCount != q.modCount)
                        throw ConcurrentModificationException()


                    if(headRecid3==0L) {
                        //deleted element is head of queue
                        q.head.set(headRecid)

                    }else{
                        //delete in middle of queue
                        q.store.updateWeak(headRecid3, q.nodeSer){oldNode->
                            Node(headRecid, oldNode!!.e)
                        }
                    }

                    //invalidate deleted element
                    q.store.delete(headRecid2, q.nodeSer)
                    headRecid2 = 0L
                }
            }
        }

    }

    override fun peek(): E? {
        lock.lockRead{
            val headRecid = head.get()
            if(headRecid==0L)
                return null

            val node = store.get(headRecid, nodeSer)!!
            return node.e
        }
    }

    override fun poll(): E? {
        lock.lockWrite{
            val headRecid = head.get()
            if(headRecid==0L)
                return null

            modCount++
            val node = store.get(headRecid, nodeSer)!!
            head.set(node.prevRecid)
            store.delete(headRecid, nodeSer)
            return node.e
        }

    }

    override fun poll(timeout: Long, unit: TimeUnit): E? {
            lock.lockWrite {
                var headRecid = head.get()
                if(headRecid == 0L){
                    //empty, await until not empty
                    notEmpty.await(timeout, unit)
                    headRecid = head.get()
                }

                if (headRecid == 0L) {
                    return null;
                }

                modCount++
                val node = store.get(headRecid, nodeSer)!!
                head.set(node.prevRecid)
                return node.e
            }

    }

    fun sizeLong():Long{
        lock.lockRead {
            var count=0L
            var recid = head.get()
            while(recid!=0L){
                val node = store.get(recid, nodeSer)!!
                count++;
                recid = node.prevRecid
            }
            return count
        }
    }

    override val size: Int
        get() =  Math.min(Int.MAX_VALUE.toLong(), sizeLong()).toInt()


    override fun take(): E? {
        while(true) {
            lock.lockWrite {
                val headRecid = head.get()
                if (headRecid != 0L) {
                    modCount++
                    val node = store.get(headRecid, nodeSer)!!
                    head.set(node.prevRecid)
                    return node.e
                }
                notEmpty.await()
            }
        }
    }

    override fun put(e: E?) {
        if(e==null)
            throw NullPointerException()
        lock.lockWrite {
            modCount++
            val node = Node(head.get(), e)
            val newHeadRecid = store.put(node, nodeSer)
            head.set(newHeadRecid)
            notEmpty.signal()
        }
    }

    override fun drainTo(c: MutableCollection<in E>?): Int {
        if(c==null)
            throw NullPointerException()
        if(c==this)
            throw IllegalArgumentException()

        lock.lockWrite {
            var counter = 0L
            var recid = head.get()
            while(recid!=0L){
                val node = store.get(recid, nodeSer)!!
                c.add(node.e)
                head.set(recid)
                store.delete(recid, nodeSer)
                recid = node.prevRecid
                counter++
            }
            modCount++
            return Math.min(Integer.MAX_VALUE.toLong(), counter).toInt()
        }
    }

    override fun drainTo(c: MutableCollection<in E>?, maxElements: Int): Int {
        if(c==null)
            throw NullPointerException()
        if(c==this)
            throw IllegalArgumentException()

        lock.lockWrite {
            var counter = 0L
            var recid = head.get()
            while(recid!=0L && counter<maxElements){
                val node = store.get(recid, nodeSer)!!
                c.add(node.e)
                head.set(recid)
                store.delete(recid, nodeSer)
                recid = node.prevRecid
                counter++
            }
            modCount++
            return Math.min(Integer.MAX_VALUE.toLong(), counter).toInt()
        }
    }

    override fun remainingCapacity(): Int {
        return Integer.MAX_VALUE
    }

}