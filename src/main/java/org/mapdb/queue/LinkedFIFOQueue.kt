package org.mapdb.queue

import org.mapdb.db.DB
import org.mapdb.DBException
import org.mapdb.Exporter
import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.store.MutableStore
import org.mapdb.store.Store
import org.mapdb.store.StoreOnHeap
import org.mapdb.util.*
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Consumer
import kotlin.NoSuchElementException


/**
 * Unbounded uncounted FIFO Queue (stack)
 */
class LinkedFIFOQueue<E> (
        private val store: MutableStore,
        private val rootRecid:Long,
        private val serializer:Serializer<E>)
    : AbstractQueue<E>(),
        BlockingQueue<E>, Exporter {

    companion object {

        val formatFIFO = "LinkedFIFOQueue"
        fun createWithParams(store:MutableStore, serializer: Serializer<*>, importInput:DataInput2? = null): MutableMap<String, String> {
            val ret = TreeMap<String,String>()

            val rootRecid:Long = if(importInput!=null){
                import(serializer, importInput, store)
            }else{
                store.put(0L, Serializers.RECID)
            }
            ret[DB.ParamNames.recid] = rootRecid.toString()
            ret[DB.ParamNames.format] = formatFIFO

            return ret
        }

        /** import, return headRecid */
        private fun import(serializer: Serializer<*>, importInput: DataInput2, store: MutableStore):Long {
            var prevRecid = 0L
            //dummy queue
            var q = LinkedFIFOQueue(store = StoreOnHeap(), rootRecid = 111L, serializer = serializer as Serializer<Any?>)
            //create stack in cycle
            while (importInput.availableMore()) {
                val e = serializer.deserialize(importInput)
                val n = Node(prevRecid, e)
                prevRecid = store.put(n, q.nodeSer)
            }
            //create head
            return store.put(prevRecid, Serializers.RECID)
        }

        fun <T> openWithParams(store: Store, serializer:Serializer<T>, qp: Map<String, String>): Queue<T> {
            val rootRecid = qp[DB.ParamNames.recid]!!.toLong()
            dataAssert(qp[DB.ParamNames.format] == formatFIFO)
            return LinkedFIFOQueue(store=store as MutableStore, rootRecid=rootRecid, serializer = serializer)
        }

    }

    private data class Node<E>(val prevRecid:Long, val e:E)

    private val nodeSer = object:Serializer<Node<E>>{
        override fun serialize(out: DataOutput2, k: Node<E>) {
            out.writePackedRecid(k.prevRecid)
            serializer.serialize(out, k.e)
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


    override fun peek(): E? {
        lock.lockRead{
            val headRecid = head.get()
            if(headRecid==0L)
                return null

            val node = store.get(headRecid, nodeSer)!!
            return node.e
        }
    }


    private fun deleteHeadNode(headRecid:Long, node: Node<E>){
        head.set(node.prevRecid)
        store.delete(headRecid, nodeSer)
    }



    override fun poll(): E? {
        lock.lockWrite{
            val headRecid = head.get()
            if(headRecid==0L)
                return null

            modCount++
            val node = store.get(headRecid, nodeSer)!!
            deleteHeadNode(headRecid, node)
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
            deleteHeadNode(headRecid, node)
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
        lock.lockWrite {
            while(true) {
                val headRecid = head.get()
                if (headRecid != 0L) {
                    modCount++
                    val node = store.get(headRecid, nodeSer)!!
                    head.set(node.prevRecid)
                    store.delete(headRecid, nodeSer)
                    return node.e
                }
                notEmpty.await()
            }
        }
        throw InternalError()
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
        if(maxElements<=0)
            return 0

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

    override fun spliterator(): Spliterator<E> {
        lock.lockRead {
            return object : Spliterator<E> {

                val iterLock = ReentrantLock()

                var iterHead = 0L
                var iterModCount:Long? = null


                private fun checkModCount() {
                    if(iterModCount==null) {
                        iterModCount = modCount
                        iterHead = head.get()
                    }
                    if (iterModCount != modCount)
                        throw ConcurrentModificationException()
                }


                override fun estimateSize() = Long.MAX_VALUE

                override fun characteristics() = (Spliterator.CONCURRENT
                        or Spliterator.NONNULL
                        or Spliterator.ORDERED)

                override fun trySplit(): Spliterator<E>? = null

                override fun tryAdvance(action: Consumer<in E>?): Boolean {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()

                            if (iterHead == 0L)
                                return false

                            val node = store.get(iterHead, nodeSer)!!
                            action!!.accept(node.e)
                            iterHead = node.prevRecid
                            return true
                        }
                    }
                }

                override fun forEachRemaining(action: Consumer<in E>?) {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()

                            while (iterHead != 0L) {
                                if (iterHead == 0L)
                                    return;
                                val node = store.get(iterHead, nodeSer)!!
                                action!!.accept(node.e)
                                iterHead = node.prevRecid
                            }
                        }
                    }
                }
            }
        }
    }


    override fun iterator(): MutableIterator<E> {
        lock.lockRead {
            return object:MutableIterator<E> {

                var iterModCount:Long? = null;
                var iterHead = 0L

                //used for delete
                var iterHeadRecid2 = 0L
                var iterHeadRecid3 = 0L

                val iterLock = ReentrantLock() //protect variables, TODO use atomic CAS or Mutex instead of lock with memory barrier


                private fun checkModCount() {
                    if(iterModCount==null) {
                        iterModCount = modCount
                        iterHead = head.get()
                    }
                    if (iterModCount != modCount)
                        throw ConcurrentModificationException()
                }

                override fun hasNext(): Boolean {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()
                            return iterHead != 0L
                        }
                    }
                }

                override fun next(): E {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()
                            if(iterHead==0L)
                                throw NoSuchElementException()

                            val node = store.get(iterHead, nodeSer)!!
                            iterHeadRecid3 = iterHeadRecid2
                            iterHeadRecid2 = iterHead
                            iterHead = node.prevRecid

                            return node.e
                        }
                    }
                }

                override fun remove() {
                    iterLock.lock{
                        if(iterHeadRecid2 == 0L)
                            throw NoSuchElementException()
                        lock.lockWrite {
                            checkModCount()

                            if(iterHeadRecid3==0L) {
                                //deleted element is head of queue
                                head.set(iterHead)
                            }else{
                                //delete in middle of queue
                                store.updateWeak(iterHeadRecid3, nodeSer){ oldNode->
                                    Node(iterHead, oldNode!!.e)
                                }
                            }

                            //invalidate deleted element
                            store.delete(iterHeadRecid2, nodeSer)
                            iterHeadRecid2 = 0L
                        }
                    }
                }

                override fun forEachRemaining(action: Consumer<in E>) {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()

                            while (iterHead != 0L) {
                                if (iterHead == 0L)
                                    return;
                                val node = store.get(iterHead, nodeSer)!!
                                action!!.accept(node.e)
                                iterHead = node.prevRecid
                            }
                        }
                    }
                }

            }
        }
    }


    override fun exportToDataOutput2(out: DataOutput2) {
        //TODO format header
        lock.lockRead {
            var recid = head.get()

            while(recid!=0L){
                val node = store.get(recid, nodeSer) ?: throw DBException.DataAssert()
                serializer.serialize(out, node.e)
                recid = node.prevRecid
            }
        }
    }
}