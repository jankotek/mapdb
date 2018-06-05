package org.mapdb.queue

import org.mapdb.DBException
import org.mapdb.Validate
import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2
import org.mapdb.serializer.Serializer
import org.mapdb.store.MutableStore
import org.mapdb.util.*
import java.io.PrintStream
import java.util.*
import java.util.concurrent.BlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer
import kotlin.NoSuchElementException

class LinkedDeque<E>(
        private val store: MutableStore,
        headRecid:Long,
        tailRecid:Long,

        private val serializer: Serializer<E>
    ):AbstractQueue<E>(), BlockingDeque<E>, Validate {


    private data class Node<E>(val prevRecid:Long, val nextRecid:Long, val e:E)


    private val nodeSer = object:Serializer<Node<E>>{
        override fun serialize(k: Node<E>, out: DataOutput2) {
            out.writePackedRecid(k.prevRecid)
            out.writePackedRecid(k.nextRecid)
            serializer.serialize(k.e, out)
        }

        override fun deserialize(input: DataInput2): Node<E> {
            return Node(
                    input.readPackedRecid(),
                    input.readPackedRecid(),
                    serializer.deserialize(input))
        }

        override fun serializedType() = Node::class.java
    }

    private val head = RecidRecord(store, headRecid)
    private val tail = RecidRecord(store, tailRecid)

    private val lock = newReadWriteLock(true)

    private var modCount = 0L

    private val notEmpty = lock!!.writeLock().newCondition()


    fun sizeLong():Long{
        lock.lockRead {
            var count=0L
            var recid = head.get()
            while(recid!=0L){
                val node = store.get(recid, nodeSer) ?: return count
                count++
                recid = node.prevRecid
            }
            return count
        }
    }

    override val size: Int
        get() =  Math.min(Int.MAX_VALUE.toLong(), sizeLong()).toInt()


    override fun offer(e: E?): Boolean {
        put(e)
        return true;
    }

    override fun offer(e: E?, timeout: Long, unit: TimeUnit?): Boolean {
        put(e)
        return true
    }



    private fun deleteHeadNode(headRecid:Long, node:Node<E>){
        lock.assertWriteLock()
        //update previous node
        val prevNode = store.get(node.prevRecid, nodeSer)
        if(prevNode == null){
            //queue is empty
            store.delete(node.prevRecid, nodeSer)
            store.delete(node.nextRecid, nodeSer)
            head.set(0L)
            tail.set(0L)
        }else{
            //modify pointer on prevNode
            store.update(node.prevRecid, nodeSer, prevNode.copy(nextRecid = node.nextRecid))
            head.set(node.prevRecid)
        }
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

    override fun poll(timeout: Long, unit: TimeUnit?): E? {
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


    override fun pollFirst(timeout: Long, unit: TimeUnit?): E? {
        return poll(timeout, unit)
    }

    override fun pollFirst(): E? {
        return poll()
    }


    override fun pollLast(): E? {
        lock.lockWrite{
            val tailRecid = tail.get()
            if(tailRecid==0L)
                return null

            modCount++
            val node = store.get(tailRecid, nodeSer)!!
            deleteHeadNode(tailRecid, node)
            return node.e
        }
    }

    override fun pollLast(timeout: Long, unit: TimeUnit?): E? {
        lock.lockWrite {
            var tailRecid = tail.get()
            if(tailRecid == 0L){
                //empty, await until not empty
                notEmpty.await(timeout, unit)
                tailRecid = tail.get()
            }

            if (tailRecid == 0L) {
                return null;
            }

            modCount++
            val node = store.get(tailRecid, nodeSer)!!
            deleteHeadNode(tailRecid, node)
            return node.e
        }
    }



    override fun addFirst(e: E) {
        put(e)
    }

    override fun offerLast(e: E): Boolean {
        putLast(e)
        return true
    }

    override fun offerLast(e: E, timeout: Long, unit: TimeUnit?): Boolean {
        return offerLast(e);
    }

    override fun putFirst(e: E) {
        put(e)
    }


    override fun getFirst(): E {
        return peekFirst()?:throw NoSuchElementException()
    }


    override fun getLast(): E {
        return peekLast()?:throw NoSuchElementException()
    }



    override fun offerFirst(e: E): Boolean {
        put(e)
        return true
    }

    override fun offerFirst(e: E, timeout: Long, unit: TimeUnit?): Boolean {
        return offerFirst(e)
    }



    override fun push(e: E) {
        put(e)
    }

    override fun take(): E {
        lock.lockWrite {
            while(true) {
                val headRecid = head.get()
                if (headRecid != 0L) {
                    modCount++
                    val node = store.get(headRecid, nodeSer)!!
                    removeNode(headRecid) //TODO this can be much faster, delete Empty Head, set current head to null
                    return node.e
                }
                notEmpty.await()
            }
        }
        throw InternalError()
    }


    override fun takeFirst(): E {
        return take()
    }

    override fun takeLast(): E {
        lock.lockWrite {
            while(true) {
                val tailRecid = tail.get()
                if (tailRecid != 0L) {
                    modCount++
                    val node = store.get(tailRecid, nodeSer)!!
                    removeNode(tailRecid) //TODO this can be much faster, delete Empty Head, set current head to null
                    return node.e
                }
                notEmpty.await()
            }
        }
        throw InternalError()
    }


    override fun removeFirst(): E {
        return pollFirst() ?: throw NoSuchElementException()
    }

    override fun removeLast(): E {
        return pollLast() ?: throw NoSuchElementException()
    }


    override fun peek(): E? {
        return peekFirst()
    }


    override fun peekFirst(): E? {
        lock.lockRead {
            val headRecid = head.get()
            if(headRecid==0L)
                return null
            val node = store.get(headRecid, nodeSer)?:throw DBException.DataAssert()
            return node.e
        }
    }


    override fun peekLast(): E? {
        lock.lockRead {
            val tailRecid = head.get()
            if(tailRecid==0L)
                return null
            val node = store.get(tailRecid, nodeSer)?:throw DBException.DataAssert()
            return node.e
        }
    }

    override fun addLast(e: E?) {
        putLast(e)
    }

    override fun putLast(e: E?) {
        e!!
        lock.lockWrite {
            val tailRecid = tail.get()
            if(tailRecid == 0L){
                //empty queue, insert new
                dataAssert(head.get()==0L)
                val node = Node(
                        store.put(null, nodeSer),
                        store.put(null, nodeSer),
                        e);
                val newTail = store.put(node, nodeSer)
                head.set(newTail)
                tail.set(newTail)
            }else{
                //not empty, update empty  node and update pointers
                val oldTail = store.get(tailRecid, nodeSer)!!
                val node = Node(
                        nextRecid = tailRecid,
                        prevRecid = store.put(null, nodeSer),
                        e = e)
                store.update(oldTail.prevRecid, nodeSer, node)
                tail.set(oldTail.prevRecid)
            }
            modCount++
            notEmpty.signal()
        }
    }

    override fun put(e: E?) {
        e!!
        lock.lockWrite {
            val headRecid = head.get()
            if(headRecid == 0L){
                //empty queue, insert new
                dataAssert(tail.get()==0L)
                val node = Node(
                        store.put(null, nodeSer),
                        store.put(null, nodeSer),
                        e);
                val newHead = store.put(node, nodeSer)
                head.set(newHead)
                tail.set(newHead)
            }else{
                //not empty, update empty  node and update pointers
                val oldHead = store.get(headRecid, nodeSer)!!
                val node = Node(
                        prevRecid = headRecid,
                        nextRecid = store.put(null, nodeSer),
                        e = e)
                store.update(oldHead.nextRecid, nodeSer, node)
                head.set(oldHead.nextRecid)
            }
            modCount++
            notEmpty.signal()
        }
    }



    private fun removeOccurFromIter(iter: MutableIterator<E>, o: Any?): Boolean {
        while (iter.hasNext()) {
            val n = iter.next()
            if (serializer.equals(n, o as E?))
                return true
        }
        return false
    }

    override fun removeFirstOccurrence(o: Any?): Boolean {
        if(o==null)
            return false
        return removeOccurFromIter(iterator(), o)
    }


    override fun removeLastOccurrence(o: Any?): Boolean {
        if(o==null)
            return false
        return removeOccurFromIter(descendingIterator(), o)
    }


    override fun pop(): E {
        return removeFirst()
    }


    override fun drainTo(c: MutableCollection<in E>?): Int {
        if(c==null)
            throw NullPointerException()
        if(c==this)
            throw IllegalArgumentException()

        lock.lockWrite {
            var counter = 0L
            var recid = head.get()
            var first = true

            //set queue to empty
            head.set(0L)
            tail.set(0L)
            while(true){
                modCount++
                val node = store.getAndDelete(recid, nodeSer) ?: break

                if(first){
                    first = false
                    if(node!=null)
                        store.delete(node.nextRecid, nodeSer)
                }

                c.add(node.e)
                recid = node.prevRecid
                counter++
            }

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
            var firstRecid:Long? = null

            while(true){
                modCount++
                val node = store.getAndDelete(recid, nodeSer)

                if(node==null){
                    //reached end of queue
                    head.set(0L)
                    tail.set(0L)
                    return Math.min(Integer.MAX_VALUE.toLong(), counter).toInt()
                }
                if(firstRecid==null){
                    firstRecid = node.nextRecid
                }

                c.add(node.e)
                recid = node.prevRecid
                counter++

                if(counter>=maxElements){
                    //return without removing all elements
                    val endNode = store.get(recid, nodeSer)
                    if(endNode == null){
                        //empty queue
                        store.delete(recid,nodeSer)
                        store.delete(firstRecid, nodeSer)
                        head.set(0L)
                        tail.set(0L)
                    }else{
                        //update current node to point to end,
                        head.set(recid)
                        store.update(recid, nodeSer, endNode.copy(nextRecid=firstRecid))
                    }
                    return Math.min(Integer.MAX_VALUE.toLong(), counter).toInt()
                }
            }
            throw AssertionError("should not be here")
        }
    }

    override fun remainingCapacity(): Int = Integer.MAX_VALUE

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
                var iterPos = 0L
                var iterTail = 0L
                var iterToDelete = 0L

                val iterLock = ReentrantLock() //protect variables, TODO use atomic CAS or Mutex instead of lock with memory barrier


                private fun checkModCount() {
                    if(iterModCount==null) {
                        iterModCount = modCount
                        iterPos = head.get()
                        iterTail = tail.get()
                    }
                    if (iterModCount != modCount)
                        throw ConcurrentModificationException()
                }

                override fun hasNext(): Boolean {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()
                            return iterPos != 0L
                        }
                    }
                }

                override fun next(): E {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()
                            if(iterPos==0L)
                                throw NoSuchElementException()

                            val node = store.get(iterPos, nodeSer)!!
                            iterToDelete = iterPos
                            iterPos =
                                    if(iterPos == iterTail) 0L
                                    else node.prevRecid
                            return node.e
                        }
                    }
                }

                override fun remove() {
                    iterLock.lock{
                        lock.lockWrite {
                            checkModCount()
                            if(iterToDelete == 0L)
                                throw NoSuchElementException()
                            removeNode(iterToDelete)
                            iterToDelete = 0L
                        }
                    }
                }

                override fun forEachRemaining(action: Consumer<in E>) {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()

                            while (iterPos != 0L) {
                                if (iterPos == 0L)
                                    return;
                                val node = store.get(iterPos, nodeSer)!!
                                action!!.accept(node.e)
                                iterPos = node.prevRecid
                            }
                        }
                    }
                }

            }
        }
    }

    private fun removeNode(nodeRecid: Long) {
        lock.assertWriteLock()
        val node = store.get(nodeRecid, nodeSer)!!

        var prevNode = store.get(node.prevRecid, nodeSer)
        var nextNode = store.get(node.nextRecid, nodeSer)

        if(nextNode==null && prevNode == null){
            //single entry
            store.delete(node.prevRecid, nodeSer)
            store.delete(node.nextRecid, nodeSer)
            head.set(0L)
            tail.set(0L)
            store.delete(nodeRecid, nodeSer)
            return
        }


        if(nextNode==null){
            head.set(node.prevRecid)
        }else{
            store.update(node.nextRecid, nodeSer, nextNode.copy(prevRecid = node.prevRecid))
        }

        if(prevNode==null){
            tail.set(node.nextRecid)
        }else{
            store.update(node.prevRecid, nodeSer, prevNode.copy(nextRecid = node.nextRecid))
        }

        store.delete(nodeRecid, nodeSer)
    }


    override fun descendingIterator(): MutableIterator<E> {
        lock.lockRead {
            return object:MutableIterator<E> {

                var iterModCount:Long? = null;
                var iterPos = 0L
                var iterHead = 0L
                var iterToDelete = 0L

                val iterLock = ReentrantLock() //protect variables, TODO use atomic CAS or Mutex instead of lock with memory barrier


                private fun checkModCount() {
                    if(iterModCount==null) {
                        iterModCount = modCount
                        iterPos = tail.get()
                        iterHead = head.get()
                    }
                    if (iterModCount != modCount)
                        throw ConcurrentModificationException()
                }

                override fun hasNext(): Boolean {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()
                            return iterPos != 0L
                        }
                    }
                }

                override fun next(): E {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()
                            if(iterPos==0L)
                                throw NoSuchElementException()

                            val node = store.get(iterPos, nodeSer)!!
                            iterToDelete = iterPos
                            iterPos =
                                    if(iterPos == iterHead) 0L
                                    else node.nextRecid
                            return node.e
                        }
                    }
                }

                override fun remove() {
                    iterLock.lock{
                        lock.lockWrite {
                            checkModCount()
                            if(iterToDelete == 0L)
                                throw NoSuchElementException()
                            removeNode(iterToDelete)
                            iterToDelete = 0L
                        }
                    }
                }

                override fun forEachRemaining(action: Consumer<in E>) {
                    iterLock.lock {
                        lock.lockRead {
                            checkModCount()

                            while (iterPos != 0L) {
                                if (iterPos == 0L)
                                    return;
                                val node = store.get(iterPos, nodeSer)!!
                                action!!.accept(node.e)
                                iterPos = node.nextRecid
                            }
                        }
                    }
                }
            }
        }
    }

    override fun validate() {
        lock.lockRead {
            if(head.get()==0L && tail.get()==0L)
                return

            var recid = head.get()

            while(true){

                var node = store.get(recid, nodeSer) ?: throw DBException.DataAssert()

                val prevNode = store.get(node.prevRecid, nodeSer)
                val nextNode = store.get(node.nextRecid, nodeSer)

                //link consistency
                if(nextNode == null){
                    //is first node
                    dataAssert(head.get() == recid)
                }else{
                    dataAssert(nextNode.prevRecid == recid)
                }

                if(prevNode==null){
                    //is last node
                    dataAssert(tail.get() == recid)
                    return
                }else{
                    dataAssert(prevNode.nextRecid == recid)
                }

                recid = node.prevRecid
            }

        }
    }


    fun printInfo(out: PrintStream = System.out){
        out.println(javaClass)
        out.println("  head:${head.get()}")
        out.println("  tail:${tail.get()}")
        var recid = head.get()
        var first = true
        while(true){
            val n = store.get(recid, nodeSer)
            if(n==null) {
                println("    $recid - END");
                return
            }
            if(first){
                first = false
                println("    ${n.nextRecid} - START")
            }

            println("    $recid - "+n.e)
            recid = n.prevRecid
        }

    }
}